#!/usr/bin/env python3
"""
Simplified ETL for OpenAlex Morocco Data → Star Schema
Uses ONLY data embedded in Work documents - NO API enrichment calls
Faster processing, simpler implementation, all data comes from works
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import json
from dotenv import load_dotenv

import psycopg2
import psycopg2.extras
from tqdm import tqdm
from pyalex import Works, config

# ============================================================================
# Configuration
# ============================================================================

load_dotenv()

config.email = os.getenv("OPENALEX_EMAIL", "your.email@example.com")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "openalex_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

API_BATCH_SIZE = 200
API_REQUEST_DELAY = 0.05
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "0"))  # 0 means no limit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# Database Connection
# ============================================================================

class DatabaseConnection:
    """Manages PostgreSQL database connections"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logger.info("✅ Connected to PostgreSQL database")
            return self.conn
        except psycopg2.Error as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("✅ Disconnected from PostgreSQL database")

    def insert_batch(self, table: str, rows: List[Dict[str, Any]]) -> int:
        """Insert multiple rows efficiently (DEPRECATED - use insert_row_by_row instead)"""
        if not rows:
            return 0

        try:
            cols = list(rows[0].keys())
            cols_str = ", ".join(cols)
            vals_template = ", ".join(["%s"] * len(cols))
            query = (
                f"INSERT INTO {table} ({cols_str}) VALUES ({vals_template}) "
                "ON CONFLICT DO NOTHING"
            )

            data = [tuple(row.get(col) for col in cols) for row in rows]
            self.cursor.executemany(query, data)
            self.conn.commit()
            return len(rows)
        except psycopg2.Error as e:
            logger.error(f"❌ Batch insert failed for {table}: {e}")
            self.conn.rollback()
            return 0

    def insert_row_by_row(self, table: str, rows: List[Dict[str, Any]]) -> int:
        """Insert rows one at a time with progress bar - allows partial success"""
        if not rows:
            return 0

        inserted = 0
        for row in tqdm(rows, desc=f"Inserting into {table}", unit="row", leave=False):
            try:
                cols = list(row.keys())
                cols_str = ", ".join(cols)
                vals_template = ", ".join(["%s"] * len(cols))
                query = (
                    f"INSERT INTO {table} ({cols_str}) VALUES ({vals_template}) "
                    "ON CONFLICT DO NOTHING"
                )
                
                values = tuple(row.get(col) for col in cols)
                self.cursor.execute(query, values)
                self.conn.commit()
                inserted += 1
            except psycopg2.Error as e:
                self.conn.rollback()
                # Log but continue with next row
                logger.debug(f"⚠️  Row skipped in {table}: {e}")
        
        return inserted


# ============================================================================
# Data Extraction (from work documents only)
# ============================================================================

class WorksDataExtractor:
    """Extracts data directly from work documents - NO API calls"""

    @staticmethod
    def extract_institutions_from_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract institutions from authorships"""
        logger.info("📍 Extracting institutions from works...")

        institutions = {}
        for work in works:
            for authorship in work.get("authorships", []):
                for inst in authorship.get("institutions", []):
                    inst_id_raw = inst.get("id", "")
                    if not inst_id_raw:
                        continue
                    
                    inst_id = inst_id_raw.replace("https://openalex.org/", "")
                    inst_name = (inst.get("display_name") or "").strip()
                    
                    if not inst_name:
                        continue
                    
                    if inst_id not in institutions:
                        institutions[inst_id] = {
                            "institution_id": inst_id,
                            "institution_name": inst_name,
                            "institution_type": inst.get("type", ""),
                            "country_code": inst.get("country_code", ""),
                            "ror_url": inst.get("ror"),
                        }

        logger.info(f"✅ Extracted {len(institutions)} unique institutions")
        return list(institutions.values())

    @staticmethod
    def extract_authors_from_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract authors from authorships - basic data only"""
        logger.info("👤 Extracting authors from works...")

        authors = {}
        for work in works:
            for authorship in work.get("authorships", []):
                author_data = authorship.get("author", {})
                author_id_raw = author_data.get("id", "")
                
                if not author_id_raw:
                    continue
                
                author_id = author_id_raw.replace("https://openalex.org/", "")
                
                if author_id not in authors:
                    authors[author_id] = {
                        "author_id": author_id,
                        "author_name": author_data.get("display_name", ""),
                        "orcid": author_data.get("orcid"),
                    }

        logger.info(f"✅ Extracted {len(authors)} unique authors")
        return list(authors.values())

    @staticmethod
    def extract_topics_from_works(works: List[Dict[str, Any]]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        """Extract topics with full hierarchy: domain → field → subfield"""
        logger.info("📚 Extracting topics from works...")

        domains = {}
        fields = {}
        subfields = {}
        topics = {}

        for work in works:
            for topic in work.get("topics", []):
                topic_id_raw = topic.get("id", "")
                if not topic_id_raw:
                    continue
                
                topic_id = topic_id_raw.replace("https://openalex.org/", "")
                
                # Extract domain
                domain_data = topic.get("domain", {})
                domain_id_raw = domain_data.get("id", "")
                if domain_id_raw:
                    domain_id = domain_id_raw.replace("https://openalex.org/", "")
                    if domain_id not in domains:
                        domains[domain_id] = {
                            "domain_id": domain_id,
                            "domain_name": domain_data.get("display_name", ""),
                        }
                else:
                    domain_id = None

                # Extract field
                field_data = topic.get("field", {})
                field_id_raw = field_data.get("id", "")
                if field_id_raw:
                    field_id = field_id_raw.replace("https://openalex.org/", "")
                    if field_id not in fields:
                        fields[field_id] = {
                            "field_id": field_id,
                            "field_name": field_data.get("display_name", ""),
                            "domain_id": domain_id,
                        }
                else:
                    field_id = None

                # Extract subfield
                subfield_data = topic.get("subfield", {})
                subfield_id_raw = subfield_data.get("id", "")
                if subfield_id_raw:
                    subfield_id = subfield_id_raw.replace("https://openalex.org/", "")
                    if subfield_id not in subfields:
                        subfields[subfield_id] = {
                            "subfield_id": subfield_id,
                            "subfield_name": subfield_data.get("display_name", ""),
                            "field_id": field_id,
                        }
                else:
                    subfield_id = None

                # Extract topic
                if topic_id not in topics:
                    topics[topic_id] = {
                        "topic_id": topic_id,
                        "topic_name": topic.get("display_name", ""),
                        "domain_id": domain_id,
                        "domain_name": domain_data.get("display_name"),
                        "field_id": field_id,
                        "field_name": field_data.get("display_name"),
                        "subfield_id": subfield_id,
                        "subfield_name": subfield_data.get("display_name"),
                    }

        logger.info(f"✅ Extracted {len(domains)} domains, {len(fields)} fields, {len(subfields)} subfields, {len(topics)} topics")
        return list(topics.values()), list(fields.values()), list(subfields.values()), list(domains.values())

    @staticmethod
    def extract_keywords_from_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract keywords from work.keywords"""
        logger.info("🔑 Extracting keywords from works...")

        keywords = {}
        for work in works:
            for kw in work.get("keywords", []):
                kw_id_raw = kw.get("id", "")
                if not kw_id_raw:
                    continue
                
                kw_id = kw_id_raw.replace("https://openalex.org/keywords/", "")
                
                if kw_id not in keywords:
                    keywords[kw_id] = {
                        "keyword_id": kw_id,
                        "keyword_name": kw.get("display_name", ""),
                    }

        logger.info(f"✅ Extracted {len(keywords)} unique keywords")
        return list(keywords.values())

    @staticmethod
    def extract_sources_from_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract sources from primary_location.source and locations[].source"""
        logger.info("📰 Extracting sources from works...")

        sources = {}
        for work in works:
            # Extract from primary location
            primary_location = work.get("primary_location", {})
            source_data = primary_location.get("source", {})
            
            if source_data:
                source_id_raw = source_data.get("id", "")
                if source_id_raw:
                    source_id = source_id_raw.replace("https://openalex.org/", "")
                    if source_id not in sources:
                        sources[source_id] = {
                            "source_id": source_id,
                            "source_name": source_data.get("display_name", ""),
                            "source_type": source_data.get("type"),
                            "issn_l": source_data.get("issn_l"),
                            "is_oa": source_data.get("is_oa", False),
                            "is_in_doaj": source_data.get("is_in_doaj", False),
                            "is_core": source_data.get("is_core", False),
                            "host_organization_name": source_data.get("host_organization_name"),
                        }
            
            # Extract from alternative locations
            for location in work.get("locations", []):
                source_data = location.get("source", {})
                if source_data:
                    source_id_raw = source_data.get("id", "")
                    if source_id_raw:
                        source_id = source_id_raw.replace("https://openalex.org/", "")
                        if source_id not in sources:
                            sources[source_id] = {
                                "source_id": source_id,
                                "source_name": source_data.get("display_name", ""),
                                "source_type": source_data.get("type"),
                                "issn_l": source_data.get("issn_l"),
                                "is_oa": source_data.get("is_oa", False),
                                "is_in_doaj": source_data.get("is_in_doaj", False),
                                "is_core": source_data.get("is_core", False),
                                "host_organization_name": source_data.get("host_organization_name"),
                            }

        logger.info(f"✅ Extracted {len(sources)} unique sources")
        return list(sources.values())

    @staticmethod
    def extract_concepts_from_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract concepts from work.concepts"""
        logger.info("💡 Extracting concepts from works...")

        concepts = {}
        for work in works:
            for concept in work.get("concepts", []):
                concept_id_raw = concept.get("id", "")
                if not concept_id_raw:
                    continue
                
                concept_id = concept_id_raw.replace("https://openalex.org/", "")
                
                if concept_id not in concepts:
                    concepts[concept_id] = {
                        "concept_id": concept_id,
                        "concept_name": concept.get("display_name", ""),
                        "concept_level": concept.get("level"),
                    }

        logger.info(f"✅ Extracted {len(concepts)} unique concepts")
        return list(concepts.values())

    @staticmethod
    def extract_citation_years_from_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract citation counts by year from work.counts_by_year"""
        logger.info("📊 Extracting citation counts by year...")

        citation_years = []
        for work in works:
            work_id = work.get("id", "").replace("https://openalex.org/", "")
            for year_data in work.get("counts_by_year", []):
                if year_data.get("year") and year_data.get("cited_by_count") is not None:
                    citation_years.append({
                        "work_id": work_id,
                        "year": year_data.get("year"),
                        "cited_by_count": year_data.get("cited_by_count"),
                    })

        logger.info(f"✅ Extracted {len(citation_years)} year-citation records")
        return citation_years

    @staticmethod
    def extract_locations_from_works(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract alternative locations/versions from work.locations"""
        logger.info("🌍 Extracting work locations...")

        locations = []
        for work in works:
            work_id = work.get("id", "").replace("https://openalex.org/", "")
            for location in work.get("locations", []):
                source_data = location.get("source", {})
                source_id = source_data.get("id", "").replace("https://openalex.org/", "") if source_data else None
                
                locations.append({
                    "work_id": work_id,
                    "location_id": location.get("id"),
                    "is_oa": location.get("is_oa", False),
                    "landing_page_url": location.get("landing_page_url"),
                    "pdf_url": location.get("pdf_url"),
                    "source_id": source_id,
                    "source_name": source_data.get("display_name") if source_data else None,
                    "source_type": source_data.get("type") if source_data else None,
                    "license_id": location.get("license_id"),
                    "version": location.get("version"),
                    "is_accepted": location.get("is_accepted", False),
                    "is_published": location.get("is_published", False),
                    "raw_source_name": location.get("raw_source_name"),
                    "raw_type": location.get("raw_type"),
                })

        logger.info(f"✅ Extracted {len(locations)} work locations")
        return locations


# ============================================================================
# Data Loaders
# ============================================================================

class DataLoader:
    """Loads normalized data into star schema tables"""

    def __init__(self, db: DatabaseConnection):
        self.db = db

    def populate_time_dimension(self):
        """Populate dim_time with years"""
        logger.info("📅 Populating time dimension...")

        rows = []
        for year in range(1950, 2031):
            rows.append({
                "year_id": year,
                "year": year,
                "decade": (year // 10) * 10,
            })

        inserted = self.db.insert_batch("dim_time", rows)
        logger.info(f"✅ Inserted {inserted} time records")

    def load_domains(self, domains: List[Dict[str, Any]]):
        """Load domain data"""
        if not domains:
            return
        logger.info(f"🔄 Loading {len(domains)} domains...")
        inserted = self.db.insert_batch("dim_domains", domains)
        logger.info(f"✅ Inserted {inserted} domain records")

    def load_fields(self, fields: List[Dict[str, Any]]):
        """Load field data"""
        if not fields:
            return
        logger.info(f"🔄 Loading {len(fields)} fields...")
        inserted = self.db.insert_batch("dim_fields", fields)
        logger.info(f"✅ Inserted {inserted} field records")

    def load_subfields(self, subfields: List[Dict[str, Any]]):
        """Load subfield data"""
        if not subfields:
            return
        logger.info(f"🔄 Loading {len(subfields)} subfields...")
        inserted = self.db.insert_batch("dim_subfields", subfields)
        logger.info(f"✅ Inserted {inserted} subfield records")

    def load_topics(self, topics: List[Dict[str, Any]]):
        """Load topic data"""
        if not topics:
            return
        logger.info(f"🔄 Loading {len(topics)} topics...")
        inserted = self.db.insert_batch("dim_topics", topics)
        logger.info(f"✅ Inserted {inserted} topic records")

    def load_keywords(self, keywords: List[Dict[str, Any]]):
        """Load keyword data"""
        if not keywords:
            return
        logger.info(f"🔄 Loading {len(keywords)} keywords...")
        inserted = self.db.insert_batch("dim_keywords", keywords)
        logger.info(f"✅ Inserted {inserted} keyword records")

    def load_sources(self, sources: List[Dict[str, Any]]):
        """Load source data"""
        if not sources:
            return
        logger.info(f"🔄 Loading {len(sources)} sources...")
        inserted = self.db.insert_batch("dim_sources", sources)
        logger.info(f"✅ Inserted {inserted} source records")

    def load_institutions(self, institutions: List[Dict[str, Any]]):
        """Load institution data"""
        if not institutions:
            return
        logger.info(f"🔄 Loading {len(institutions)} institutions...")
        inserted = self.db.insert_batch("dim_institutions", institutions)
        logger.info(f"✅ Inserted {inserted} institution records")

    def load_authors(self, authors: List[Dict[str, Any]]):
        """Load author data"""
        if not authors:
            return
        logger.info(f"🔄 Loading {len(authors)} authors...")
        inserted = self.db.insert_batch("dim_authors", authors)
        logger.info(f"✅ Inserted {inserted} author records")

    def load_concepts(self, concepts: List[Dict[str, Any]]):
        """Load concept data"""
        if not concepts:
            return
        logger.info(f"🔄 Loading {len(concepts)} concepts...")
        inserted = self.db.insert_batch("dim_concepts", concepts)
        logger.info(f"✅ Inserted {inserted} concept records")

    def load_citation_years(self, citation_years: List[Dict[str, Any]]):
        """Load citation by year data - row by row for partial success"""
        if not citation_years:
            return
        logger.info(f"🔄 Loading {len(citation_years)} citation year records...")
        inserted = self.db.insert_row_by_row("fact_work_citation_year", citation_years)
        logger.info(f"✅ Inserted {inserted}/{len(citation_years)} citation year records")

    def load_locations(self, locations: List[Dict[str, Any]]):
        """Load work locations data - row by row for partial success"""
        if not locations:
            return
        logger.info(f"🔄 Loading {len(locations)} work locations...")
        inserted = self.db.insert_row_by_row("fact_work_locations", locations)
        logger.info(f"✅ Inserted {inserted}/{len(locations)} work location records")

    def load_works(self, works: List[Dict[str, Any]]):
        """Load works and related fact tables"""
        logger.info(f"🔄 Loading {len(works)} works...")

        work_facts = []
        work_authors = []
        work_author_institutions = []
        work_topics = []
        work_keywords = []
        work_concepts = []

        for work in tqdm(works, desc="Processing works"):
            # Create work fact
            primary_location = work.get("primary_location", {})
            source_data = primary_location.get("source", {})
            source_id = source_data.get("id", "").replace("https://openalex.org/", "") if source_data else None

            work_id = work.get("id", "").replace("https://openalex.org/", "")
            
            work_facts.append({
                "work_id": work_id,
                "doi": work.get("doi"),
                "title": work.get("title", ""),
                "publication_year": work.get("publication_year"),
                "publication_date": work.get("publication_date"),
                "language": work.get("language"),
                "work_type": work.get("type"),
                "source_id": source_id,
                "source_name": source_data.get("display_name") if source_data else None,
                "cited_by_count": work.get("cited_by_count", 0),
                "is_oa": work.get("open_access", {}).get("is_oa", False),
                "oa_status": work.get("open_access", {}).get("oa_status"),
                "oa_url": work.get("open_access", {}).get("oa_url"),
                "landing_page_url": primary_location.get("landing_page_url"),
                "pdf_url": primary_location.get("pdf_url"),
                "best_oa_location": work.get("best_oa_location", {}).get("landing_page_url") if isinstance(work.get("best_oa_location"), dict) else work.get("best_oa_location"),
                "is_retracted": work.get("is_retracted", False),
                "countries_distinct_count": work.get("countries_distinct_count"),
                "institutions_distinct_count": work.get("institutions_distinct_count"),
                "author_count": len(work.get("authorships", [])),
            })

            # Extract work-author relationships
            for authorship in work.get("authorships", []):
                author = authorship.get("author", {})
                author_id = author.get("id", "").replace("https://openalex.org/", "") if author.get("id") else None
                
                if author_id:
                    work_authors.append({
                        "work_id": work_id,
                        "author_id": author_id,
                        "author_name": author.get("display_name"),
                        "author_position": authorship.get("author_position"),
                        "is_corresponding": authorship.get("is_corresponding", False),
                    })

                    # Extract work-author-institution relationships
                    for inst in authorship.get("institutions", []):
                        inst_id = inst.get("id", "").replace("https://openalex.org/", "") if inst.get("id") else None
                        
                        if inst_id:
                            work_author_institutions.append({
                                "work_id": work_id,
                                "author_id": author_id,
                                "institution_id": inst_id,
                                "institution_name": inst.get("display_name"),
                                "institution_country": inst.get("country_code"),
                            })

            # Extract work-topic relationships
            for topic in work.get("topics", []):
                topic_id = topic.get("id", "").replace("https://openalex.org/", "") if topic.get("id") else None
                
                if topic_id:
                    work_topics.append({
                        "work_id": work_id,
                        "topic_id": topic_id,
                        "topic_name": topic.get("display_name"),
                        "topic_score": topic.get("score"),
                        "domain_id": topic.get("domain", {}).get("id", "").replace("https://openalex.org/", "") if topic.get("domain", {}).get("id") else None,
                        "domain_name": topic.get("domain", {}).get("display_name"),
                        "field_id": topic.get("field", {}).get("id", "").replace("https://openalex.org/", "") if topic.get("field", {}).get("id") else None,
                        "field_name": topic.get("field", {}).get("display_name"),
                        "subfield_id": topic.get("subfield", {}).get("id", "").replace("https://openalex.org/", "") if topic.get("subfield", {}).get("id") else None,
                        "subfield_name": topic.get("subfield", {}).get("display_name"),
                    })

            # Extract work-keyword relationships
            for keyword in work.get("keywords", []):
                kw_id = keyword.get("id", "").replace("https://openalex.org/keywords/", "") if keyword.get("id") else None
                
                if kw_id:
                    work_keywords.append({
                        "work_id": work_id,
                        "keyword_id": kw_id,
                        "keyword_name": keyword.get("display_name"),
                        "keyword_score": keyword.get("score"),
                    })

            # Extract work-concept relationships
            for concept in work.get("concepts", []):
                concept_id = concept.get("id", "").replace("https://openalex.org/", "") if concept.get("id") else None
                
                if concept_id:
                    work_concepts.append({
                        "work_id": work_id,
                        "concept_id": concept_id,
                        "concept_name": concept.get("display_name"),
                        "concept_score": concept.get("score"),
                    })

        # Insert all fact tables row-by-row to allow partial success
        if work_facts:
            inserted = self.db.insert_row_by_row("fact_works", work_facts)
            logger.info(f"✅ Inserted {inserted}/{len(work_facts)} work facts")

        if work_authors:
            inserted = self.db.insert_row_by_row("fact_work_authors", work_authors)
            logger.info(f"✅ Inserted {inserted}/{len(work_authors)} work-author relationships")

        if work_author_institutions:
            inserted = self.db.insert_row_by_row("fact_work_author_institutions", work_author_institutions)
            logger.info(f"✅ Inserted {inserted}/{len(work_author_institutions)} work-author-institution relationships")

        if work_topics:
            inserted = self.db.insert_row_by_row("fact_work_topics", work_topics)
            logger.info(f"✅ Inserted {inserted}/{len(work_topics)} work-topic relationships")

        if work_keywords:
            inserted = self.db.insert_row_by_row("fact_work_keywords", work_keywords)
            logger.info(f"✅ Inserted {inserted}/{len(work_keywords)} work-keyword relationships")

        if work_concepts:
            inserted = self.db.insert_row_by_row("fact_work_concepts", work_concepts)
            logger.info(f"✅ Inserted {inserted}/{len(work_concepts)} work-concept relationships")


# ============================================================================
# OpenAlex Works Fetcher
# ============================================================================

class OpenAlexWorksFetcher:
    """Fetches works data from OpenAlex API"""

    def fetch_morocco_works(self) -> List[Dict[str, Any]]:
        """Fetch works with Moroccan institutions"""
        logger.info("🔎 Fetching works with Moroccan institutions...")

        query = Works().filter(institutions={"country_code": "MA"})
        works = []
        count = 0
        retries = 0
        max_retries = 3

        try:
            for page in query.paginate(per_page=API_BATCH_SIZE):
                try:
                    for work in page:
                        works.append(work)
                        count += 1

                        if FETCH_LIMIT and count >= FETCH_LIMIT:
                            break

                        if count % 100 == 0:
                            logger.info(f"  📥 Retrieved {count} works...")

                    if FETCH_LIMIT and count >= FETCH_LIMIT:
                        break
                    
                    retries = 0

                except Exception as page_error:
                    retries += 1
                    if retries < max_retries:
                        logger.warning(f"⚠️ Retry {retries}/{max_retries}: {page_error}")
                        time.sleep(2 ** retries)
                        continue
                    else:
                        logger.warning(f"⚠️ Max retries reached, got {count} works so far")
                        break

                time.sleep(API_REQUEST_DELAY)

            logger.info(f"✅ Fetched {count} works from Morocco")
            return works

        except Exception as e:
            logger.warning(f"⚠️ Error fetching works: {e}")
            return works


# ============================================================================
# Main ETL Pipeline
# ============================================================================

def main():
    """Execute complete ETL pipeline"""
    logger.info("🚀 Starting OpenAlex Morocco Simplified ETL Pipeline")
    start_time = time.time()
    logger.info(f"🔢 Fetch limit: {FETCH_LIMIT if FETCH_LIMIT else 'No limit'}")

    db = DatabaseConnection(DB_CONFIG)
    db.connect()

    try:
        fetcher = OpenAlexWorksFetcher()
        extractor = WorksDataExtractor()
        loader = DataLoader(db)

        # STEP 1: Populate time dimension
        logger.info("\n" + "=" * 70)
        logger.info("STEP 1: Populating Reference Dimensions")
        logger.info("=" * 70)
        loader.populate_time_dimension()

        # STEP 2: Fetch works
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: Fetching Works with Moroccan Institutions")
        logger.info("=" * 70)
        works = fetcher.fetch_morocco_works()

        if not works:
            logger.error("❌ No works fetched. Aborting pipeline.")
            return

        logger.info(f"📊 Total works to process: {len(works)}")

        # STEP 3: Extract all entities from works
        logger.info("\n" + "=" * 70)
        logger.info("STEP 3: Extracting Entities from Works")
        logger.info("=" * 70)

        institutions = extractor.extract_institutions_from_works(works)
        authors = extractor.extract_authors_from_works(works)
        topics, fields, subfields, domains = extractor.extract_topics_from_works(works)
        keywords = extractor.extract_keywords_from_works(works)
        sources = extractor.extract_sources_from_works(works)
        concepts = extractor.extract_concepts_from_works(works)
        citation_years = extractor.extract_citation_years_from_works(works)
        locations = extractor.extract_locations_from_works(works)

        # STEP 4: Load dimensions
        logger.info("\n" + "=" * 70)
        logger.info("STEP 4: Loading Dimensions into Database")
        logger.info("=" * 70)

        loader.load_domains(domains)
        loader.load_fields(fields)
        loader.load_subfields(subfields)
        loader.load_topics(topics)
        loader.load_keywords(keywords)
        loader.load_sources(sources)
        loader.load_institutions(institutions)
        loader.load_authors(authors)
        loader.load_concepts(concepts)

        # STEP 5: Load facts
        logger.info("\n" + "=" * 70)
        logger.info("STEP 5: Loading Facts into Database")
        logger.info("=" * 70)

        loader.load_works(works)
        loader.load_citation_years(citation_years)
        loader.load_locations(locations)

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("ETL PIPELINE COMPLETED")
        logger.info("=" * 70)

        elapsed = time.time() - start_time
        logger.info(f"✅ Pipeline completed in {elapsed:.2f} seconds")
        logger.info(f"📦 Works: {len(works)}")
        logger.info(f"👤 Authors: {len(authors)}")
        logger.info(f"🏛️ Institutions: {len(institutions)}")
        logger.info(f"📰 Sources: {len(sources)}")
        logger.info(f"📚 Topics: {len(topics)}")
        logger.info(f"💡 Concepts: {len(concepts)}")
        logger.info(f"🔑 Keywords: {len(keywords)}")
        logger.info(f"📊 Citation Years: {len(citation_years)}")
        logger.info(f"🌍 Work Locations: {len(locations)}")

    except Exception as e:
        logger.error(f"❌ ETL Pipeline failed: {e}")
        raise

    finally:
        db.disconnect()


if __name__ == "__main__":
    main()
