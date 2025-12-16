-- OpenAlex Simplified Schema - Using ONLY data embedded in Work documents
-- No API enrichment calls - all data comes from the work.json structure
-- Star schema optimized for Power BI / Apache Superset

-- ============================================================================
-- Dimension Tables (DIM_*)
-- ============================================================================

-- Dimension: Domains (from topics)
CREATE TABLE IF NOT EXISTS dim_domains (
    domain_id VARCHAR(255) PRIMARY KEY,
    domain_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Fields (from topics)
CREATE TABLE IF NOT EXISTS dim_fields (
    field_id VARCHAR(255) PRIMARY KEY,
    field_name VARCHAR(255) NOT NULL,
    domain_id VARCHAR(255) REFERENCES dim_domains(domain_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Subfields (from topics)
CREATE TABLE IF NOT EXISTS dim_subfields (
    subfield_id VARCHAR(255) PRIMARY KEY,
    subfield_name VARCHAR(255) NOT NULL,
    field_id VARCHAR(255) REFERENCES dim_fields(field_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Topics (with full hierarchy)
CREATE TABLE IF NOT EXISTS dim_topics (
    topic_id VARCHAR(255) PRIMARY KEY,
    topic_name TEXT NOT NULL,
    domain_id VARCHAR(255) REFERENCES dim_domains(domain_id),
    domain_name VARCHAR(255),
    field_id VARCHAR(255) REFERENCES dim_fields(field_id),
    field_name VARCHAR(255),
    subfield_id VARCHAR(255) REFERENCES dim_subfields(subfield_id),
    subfield_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Keywords (from work.keywords)
CREATE TABLE IF NOT EXISTS dim_keywords (
    keyword_id VARCHAR(255) PRIMARY KEY,
    keyword_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Concepts (legacy - for reference)
CREATE TABLE IF NOT EXISTS dim_concepts (
    concept_id VARCHAR(255) PRIMARY KEY,
    concept_name TEXT NOT NULL,
    concept_level INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Sources (Journals - from primary_location.source)
CREATE TABLE IF NOT EXISTS dim_sources (
    source_id VARCHAR(255) PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_type VARCHAR(50),
    issn_l VARCHAR(50),
    is_oa BOOLEAN DEFAULT FALSE,
    is_in_doaj BOOLEAN DEFAULT FALSE,
    is_core BOOLEAN DEFAULT FALSE,
    host_organization_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Institutions (from authorships[].institutions[])
CREATE TABLE IF NOT EXISTS dim_institutions (
    institution_id VARCHAR(255) PRIMARY KEY,
    institution_name TEXT NOT NULL,
    institution_type VARCHAR(50),
    country_code VARCHAR(2),
    ror_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Authors (from authorships[].author)
CREATE TABLE IF NOT EXISTS dim_authors (
    author_id VARCHAR(255) PRIMARY KEY,
    author_name TEXT NOT NULL,
    orcid TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Time
CREATE TABLE IF NOT EXISTS dim_time (
    year_id INT PRIMARY KEY,
    year INT NOT NULL,
    decade INT
);

-- ============================================================================
-- Fact Tables (FACT_*)
-- ============================================================================

-- Fact: Works (main publication facts)
CREATE TABLE IF NOT EXISTS fact_works (
    work_id VARCHAR(255) PRIMARY KEY,
    doi TEXT,
    title TEXT NOT NULL,
    publication_year INT,
    publication_date DATE,
    language VARCHAR(10),
    work_type VARCHAR(50),
    source_id VARCHAR(255) REFERENCES dim_sources(source_id),
    source_name TEXT,
    cited_by_count INT DEFAULT 0,
    is_oa BOOLEAN DEFAULT FALSE,
    oa_status VARCHAR(50),
    oa_url TEXT,
    landing_page_url TEXT,
    pdf_url TEXT,
    best_oa_location TEXT,
    is_retracted BOOLEAN DEFAULT FALSE,
    countries_distinct_count INT,
    institutions_distinct_count INT,
    author_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Work-Author relationships
CREATE TABLE IF NOT EXISTS fact_work_authors (
    fact_id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) NOT NULL REFERENCES fact_works(work_id),
    author_id VARCHAR(255) NOT NULL REFERENCES dim_authors(author_id),
    author_name TEXT,
    author_position VARCHAR(50),
    is_corresponding BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Work-Author-Institution relationships
CREATE TABLE IF NOT EXISTS fact_work_author_institutions (
    fact_id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) NOT NULL REFERENCES fact_works(work_id),
    author_id VARCHAR(255) NOT NULL REFERENCES dim_authors(author_id),
    institution_id VARCHAR(255) NOT NULL REFERENCES dim_institutions(institution_id),
    institution_name TEXT,
    institution_country VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Work-Topic relationships
CREATE TABLE IF NOT EXISTS fact_work_topics (
    fact_id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) NOT NULL REFERENCES fact_works(work_id),
    topic_id VARCHAR(255) NOT NULL REFERENCES dim_topics(topic_id),
    topic_name TEXT,
    topic_score DECIMAL(18, 15),
    domain_id VARCHAR(255),
    domain_name VARCHAR(255),
    field_id VARCHAR(255),
    field_name VARCHAR(255),
    subfield_id VARCHAR(255),
    subfield_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Work-Keyword relationships
CREATE TABLE IF NOT EXISTS fact_work_keywords (
    fact_id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) NOT NULL REFERENCES fact_works(work_id),
    keyword_id VARCHAR(255) NOT NULL REFERENCES dim_keywords(keyword_id),
    keyword_name TEXT,
    keyword_score DECIMAL(18, 15),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Work-Concept relationships (optional)
CREATE TABLE IF NOT EXISTS fact_work_concepts (
    fact_id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) NOT NULL REFERENCES fact_works(work_id),
    concept_id VARCHAR(255) NOT NULL REFERENCES dim_concepts(concept_id),
    concept_name TEXT,
    concept_score DECIMAL(18, 15),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Work Citation by Year (counts_by_year historical data)
CREATE TABLE IF NOT EXISTS fact_work_citation_year (
    fact_id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) NOT NULL REFERENCES fact_works(work_id),
    year INT NOT NULL,
    cited_by_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(work_id, year)
);

-- Fact: Work Locations (alternative sources/versions of the work)
CREATE TABLE IF NOT EXISTS fact_work_locations (
    fact_id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) NOT NULL REFERENCES fact_works(work_id),
    location_id TEXT,
    is_oa BOOLEAN DEFAULT FALSE,
    landing_page_url TEXT,
    pdf_url TEXT,
    source_id VARCHAR(255) REFERENCES dim_sources(source_id),
    source_name TEXT,
    source_type VARCHAR(50),
    license_id VARCHAR(100),
    version VARCHAR(100),
    is_accepted BOOLEAN DEFAULT FALSE,
    is_published BOOLEAN DEFAULT FALSE,
    raw_source_name TEXT,
    raw_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Indexes for Better Query Performance
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_dim_topics_domain ON dim_topics(domain_id);
CREATE INDEX IF NOT EXISTS idx_dim_topics_field ON dim_topics(field_id);
CREATE INDEX IF NOT EXISTS idx_dim_topics_subfield ON dim_topics(subfield_id);
CREATE INDEX IF NOT EXISTS idx_dim_fields_domain ON dim_fields(domain_id);

CREATE INDEX IF NOT EXISTS idx_fact_works_year ON fact_works(publication_year);
CREATE INDEX IF NOT EXISTS idx_fact_works_source ON fact_works(source_id);
CREATE INDEX IF NOT EXISTS idx_fact_works_citations ON fact_works(cited_by_count);
CREATE INDEX IF NOT EXISTS idx_fact_works_oa ON fact_works(is_oa);

CREATE INDEX IF NOT EXISTS idx_fact_work_authors_work ON fact_work_authors(work_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_authors_author ON fact_work_authors(author_id);

CREATE INDEX IF NOT EXISTS idx_fact_work_author_institutions_work ON fact_work_author_institutions(work_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_author_institutions_author ON fact_work_author_institutions(author_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_author_institutions_institution ON fact_work_author_institutions(institution_id);

CREATE INDEX IF NOT EXISTS idx_fact_work_topics_work ON fact_work_topics(work_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_topics_topic ON fact_work_topics(topic_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_topics_domain ON fact_work_topics(domain_id);

CREATE INDEX IF NOT EXISTS idx_fact_work_keywords_work ON fact_work_keywords(work_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_keywords_keyword ON fact_work_keywords(keyword_id);

CREATE INDEX IF NOT EXISTS idx_fact_work_citation_year_work ON fact_work_citation_year(work_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_citation_year_year ON fact_work_citation_year(year);

CREATE INDEX IF NOT EXISTS idx_fact_work_locations_work ON fact_work_locations(work_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_locations_source ON fact_work_locations(source_id);
CREATE INDEX IF NOT EXISTS idx_fact_work_locations_is_oa ON fact_work_locations(is_oa);

-- ============================================================================
-- Analytical Views for Power BI / Superset
-- ============================================================================

-- View: Works with basic metadata
CREATE OR REPLACE VIEW vw_works_overview AS
SELECT 
    fw.work_id,
    fw.doi,
    fw.title,
    fw.publication_year,
    fw.publication_date,
    fw.language,
    fw.work_type,
    fw.source_name,
    fw.cited_by_count,
    fw.is_oa,
    fw.oa_status,
    fw.author_count,
    fw.institutions_distinct_count,
    fw.countries_distinct_count
FROM fact_works fw
ORDER BY fw.publication_year DESC;

-- View: Works with topics and topics hierarchy
CREATE OR REPLACE VIEW vw_works_with_topics AS
SELECT 
    fw.work_id,
    fw.title,
    fw.publication_year,
    fw.cited_by_count,
    fwt.topic_name,
    fwt.topic_score,
    fwt.domain_name,
    fwt.field_name,
    fwt.subfield_name
FROM fact_works fw
LEFT JOIN fact_work_topics fwt ON fw.work_id = fwt.work_id
ORDER BY fw.publication_year DESC, fwt.topic_score DESC;

-- View: Publication trends by year
CREATE OR REPLACE VIEW vw_publication_trends_by_year AS
SELECT 
    fw.publication_year,
    COUNT(*) as work_count,
    SUM(CASE WHEN fw.is_oa THEN 1 ELSE 0 END) as oa_count,
    ROUND(AVG(fw.cited_by_count), 2) as avg_citations,
    MAX(fw.cited_by_count) as max_citations
FROM fact_works fw
WHERE fw.publication_year IS NOT NULL
GROUP BY fw.publication_year
ORDER BY fw.publication_year DESC;

-- View: Publication distribution by OA status
CREATE OR REPLACE VIEW vw_publication_distribution_oa AS
SELECT 
    fw.publication_year,
    fw.is_oa,
    COUNT(*) as work_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY fw.publication_year), 2) as percentage
FROM fact_works fw
WHERE fw.publication_year IS NOT NULL
GROUP BY fw.publication_year, fw.is_oa
ORDER BY fw.publication_year DESC;

-- View: Top authors by publication count
CREATE OR REPLACE VIEW vw_top_authors_by_publications AS
SELECT 
    da.author_id,
    da.author_name,
    COUNT(DISTINCT fwa.work_id) as publication_count,
    ROUND(AVG(fw.cited_by_count), 2) as avg_citations_per_work
FROM dim_authors da
LEFT JOIN fact_work_authors fwa ON da.author_id = fwa.author_id
LEFT JOIN fact_works fw ON fwa.work_id = fw.work_id
GROUP BY da.author_id, da.author_name
HAVING COUNT(DISTINCT fwa.work_id) > 0
ORDER BY publication_count DESC;

-- View: Top institutions by research output
CREATE OR REPLACE VIEW vw_top_institutions_by_output AS
SELECT 
    di.institution_id,
    di.institution_name,
    di.country_code,
    COUNT(DISTINCT fwai.work_id) as work_count,
    ROUND(AVG(fw.cited_by_count), 2) as avg_citations
FROM dim_institutions di
LEFT JOIN fact_work_author_institutions fwai ON di.institution_id = fwai.institution_id
LEFT JOIN fact_works fw ON fwai.work_id = fw.work_id
GROUP BY di.institution_id, di.institution_name, di.country_code
HAVING COUNT(DISTINCT fwai.work_id) > 0
ORDER BY work_count DESC;

-- View: Top topics by research activity
CREATE OR REPLACE VIEW vw_top_topics_by_activity AS
SELECT 
    dt.topic_id,
    dt.topic_name,
    dt.domain_name,
    dt.field_name,
    dt.subfield_name,
    COUNT(DISTINCT fwt.work_id) as work_count,
    ROUND(AVG(fwt.topic_score), 6) as avg_topic_score,
    ROUND(AVG(fw.cited_by_count), 2) as avg_citations
FROM dim_topics dt
LEFT JOIN fact_work_topics fwt ON dt.topic_id = fwt.topic_id
LEFT JOIN fact_works fw ON fwt.work_id = fw.work_id
GROUP BY dt.topic_id, dt.topic_name, dt.domain_name, dt.field_name, dt.subfield_name
HAVING COUNT(DISTINCT fwt.work_id) > 0
ORDER BY work_count DESC;

-- View: Geographic distribution
CREATE OR REPLACE VIEW vw_geographic_distribution AS
SELECT 
    fwai.institution_country as country_code,
    COUNT(DISTINCT di.institution_id) as institution_count,
    COUNT(DISTINCT fwai.work_id) as work_count,
    ROUND(AVG(fw.cited_by_count), 2) as avg_citations
FROM fact_work_author_institutions fwai
LEFT JOIN dim_institutions di ON fwai.institution_id = di.institution_id
LEFT JOIN fact_works fw ON fwai.work_id = fw.work_id
WHERE fwai.institution_country IS NOT NULL
GROUP BY fwai.institution_country
ORDER BY work_count DESC;

-- View: Source/Journal performance
CREATE OR REPLACE VIEW vw_source_performance AS
SELECT 
    ds.source_id,
    ds.source_name,
    ds.host_organization_name,
    ds.is_oa,
    ds.is_core,
    COUNT(fw.work_id) as work_count,
    ROUND(AVG(fw.cited_by_count), 2) as avg_citations,
    MAX(fw.publication_year) as last_publication_year
FROM dim_sources ds
LEFT JOIN fact_works fw ON ds.source_id = fw.source_id
GROUP BY ds.source_id, ds.source_name, ds.host_organization_name, ds.is_oa, ds.is_core
ORDER BY work_count DESC;

-- View: Top keywords
CREATE OR REPLACE VIEW vw_top_keywords AS
SELECT 
    dk.keyword_id,
    dk.keyword_name,
    COUNT(DISTINCT fwk.work_id) as work_count,
    ROUND(AVG(fwk.keyword_score), 6) as avg_score
FROM dim_keywords dk
LEFT JOIN fact_work_keywords fwk ON dk.keyword_id = fwk.keyword_id
GROUP BY dk.keyword_id, dk.keyword_name
HAVING COUNT(DISTINCT fwk.work_id) > 0
ORDER BY work_count DESC;
