"""
Abstract Extractor Library
A modular library for extracting abstracts from HTML content of academic papers.
Can be used as a standalone script or imported as a module.
"""

import re
from typing import List, Dict, Optional, Tuple
from pathlib import Path


class AbstractExtractor:
    """Main class for extracting abstracts from HTML content"""
    
    def __init__(self, min_length: int = 50, max_length: int = 5000, verbose: bool = False):
        """
        Initialize the AbstractExtractor
        
        Args:
            min_length: Minimum abstract length in characters
            max_length: Maximum abstract length in characters
            verbose: Whether to print detailed information
        """
        self.min_length = min_length
        self.max_length = max_length
        self.verbose = verbose
    
    def extract(self, html_content: str) -> Optional[str]:
        """
        Extract abstract from HTML content
        
        Args:
            html_content: The HTML content as a string
            
        Returns:
            The extracted abstract or None if not found
        """
        all_abstracts = []
        
        # Method 1: Pattern-based extraction
        patterns = self._extract_patterns(html_content)
        all_abstracts.extend(patterns)
        
        # Method 2: JSON-LD structured data
        json_ld = self._extract_json_ld(html_content)
        all_abstracts.extend(json_ld)
        
        # Method 3: Meta tags
        meta_abstracts = self._extract_meta_tags(html_content)
        all_abstracts.extend(meta_abstracts)
        
        if not all_abstracts:
            if self.verbose:
                print("[INFO] No abstract candidates found")
            return None
        
        if self.verbose:
            print(f"[INFO] Found {len(all_abstracts)} abstract candidates")
        
        # Filter and rank abstracts
        filtered = self._filter_quality_abstracts(all_abstracts)
        
        if not filtered:
            if self.verbose:
                print("[INFO] No valid abstracts after filtering")
            return None
        
        # Remove duplicates and near-duplicates
        unique_abstracts = self._remove_duplicates(filtered)
        
        # Use the longest valid abstract (usually the most complete)
        if unique_abstracts:
            best_abstract = max(unique_abstracts, key=len)
            
            if self.verbose:
                word_count = len(best_abstract.split())
                print(f"[INFO] Best abstract selected ({len(best_abstract)} characters, {word_count} words)")
            
            return best_abstract
        
        return None
    
    def extract_all(self, html_content: str) -> List[str]:
        """
        Extract all valid abstracts from HTML content
        
        Args:
            html_content: The HTML content as a string
            
        Returns:
            List of extracted abstracts
        """
        all_abstracts = []
        
        # Method 1: Pattern-based extraction
        patterns = self._extract_patterns(html_content)
        all_abstracts.extend(patterns)
        
        # Method 2: JSON-LD structured data
        json_ld = self._extract_json_ld(html_content)
        all_abstracts.extend(json_ld)
        
        # Method 3: Meta tags
        meta_abstracts = self._extract_meta_tags(html_content)
        all_abstracts.extend(meta_abstracts)
        
        if not all_abstracts:
            return []
        
        # Filter and rank abstracts
        filtered = self._filter_quality_abstracts(all_abstracts)
        
        # Remove duplicates
        unique_abstracts = self._remove_duplicates(filtered)
        
        # Sort by length (longest first)
        unique_abstracts.sort(key=len, reverse=True)
        
        return unique_abstracts
    
    def extract_with_metadata(self, html_content: str) -> Optional[Dict]:
        """
        Extract abstract with metadata about the extraction
        
        Args:
            html_content: The HTML content as a string
            
        Returns:
            Dictionary with 'abstract', 'length', 'word_count', 'method', or None
        """
        abstract = self.extract(html_content)
        
        if not abstract:
            return None
        
        return {
            'abstract': abstract,
            'length': len(abstract),
            'word_count': len(abstract.split()),
            'sentence_count': len(re.split(r'[.!?]+', abstract)),
            'has_numbers': bool(re.search(r'\d', abstract)),
        }
    
    # Private methods
    
    def _extract_patterns(self, html_content: str) -> List[str]:
        """Extract abstract using comprehensive HTML patterns"""
        
        patterns = [
            # IEEE patterns
            r'<div[^>]*class="[^"]*abstract[^"]*"[^>]*>.*?</div>',
            r'<section[^>]*class="[^"]*abstract[^"]*"[^>]*>.*?</section>',
            
            # Springer patterns
            r'<div[^>]*id="Abs1"[^>]*>.*?</div>',
            r'<section[^>]*id="Abs\d+"[^>]*>.*?</section>',
            r'<div[^>]*class="[^"]*springer[^"]*abstract[^"]*"[^>]*>.*?</div>',
            
            # ScienceDirect patterns
            r'<div[^>]*class="[^"]*abstract[^"]*"[^>]*>.*?</div>',
            r'<section[^>]*data-testid="abstract[^"]*"[^>]*>.*?</section>',
            
            # ACM Digital Library patterns
            r'<div[^>]*id="[^"]*abstract[^"]*"[^>]*>.*?</div>',
            r'<section[^>]*class="[^"]*abstractSection[^"]*"[^>]*>.*?</section>',
            
            # JMLR patterns
            r'<div[^>]*class="[^"]*paper-abstract[^"]*"[^>]*>.*?</div>',
            
            # ArXiv patterns
            r'<blockquote[^>]*class="[^"]*abstract[^"]*"[^>]*>.*?</blockquote>',
            r'<span[^>]*class="[^"]*abstract[^"]*"[^>]*>.*?</span>',
            
            # Generic abstract patterns
            r'<div[^>]*role="doc-abstract"[^>]*>.*?</div>',
            r'<article[^>]*id="abstract"[^>]*>.*?</article>',
            r'<div[^>]*id="abstract"[^>]*>.*?</div>',
            r'<section[^>]*id="abstract"[^>]*>.*?</section>',
            
            # Abstract heading patterns
            r'<h\d[^>]*>Abstract</h\d>.*?(?=<h\d|<section|<div class="[^"]*(?:article|paper|publication)[^"]*"|<footer|<nav|$)',
            r'<h\d[^>]*>Summary</h\d>.*?(?=<h\d|<section|<div|<footer|$)',
            
            # OpenReview patterns
            r'<div[^>]*class="[^"]*note_content_value[^"]*"[^>]*>.*?</div>',
            
            # ResearchGate patterns
            r'<div[^>]*class="[^"]*abstract-text[^"]*"[^>]*>.*?</div>',
            
            # Semantic Scholar patterns
            r'<div[^>]*data-test-id="paper-abstract"[^>]*>.*?</div>',
        ]
        
        abstracts = []
        for pattern in patterns:
            try:
                matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
                abstracts.extend(matches)
            except re.error:
                continue
        
        return abstracts
    
    def _extract_json_ld(self, html_content: str) -> List[str]:
        """Extract abstract from JSON-LD structured data"""
        try:
            import json
            
            json_ld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
            matches = re.findall(json_ld_pattern, html_content, re.DOTALL)
            
            abstracts = []
            for match in matches:
                try:
                    data = json.loads(match)
                    
                    if isinstance(data, dict):
                        if 'description' in data:
                            abstracts.append(data['description'])
                        if 'abstract' in data:
                            abstracts.append(data['abstract'])
                        if 'author' in data and isinstance(data['author'], dict):
                            if 'description' in data['author']:
                                abstracts.append(data['author']['description'])
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
            
            return abstracts
        except Exception:
            return []
    
    def _extract_meta_tags(self, html_content: str) -> List[str]:
        """Extract abstract from meta tags"""
        abstracts = []
        
        # Meta description
        meta_desc = re.search(r'<meta[^>]*name="description"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
        if meta_desc:
            abstracts.append(meta_desc.group(1))
        
        # OG description
        og_desc = re.search(r'<meta[^>]*property="og:description"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
        if og_desc:
            abstracts.append(og_desc.group(1))
        
        # Twitter description
        twitter_desc = re.search(r'<meta[^>]*name="twitter:description"[^>]*content="([^"]*)"', html_content, re.IGNORECASE)
        if twitter_desc:
            abstracts.append(twitter_desc.group(1))
        
        return abstracts
    
    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and clean up text"""
        # Remove script and style tags completely
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove common bloat patterns
        text = re.sub(r'<(noscript|iframe|embed)[^>]*>.*?</\1>', '', text, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Decode HTML entities
        html_entities = {
            '&nbsp;': ' ', '&amp;': '&', '&lt;': '<', '&gt;': '>',
            '&quot;': '"', '&#39;': "'", '&apos;': "'", '&mdash;': '—',
            '&ndash;': '–', '&ldquo;': '"', '&rdquo;': '"', '&lsquo;': "'",
            '&rsquo;': "'", '&hellip;': '…', '&deg;': '°', '&copy;': '©',
        }
        
        for entity, char in html_entities.items():
            text = text.replace(entity, char)
        
        # Remove numeric HTML entities
        text = re.sub(r'&#?\w+;', '', text)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        # Remove "Show More" and similar patterns
        text = re.sub(r'\s*Show More.*$', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\s*\.\.\.$', '', text)
        
        return text
    
    def _filter_quality_abstracts(self, abstracts: List[str]) -> List[str]:
        """Filter abstracts by quality metrics"""
        filtered = []
        
        for abstract in abstracts:
            cleaned = self._clean_html(abstract)
            
            # Check length
            if len(cleaned) < self.min_length or len(cleaned) > self.max_length:
                continue
            
            # Check if it looks like actual abstract (not just metadata)
            word_count = len(cleaned.split())
            if word_count < 10 or word_count > 1000:
                continue
            
            # Check for minimum sentence structure
            sentences = re.split(r'[.!?]+', cleaned)
            if len([s for s in sentences if len(s.split()) > 3]) < 2:
                continue
            
            # Skip if mostly uppercase (likely a title or header)
            uppercase_ratio = sum(1 for c in cleaned if c.isupper()) / len(cleaned) if cleaned else 0
            if uppercase_ratio > 0.4:
                continue
            
            filtered.append(cleaned)
        
        return filtered
    
    def _remove_duplicates(self, abstracts: List[str]) -> List[str]:
        """Remove exact and near-duplicate abstracts"""
        unique_abstracts = []
        seen_lengths = set()
        
        for abstract in abstracts:
            # Skip if we already have an abstract of similar length
            if any(abs(len(abstract) - seen_len) < 50 for seen_len in seen_lengths):
                continue
            
            # Skip if exact duplicate
            if abstract in unique_abstracts:
                continue
            
            unique_abstracts.append(abstract)
            seen_lengths.add(len(abstract))
        
        return unique_abstracts


# Convenience functions for direct usage

def extract_abstract(html_content: str, verbose: bool = False) -> Optional[str]:
    """
    Extract the best abstract from HTML content
    
    Args:
        html_content: The HTML content as a string
        verbose: Whether to print detailed information
        
    Returns:
        The extracted abstract or None if not found
    """
    extractor = AbstractExtractor(verbose=verbose)
    return extractor.extract(html_content)


def extract_all_abstracts(html_content: str, verbose: bool = False) -> List[str]:
    """
    Extract all valid abstracts from HTML content
    
    Args:
        html_content: The HTML content as a string
        verbose: Whether to print detailed information
        
    Returns:
        List of extracted abstracts
    """
    extractor = AbstractExtractor(verbose=verbose)
    return extractor.extract_all(html_content)


def extract_with_metadata(html_content: str, verbose: bool = False) -> Optional[Dict]:
    """
    Extract abstract with metadata about the extraction
    
    Args:
        html_content: The HTML content as a string
        verbose: Whether to print detailed information
        
    Returns:
        Dictionary with abstract info or None if not found
    """
    extractor = AbstractExtractor(verbose=verbose)
    return extractor.extract_with_metadata(html_content)


# Standalone script functionality

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python abstract_extractor.py <html_file> [options]")
        print("\nOptions:")
        print("  -v, --verbose   Show detailed extraction information")
        print("  -a, --all       Extract all abstracts")
        print("  -m, --metadata  Include metadata")
        print("\nExamples:")
        print("  python abstract_extractor.py paper.html")
        print("  python abstract_extractor.py paper.html -v")
        print("  python abstract_extractor.py paper.html -a")
        sys.exit(1)
    
    html_file = sys.argv[1]
    verbose = '-v' in sys.argv or '--verbose' in sys.argv
    extract_all = '-a' in sys.argv or '--all' in sys.argv
    with_metadata = '-m' in sys.argv or '--metadata' in sys.argv
    
    # Read HTML file
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Error: File '{html_file}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)
    
    print(f"\n{'='*70}")
    print(f"Processing: {html_file}")
    print(f"{'='*70}\n")
    
    if extract_all:
        abstracts = extract_all_abstracts(html_content, verbose=verbose)
        
        if abstracts:
            print(f"✓ Found {len(abstracts)} abstract(s)\n")
            for i, abstract in enumerate(abstracts, 1):
                print(f"--- Abstract {i} ({len(abstract)} characters) ---")
                print(abstract[:300] + ("..." if len(abstract) > 300 else ""))
                print()
        else:
            print("✗ No abstracts found")
            sys.exit(1)
    else:
        if with_metadata:
            result = extract_with_metadata(html_content, verbose=verbose)
            if result:
                print(f"✓ Abstract found\n")
                print(f"Length: {result['length']} characters")
                print(f"Words: {result['word_count']}")
                print(f"Sentences: {result['sentence_count']}\n")
                print(result['abstract'])
                
                # Save to file
                output_file = Path(html_file).stem + '_abstract.txt'
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(result['abstract'])
                print(f"\n✓ Abstract saved to '{output_file}'")
            else:
                print("✗ No abstract found")
                sys.exit(1)
        else:
            abstract = extract_abstract(html_content, verbose=verbose)
            
            if abstract:
                print(f"✓ Abstract found ({len(abstract)} characters)\n")
                print(abstract[:300] + ("..." if len(abstract) > 300 else ""))
                
                # Save to file
                output_file = Path(html_file).stem + '_abstract.txt'
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(abstract)
                print(f"\n✓ Abstract saved to '{output_file}'")
            else:
                print("✗ No abstract found")
                sys.exit(1)
