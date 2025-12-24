

import json
import logging
import csv
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ValidationError

class MetadataModel(BaseModel):
    # optional fields
    title: str | None = None
    url: str
    external_id: str
    publish_date: str | None = None
    datetime: str | None = None
    first_publish_date: str | None = None
    website: str | None = None
    sections: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    thumb: str | None = None

    # list fields


class QdrantDocument(BaseModel):
    text: str
    metadata: MetadataModel

import json
import logging
import csv
import re
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

# --- 1. SETUP LOGGING ---
# This logger captures the "story" of each document
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("output/pipeline.log", mode='w', encoding='utf-8'), # 'w' overwrites log each run
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CapitolPipeline")

class DataTransformer:
    def __init__(self):
        # We can track state here if needed (e.g. total processed count)
        pass
        

    def clean_text(self, raw_string):
        if raw_string is None:
            return ""
        
        # Defensive Coding
        if not isinstance(raw_string, str):
            raw_string = str(raw_string)
    
        # CHANGE HERE: Use \n separator to preserve paragraph structure
        # and strip=True to remove leading/trailing whitespace from each block
        soup = BeautifulSoup(raw_string, "html.parser")
        text = soup.get_text(separator="\n")
        
        return text

    def clean_html_old(self, raw_html):
        """Helper: Removes HTML tags and returns clean text."""
        if not raw_html:
            return ""
        soup = BeautifulSoup(raw_html, "html.parser")

        text = soup.get_text(separator="\n")
        return text

    def clean_html(self, raw_html):
        """Helper: Removes HTML tags and returns clean text."""
        if not raw_html:
            return "", False
        soup = BeautifulSoup(raw_html, "html.parser")

        tags_found = [tag.name for tag in soup.find_all()]
        # if tags_found:
        #     print(f"DEBUG: Found HTML tags: {tags_found}")
        # else:
        #     print("DEBUG: No HTML tags found (Plain text).")
    
        text = soup.get_text(separator="\n", strip=True)
        return text, tags_found

    def get_text_body_old_working(self, raw_doc):
        """
        Loops through 'content_elements' to build the full article text.
        """
        text_parts = []
        elements = raw_doc.get('content_elements', [])
        
        for element in elements:
            el_type = element.get('type', '')
            
            # Case A: Standard Text or Headers
            if el_type in ['text']:
                content = element.get('content', '')
                if content:
                    text_parts.append(self.clean_html(content))
        
        # Join all parts with double newlines to separate paragraphs
        return " ".join(text_parts)

    def get_text_body_aj_test(self, raw_doc):
        """
        Loops through 'content_elements' to build the full article text.
        """
        text_parts = []
        elements = raw_doc.get('content_elements', [])

        full_text = ""
        is_prev_html = False
        
        for element in elements:
            el_type = element.get('type', '')
            
            # Case A: Standard Text or Headers
            if el_type in ['text']:
                content = element.get('content', '')
                if content:
                    # text_parts.append(self.clean_html(content))
                    cleaned_text, is_html = self.clean_html(content)
                    if is_html:
                        full_text = full_text + "\n" + cleaned_text.strip()
                        is_prev_html = True
                    elif is_prev_html:
                        full_text = full_text + "\n" + cleaned_text.strip()
                        is_prev_html = False
                    else:
                        full_text = full_text + " " + cleaned_text.strip()
        
        # Join all parts with double newlines to separate paragraphs
        # return " ".join(text_parts)
        return full_text

    def get_text_body(self, raw_doc):
        """
        Loops through 'content_elements' to build the full article text.
        """
        text_parts = []
        elements = raw_doc.get('content_elements', [])

        full_text = ""
        full_text_with_html = ""
        is_prev_html = False
        
        for element in elements:
            el_type = element.get('type', '')
            
            # Case A: Standard Text or Headers
            if el_type in ['text']:
                content = element.get('content', '')
                if content:
                    full_text_with_html = full_text_with_html + " " + content
                    full_text, is_html = self.clean_html(full_text_with_html)
        
        return full_text.strip()

    def extract_title(self, article):
        headlines = article.get("headlines")
        if not isinstance(headlines, dict):
            return ""
        raw_title = headlines.get("basic", "")
        return self.clean_text(raw_title)

    def extract_url_general(self, raw_doc: dict) -> str:
        """
        Build a clean URL from the raw doc.
    
        Rules:
        - If `website_url` is a relative path and `canonical_website` is present,
          build: https://www.{canonical_website}.com{website_url}
        - If `website_url` is already a full URL, return it as-is.
        - Otherwise, if `canonical_url` is a full URL, use that.
        - If nothing valid is found, return "" so the caller can treat it as missing.
        """
        raw_relative = raw_doc.get("website_url")
        raw_canonical = raw_doc.get("canonical_url")
        site_id = raw_doc.get("canonical_website")
    
        # Normalize strings safely
        def norm(s: Any) -> str:
            if not isinstance(s, str):
                return ""
            return s.strip()
    
        relative_path = norm(raw_relative)
        canonical_url = norm(raw_canonical)
    
        # 1) If we have a site_id and a relative path starting with '/', build absolute URL
        if isinstance(site_id, str) and site_id.strip():
            site_id = site_id.strip()
            base_domain = f"https://www.{site_id}.com"
    
            if relative_path.startswith("/"):
                return base_domain + relative_path
    
            # If website_url is already a full URL, just use it
            if relative_path.startswith("http://") or relative_path.startswith("https://"):
                return relative_path
    
        # 2) No usable site_id or no relative path.
        #    Accept website_url if it looks like a full URL.
        if relative_path.startswith("http://") or relative_path.startswith("https://"):
            return relative_path
    
        # 3) Fallback: canonical_url, but ONLY if it looks like a proper URL
        if canonical_url.startswith("http://") or canonical_url.startswith("https://"):
            return canonical_url
    
        # 4) Nothing valid → treat as missing.
        return ""

    def extract_id(self, raw_doc):
        """
        Extracts the unique identifier (_id) from the raw document.
        """
        doc_id = raw_doc.get('_id')
        if doc_id is None:
            return None
        return str(doc_id)

    def format_date_iso(self, date_str: Any) -> Optional[str]:
            """
            Normalize to ISO 8601 UTC.
            """
            if not date_str or not isinstance(date_str, str) or date_str[-1] != "Z":
                return None
    
            try:
                return date_str
    
            except (ValueError, TypeError):
                return None

    def extract_publish_date(self, raw_doc):
        # Try root level first, then additional_properties
        date_str = raw_doc.get('publish_date')
        return self.format_date_iso(date_str)

    def extract_first_publish_date(self, raw_doc):
        date_str = raw_doc.get('first_publish_date')
        return self.format_date_iso(date_str)

    def extract_datetime(self, raw_doc):
        """
        Logic: Prioritize 'last_updated_date' to represent current state.
        If missing, fallback to 'publish_date'.
        """
        update_str = raw_doc.get('last_updated_date')
        formatted = self.format_date_iso(update_str)
        if formatted:
            return formatted
        return self.extract_publish_date(raw_doc)

    def extract_website(self, raw_doc):
        # 'canonical_website' is the authoritative field in the raw data
        site = raw_doc.get('canonical_website')
        return str(site) if site else None

    def extract_sections(self, raw_doc):
        """
        Extracts the list of section names from the document's taxonomy.
        """
        taxonomy = raw_doc.get('taxonomy', {})
        if not taxonomy:
            return []

        raw_sections = taxonomy.get('sections', [])
        
        if not isinstance(raw_sections, list):
            return []
        
        section_names = []
        for section in raw_sections:
            if isinstance(section, dict):
                name = section.get('name')
                if name and isinstance(name, str):
                    section_names.append(name.strip())
                    
        return list(dict.fromkeys(section_names))

    def extract_categories(self, raw_doc):
        taxonomy = raw_doc.get('taxonomy', {})
        if not taxonomy:
            return []

        raw_categories = taxonomy.get('categories', [])
        if not isinstance(raw_categories, list):
            return []

        iab_cats = [
            c for c in raw_categories
            if isinstance(c, dict) and c.get('classifier') == 'iab_content_taxonomy'
        ]

        iab_cats.sort(key=lambda x: x.get('score', 0) or 0, reverse=True)

        final_categories = []
        seen = set()

        for cat in iab_cats:
            name = cat.get('name')
            if not name:
                continue

            cleaned = self.clean_text(name)
            if cleaned in seen:
                continue 

            seen.add(cleaned)
            final_categories.append(cleaned)

            if len(final_categories) == 5:
                break

        return final_categories

    def extract_tags(self, raw_doc):
        """
        Extracts tags for metadata.tags.
        """
        taxonomy = raw_doc.get("taxonomy", {})
        if not isinstance(taxonomy, dict):
            return []

        raw_tags = taxonomy.get("tags", [])
        if not isinstance(raw_tags, list):
            return []

        tags = []
        seen = set()

        for tag in raw_tags:
            if not isinstance(tag, dict):
                continue

            slug = tag.get("slug")
            if not isinstance(slug, str):
                continue

            slug = slug.strip()
            if not slug or slug in seen:
                continue

            seen.add(slug)
            tags.append(slug)

            if len(tags) == 5:
                break

        return tags

    def extract_thumb(self, raw_doc):
        """
        Extracts metadata.thumb (optional).
        Converts relative resizeUrl to absolute URL.
        """
        promo_items = raw_doc.get("promo_items") or {}
        if not isinstance(promo_items, dict): return None

        basic = promo_items.get("basic")
        if not isinstance(basic, dict): return None

        add_props = basic.get("additional_properties") or {}
        if not isinstance(add_props, dict): return None

        rel = add_props.get("resizeUrl")
        site = raw_doc.get("canonical_website")

        if isinstance(rel, str) and rel.strip() and isinstance(site, str) and site.strip():
            return f"https://www.{site.strip().lower()}.com{rel.strip()}"

        return None

    # --- MAIN PROCESSOR WITH DETAILED LOGGING ---
    # --- MAIN PROCESSOR WITH DETAILED LOGGING ---
    def process_document(self, raw_doc: dict) -> tuple[Optional[Dict], Dict]:
        """
        Orchestrates transformation with comprehensive logging for ALL fields.
        Returns: (Processed Document OR None, Report Dictionary)
        """
        if not isinstance(raw_doc, dict):
            logger.error(f"❌ SKIPPING: raw_doc is not a dict (got {type(raw_doc).__name__})")
            return None, {
                "id": "UNKNOWN_ID",
                "status": "SKIPPED",
                "reason": "Invalid document type (expected object)"
            }

        doc_id = raw_doc.get('_id', 'UNKNOWN_ID')
        report: Dict[str, Any] = {"id": doc_id, "status": "SKIPPED", "reason": "Unknown"}

        logger.info(f"📄 Processing Document ID: {doc_id}")

        # --- 1. CRITICAL CHECKS (Fail if missing) ---
        external_id = self.extract_id(raw_doc)
        if not external_id:
            logger.error("❌ SKIPPING: Document missing '_id'.")
            report["reason"] = "Missing ID"
            return None, report

        url = self.extract_url_general(raw_doc)
        if not url:
            logger.warning(f"   ⚠️ SKIPPING {doc_id}: Required field 'url' not found.")
            report["reason"] = "Missing URL"
            return None, report
        logger.info(f"   ✅ URL found: {url}")

        text_content = self.get_text_body(raw_doc)
        if not text_content or not text_content.strip():
            logger.warning(f"   ⚠️ SKIPPING {doc_id}: Required field 'text' is empty.")
            report["reason"] = "Missing Text"
            return None, report
        logger.info(f"   ✅ Text body found ({len(text_content)} chars).")

        # --- 2. OPTIONAL FIELD CHECKS (Log existence) ---
        title = self.extract_title(raw_doc)
        if title:
            logger.info(f"   ☑️ Title found: '{title[:30]}...'")
        else:
            logger.info("   ⚪ Title NOT found.")

        pub_date = self.extract_publish_date(raw_doc)
        if pub_date:
            logger.info(f"   ☑️ Publish Date found: {pub_date}")
        else:
            logger.info("   ⚪ Publish Date NOT found.")

        first_pub = self.extract_first_publish_date(raw_doc)
        if first_pub:
            logger.info(f"   ☑️ First Publish Date found: {first_pub}")
        else:
            logger.info("   ⚪ First Publish Date NOT found.")

        doc_time = self.extract_datetime(raw_doc)
        if doc_time:
            logger.info(f"   ☑️ Datetime found: {doc_time}")
        else:
            logger.info("   ⚪ Datetime NOT found.")

        website = self.extract_website(raw_doc)
        if website:
            logger.info(f"   ☑️ Website found: {website}")
        else:
            logger.info("   ⚪ Website NOT found.")

        thumb = self.extract_thumb(raw_doc)
        if thumb:
            logger.info("   ☑️ Thumbnail found.")
        else:
            logger.info("   ⚪ Thumbnail NOT found.")

        # Taxonomy lists
        sections = self.extract_sections(raw_doc)
        categories = self.extract_categories(raw_doc)
        tags = self.extract_tags(raw_doc)

        if sections:
            logger.info(f"   ☑️ Sections found: {len(sections)} items")
        else:
            logger.info("   ⚪ Sections empty.")

        if categories:
            logger.info(f"   ☑️ Categories found: {len(categories)} items")
        else:
            logger.info("   ⚪ Categories empty.")

        if tags:
            logger.info(f"   ☑️ Tags found: {len(tags)} items")
        else:
            logger.info("   ⚪ Tags empty.")

        # --- 3. BUILD METADATA ---
        try:
            # Build in the exact order you want to see in JSON
            metadata: Dict[str, Any] = {}

            # 1. Core identity
            if title:
                metadata["title"] = title
            metadata["url"] = url
            metadata["external_id"] = external_id

            # 2. Dates
            if pub_date:
                metadata["publish_date"] = pub_date
            if doc_time:
                metadata["datetime"] = doc_time
            if first_pub:
                metadata["first_publish_date"] = first_pub

            # 3. Source
            if website:
                metadata["website"] = website

            # 4. Taxonomy lists (always included, even if empty)
            metadata["sections"] = sections
            metadata["categories"] = categories
            metadata["tags"] = tags

            # 5. Media
            if thumb:
                metadata["thumb"] = thumb

            try:
                meta_model = MetadataModel(**metadata)
                doc_model = QdrantDocument(
                    text=text_content,
                    metadata=meta_model,
                )
            except ValidationError as ve:
                logger.error(f"   ❌ Validation failed for {doc_id}: {ve}")
                report["reason"] = "Schema validation failed"
                return None, report

            logger.info(f"   🎉 Document {doc_id} successfully transformed.")
            report["status"] = "SUCCESS"
            report["reason"] = ""

            # exclude_none=True ensures we don't output "title": null
            return doc_model.model_dump(exclude_none=True), report

        except Exception as e:
            logger.error(f"   💥 CRITICAL ERROR on {doc_id}: {e}")
            report["reason"] = f"Crash: {str(e)}"
            return None, report
dead_letter_path = "output/dead_letter_queue.jsonl"


def print_telemetry_dashboard(report_data):
    """
    Prints a simple reliability dashboard to the console.
    """
    total = len(report_data)
    if total == 0: return

    # 1. Calculate Metrics
    success = sum(1 for r in report_data if r['status'] == 'SUCCESS')
    failed = total - success
    success_rate = (success / total) * 100

    # 2. Group Failure Reasons
    failures = {}
    for r in report_data:
        if r['status'] != 'SUCCESS':
            reason = r.get('reason', 'Unknown')
            failures[reason] = failures.get(reason, 0) + 1

    # 3. Print The Dashboard
    print("\n" + "="*40)
    print(" 📊 PIPELINE HEALTH DASHBOARD")
    print("="*40)
    print(f" • Total Documents:   {total}")
    print(f" • Success Rate:      {success_rate:.1f}%")
    print(f" • Failed/Skipped:    {failed}")
    
    if failures:
        print("\n ⚠️  FAILURE BREAKDOWN (ROOT CAUSE)")
        print("-" * 40)
        # Sort by count (highest first)
        for reason, count in sorted(failures.items(), key=lambda x: x[1], reverse=True):
            print(f"   - {reason:<20} : {count} docs")
    print("="*40 + "\n")


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    try:
        with open("data/raw_customer_api.json", "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        print("❌ Error: 'raw_customer_api.json' not found.")
        exit(1)

    transformer = DataTransformer()
    
    # Key = external_id, Value = processed_doc
    # Using a dict ensures that if the same ID appears later, it overwrites the old one (Update).
    valid_docs_map = {}
    report_data = []

    print(f"🚀 Starting ingestion of {len(raw_data)} documents...")

    for doc in raw_data:
        # 1. Process EVERY document first
        processed_doc, report = transformer.process_document(doc)
        report_data.append(report)
        
        if processed_doc:
            # 2. Check for ID and handle Upsert (Update/Insert)
            ext_id = processed_doc.get('metadata', {}).get('external_id')
            if ext_id:
                if ext_id in valid_docs_map:
                    logger.info(f"   🔄 UPDATING: Overwriting existing record for ID {ext_id}")
                
                # This line handles both Insert (new key) and Update (overwrite value)
                valid_docs_map[ext_id] = processed_doc
        else:
            # 3. Handle Failures (Missing URL/ID/Text)
            doc_id = doc.get('_id', 'UNKNOWN')
            with open(dead_letter_path, "a", encoding="utf-8") as dl:
                record = {
                    "id": doc_id,
                    "reason": report.get("reason", "unknown"),
                    "raw_doc": doc,
                }
                dl.write(json.dumps(record, ensure_ascii=False) + "\n")

    # Convert map back to list for final JSON output
    valid_docs = list(valid_docs_map.values())

    # Save Valid Output (JSON)
    with open("output/processed_output_updated_2.json", "w", encoding="utf-8") as f:
        json.dump(valid_docs, f, indent=2, ensure_ascii=False)

    # Save Summary Report (CSV)
    with open("output/ingestion_report.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "status", "reason"])
        writer.writeheader()
        writer.writerows(report_data)


    print_telemetry_dashboard(report_data)
    # Calculate Stats for Final Print
    total_input = len(raw_data)
    total_unique_output = len(valid_docs)
    total_failures = len([r for r in report_data if r['status'] != 'SUCCESS'])
    # Math: The missing count is the duplicates that were absorbed/merged
    total_merged = total_input - total_unique_output - total_failures

    print("\n✅ Processing Complete!")
    print(f"   - Total Input:      {total_input}")
    print(f"   - Unique Success:   {total_unique_output} (Saved to processed_output_updated.json)")
    print(f"   - Duplicates:       {total_merged} (Merged/Updated)")
    print(f"   - Failures:         {total_failures} (Sent to Dead Letter Queue)")
    print(f"   - Detailed Logs:    pipeline.log")
    print(f"   - Summary Report:   ingestion_report.csv")


def execute_transformation_step(input_file: str, output_file: str) -> List[Dict]:
    transformer = DataTransformer()
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file '{input_file}' not found.")
    
    valid_docs_map = {}
    
    for doc in raw_data:
        result, report = transformer.process_document(doc)
        
        if result: 
            ext_id = result.get('metadata', {}).get('external_id')
            if ext_id:
                valid_docs_map[ext_id] = result
    
    valid_docs = list(valid_docs_map.values())

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(valid_docs, f, indent=2, ensure_ascii=False)
    
    return valid_docs


