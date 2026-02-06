#!/usr/bin/env python3
"""
ACF Guidance — HTML to PDF Converter
======================================
Converts saved page_content.html files to PDFs for rows where the
guidance document was embedded in the webpage rather than available
as a separate downloadable file.

Also updates metadata.json to reflect the conversion.

Usage:
  python html_to_pdf.py                    # Convert all eligible rows
  python html_to_pdf.py --dry-run          # Show what would be converted
  python html_to_pdf.py --start-row 3243   # Start from specific row
"""

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from fpdf import FPDF

DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output")
DEFAULT_REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")


class GuidancePDF(FPDF):
    """Custom PDF class for guidance documents."""

    def __init__(self, title="", doc_number="", office="", issue_date="", source_url=""):
        super().__init__()
        self.doc_title = title
        self.doc_number = doc_number
        self.office = office
        self.issue_date = issue_date
        self.source_url = source_url

    def header(self):
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 100, 100)
        header_text = f"ACF {self.office}"
        if self.doc_number:
            header_text += f" | {self.doc_number}"
        self.cell(0, 5, header_text, new_x="LMARGIN", new_y="NEXT", align="L")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}} | Source: {self.source_url[:80]}",
                  new_x="LMARGIN", new_y="TOP", align="C")


def sha256_file(filepath):
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_text(text):
    """Clean extracted text for PDF rendering."""
    # Remove excessive whitespace
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    # Remove leading/trailing whitespace per line
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    # Remove common navigation/footer junk
    for junk in [
        "Skip to main content",
        "An official website of the United States government",
        "Here's how you know",
        "Official websites use .gov",
        "Secure .gov websites use HTTPS",
        "Share sensitive information only on official",
    ]:
        text = text.replace(junk, "")
    return text.strip()


def extract_content_from_html(html_path):
    """Extract main content text from an HTML file."""
    with open(html_path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")

    # Remove script, style, nav, footer elements
    for tag in soup.find_all(["script", "style", "nav", "footer", "header",
                               "noscript", "iframe"]):
        tag.decompose()

    # Try to find main content area
    main = (soup.find("main") or
            soup.find("article") or
            soup.find("div", {"class": re.compile(r"content|main|body", re.I)}) or
            soup.find("div", {"id": re.compile(r"content|main|body", re.I)}))

    if main:
        content = main
    else:
        content = soup.find("body") or soup

    # Extract text with some structure
    sections = []
    current_section = {"heading": "", "paragraphs": []}

    for element in content.descendants:
        if element.name in ["h1", "h2", "h3", "h4"]:
            # Save previous section
            if current_section["paragraphs"]:
                sections.append(current_section)
            current_section = {
                "heading": element.get_text(strip=True),
                "level": int(element.name[1]),
                "paragraphs": []
            }
        elif element.name in ["p", "li"]:
            text = element.get_text(strip=True)
            if text and len(text) > 5:
                prefix = "  - " if element.name == "li" else ""
                current_section["paragraphs"].append(prefix + text)
        elif element.name in ["td", "th"]:
            text = element.get_text(strip=True)
            if text:
                current_section["paragraphs"].append(text)

    # Don't forget the last section
    if current_section["paragraphs"]:
        sections.append(current_section)

    return sections


def create_pdf(sections, metadata, output_path):
    """Create a PDF from extracted content sections."""
    title = metadata.get("title", "")
    doc_number = metadata.get("doc_number", "")
    office = metadata.get("office", "")
    issue_date = metadata.get("issue_date", "")
    source_url = ""
    if metadata.get("urls"):
        source_url = metadata["urls"][0].get("url", "")

    pdf = GuidancePDF(
        title=title,
        doc_number=doc_number,
        office=office,
        issue_date=issue_date,
        source_url=source_url
    )
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(0, 0, 0)
    if title:
        pdf.multi_cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # Metadata line
    meta_parts = []
    if doc_number:
        meta_parts.append(f"Document: {doc_number}")
    if office:
        meta_parts.append(f"Office: {office}")
    if issue_date:
        meta_parts.append(f"Date: {issue_date[:10]}")

    if meta_parts:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(80, 80, 80)
        pdf.multi_cell(0, 5, " | ".join(meta_parts), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

    if source_url:
        pdf.set_font("Helvetica", "I", 8)
        pdf.set_text_color(0, 0, 200)
        pdf.multi_cell(0, 5, f"Source: {source_url}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(5)

    # Content
    pdf.set_text_color(0, 0, 0)

    for section in sections:
        heading = section.get("heading", "")
        level = section.get("level", 2)

        if heading:
            if level == 1:
                pdf.set_font("Helvetica", "B", 14)
            elif level == 2:
                pdf.set_font("Helvetica", "B", 12)
            else:
                pdf.set_font("Helvetica", "B", 11)

            pdf.ln(3)
            # Encode safely
            safe_heading = heading.encode('latin-1', 'replace').decode('latin-1')
            pdf.multi_cell(0, 6, safe_heading, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

        pdf.set_font("Helvetica", "", 10)
        for para in section["paragraphs"]:
            # Encode safely for PDF (latin-1 compatible)
            safe_para = para.encode('latin-1', 'replace').decode('latin-1')
            if safe_para:
                pdf.multi_cell(0, 5, safe_para, new_x="LMARGIN", new_y="NEXT")
                pdf.ln(2)

    pdf.output(output_path)


def process_row(row_dir, dry_run=False):
    """Process a single row folder — convert HTML to PDF if applicable."""
    meta_path = os.path.join(row_dir, "metadata.json")
    html_path = os.path.join(row_dir, "page_content.html")

    if not os.path.exists(meta_path):
        return None
    if not os.path.exists(html_path):
        return None

    with open(meta_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    # Only process rows with no_documents status, or rows that only have page_content.html
    status = metadata.get("status", "")
    if status not in ("no_documents",):
        return None

    folder_name = os.path.basename(row_dir)

    if dry_run:
        print(f"  [DRY RUN] Would convert: {folder_name} — {metadata.get('doc_number', '')} "
              f"({metadata.get('office', '')})")
        return "dry_run"

    # Extract content
    sections = extract_content_from_html(html_path)

    if not sections:
        print(f"  [SKIP] {folder_name}: No extractable content in HTML")
        return "empty"

    # Generate PDF filename
    doc_num = metadata.get("doc_number", "").strip()
    if doc_num:
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
        pdf_name = f"{safe_name}_converted.pdf"
    else:
        pdf_name = f"guidance_{folder_name}_converted.pdf"

    pdf_path = os.path.join(row_dir, pdf_name)

    try:
        create_pdf(sections, metadata, pdf_path)
    except Exception as e:
        print(f"  [ERROR] {folder_name}: PDF creation failed — {e}")
        return "error"

    file_size = os.path.getsize(pdf_path)
    file_hash = sha256_file(pdf_path)

    # Update metadata
    source_url = ""
    if metadata.get("urls"):
        source_url = metadata["urls"][0].get("url", "")

    metadata["files_downloaded"].append({
        "filename": pdf_name,
        "source_url": source_url,
        "size_bytes": file_size,
        "sha256": file_hash,
        "content_type": "application/pdf",
        "download_type": "html_to_pdf_conversion",
        "converted_at": datetime.now().isoformat()
    })
    metadata["status"] = "success"
    metadata["conversion_note"] = "Guidance was embedded in webpage; converted HTML to PDF"

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, default=str)

    print(f"  [OK] {folder_name}: {pdf_name} ({file_size:,} bytes)")
    return "converted"


def main():
    parser = argparse.ArgumentParser(description="Convert HTML guidance pages to PDF")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output directory")
    parser.add_argument("--reports", default=DEFAULT_REPORTS, help="Reports directory")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be converted")
    parser.add_argument("--start-row", type=int, default=None, help="Start from this row number")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print("=" * 60)
    print("ACF GUIDANCE — HTML TO PDF CONVERTER")
    print("=" * 60)
    print(f"Output directory: {output_dir}")
    if args.dry_run:
        print("MODE: DRY RUN (no files will be created)")
    print()

    stats = {"converted": 0, "empty": 0, "error": 0, "dry_run": 0, "skipped": 0}
    start_time = datetime.now()

    folders = sorted(os.listdir(output_dir))
    for folder_name in folders:
        if not folder_name.isdigit():
            continue

        if args.start_row and int(folder_name) < args.start_row:
            continue

        row_dir = os.path.join(output_dir, folder_name)
        if not os.path.isdir(row_dir):
            continue

        result = process_row(row_dir, dry_run=args.dry_run)
        if result:
            stats[result] = stats.get(result, 0) + 1
        else:
            stats["skipped"] += 1

    elapsed = (datetime.now() - start_time).total_seconds()

    print()
    print("=" * 60)
    print("CONVERSION COMPLETE")
    print(f"  Duration: {elapsed:.1f} seconds")
    print(f"  Converted: {stats['converted']}")
    print(f"  Empty (no content): {stats['empty']}")
    print(f"  Errors: {stats['error']}")
    print(f"  Skipped (not eligible): {stats['skipped']}")
    if args.dry_run:
        print(f"  Would convert: {stats['dry_run']}")
    print("=" * 60)

    # Save conversion report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"conversion_report_{timestamp}.json")
    report = {
        "timestamp": timestamp,
        "duration_seconds": elapsed,
        "stats": stats
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"Report saved: {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
