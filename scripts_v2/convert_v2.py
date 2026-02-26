#!/usr/bin/env python3
"""
ACF Guidance Harvester v2 — Clean PDF Converter
=================================================
Converts validated HTML pages to clean PDFs using Playwright.

Key improvement over v1: Renders only the visible content area,
stripping navigation chrome, headers, footers, and site-wide elements.

Usage:
  python convert_v2.py                    # Convert all HTML in output_v2
  python convert_v2.py --dry-run          # Preview only
  python convert_v2.py --row 0412         # Convert specific row

Requires: playwright install chromium
"""

import os
import sys
import json
import re
import hashlib
import argparse
import logging
from datetime import datetime
from pathlib import Path

from validate import validate_content, detect_file_type

# ── Configuration ────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output_v2")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

if not os.path.exists(os.path.join(BASE_DIR, "output_v2")):
    BASE_DIR = os.path.dirname(BASE_DIR)
    OUTPUT_DIR = os.path.join(BASE_DIR, "output_v2")
    REPORTS_DIR = os.path.join(BASE_DIR, "reports")

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"

# CSS to inject for clean rendering
CLEAN_CSS = """
<style>
  /* Hide site chrome */
  nav, header, footer, .breadcrumb, .breadcrumbs, .site-header,
  .site-footer, .skip-link, .skip-nav, .navbar, .menu, .sidebar,
  .social-share, .cookie-notice, .banner, .alert-banner,
  #skip-to-content, [role="navigation"], [role="banner"],
  [role="contentinfo"], .usa-banner, .usa-header, .usa-footer,
  .usa-nav, .usa-menu, .usa-accordion {
    display: none !important;
  }

  /* Clean up the main content area */
  body {
    font-family: 'Times New Roman', serif;
    font-size: 12pt;
    line-height: 1.6;
    color: #000;
    background: #fff;
    margin: 0.75in;
    max-width: 100%;
  }

  main, article, .content, .main-content, #main-content,
  [role="main"] {
    max-width: 100%;
    margin: 0;
    padding: 0;
  }

  /* Ensure tables render properly */
  table {
    border-collapse: collapse;
    width: 100%;
    margin: 1em 0;
  }
  td, th {
    border: 1px solid #999;
    padding: 6px 8px;
  }

  /* Clean links */
  a { color: #000; text-decoration: underline; }

  /* Print-friendly */
  @media print {
    body { margin: 0; }
  }
</style>
"""

# JavaScript to clean up the page before PDF generation
CLEANUP_JS = """
() => {
    // Remove nav, header, footer, banners
    const removeSelectors = [
        'nav', 'header', 'footer', '.breadcrumb', '.breadcrumbs',
        '.site-header', '.site-footer', '.skip-link', '.skip-nav',
        '.navbar', '.menu', '.sidebar', '.social-share', '.cookie-notice',
        '.banner', '.alert-banner', '#skip-to-content',
        '[role="navigation"]', '[role="banner"]', '[role="contentinfo"]',
        '.usa-banner', '.usa-header', '.usa-footer', '.usa-nav',
        '.usa-menu', '.usa-accordion', 'script', 'noscript',
        'iframe', '.overlay', '.modal'
    ];

    removeSelectors.forEach(sel => {
        document.querySelectorAll(sel).forEach(el => el.remove());
    });

    // Remove hidden elements
    document.querySelectorAll('[style*="display: none"], [style*="display:none"], [hidden]')
        .forEach(el => el.remove());

    // Clean up the body
    document.body.style.margin = '0';
    document.body.style.padding = '20px';

    // Return the visible text length for validation
    return document.body.innerText.trim().length;
}
"""


def sha256_file(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def convert_html_to_pdf_playwright(html_path, pdf_path, logger):
    """
    Convert an HTML file to a clean PDF using Playwright.
    Injects CSS and JS to strip navigation chrome before rendering.
    """
    from playwright.sync_api import sync_playwright

    # Read the HTML
    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        html_content = f.read()

    # Inject our clean CSS before </head> or at the start
    if "</head>" in html_content.lower():
        html_content = re.sub(
            r"(</head>)", CLEAN_CSS + r"\1",
            html_content, count=1, flags=re.IGNORECASE
        )
    else:
        html_content = CLEAN_CSS + html_content

    # Write modified HTML to temp file
    temp_html = html_path + ".clean.html"
    with open(temp_html, "w", encoding="utf-8") as f:
        f.write(html_content)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Load the cleaned HTML
            page.goto(f"file:///{os.path.abspath(temp_html).replace(os.sep, '/')}")
            page.wait_for_load_state("networkidle")

            # Run cleanup JS
            visible_chars = page.evaluate(CLEANUP_JS)

            if visible_chars < 200:
                logger.debug(f"  Only {visible_chars} visible chars after cleanup")
                browser.close()
                return False, f"insufficient_content_after_cleanup ({visible_chars} chars)"

            # Generate PDF
            page.pdf(
                path=pdf_path,
                format="Letter",
                margin={"top": "0.75in", "bottom": "0.75in",
                        "left": "0.75in", "right": "0.75in"},
                print_background=False,
            )

            browser.close()

        # Validate the output PDF
        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
            return True, "ok"
        else:
            return False, "pdf_too_small"

    except Exception as e:
        logger.error(f"  Playwright error: {e}")
        return False, str(e)
    finally:
        # Cleanup temp file
        if os.path.exists(temp_html):
            os.remove(temp_html)


def convert_html_to_pdf_simple(html_path, pdf_path, metadata, logger):
    """
    Fallback: Convert HTML to a simple text-based PDF using reportlab.
    Used when Playwright is not available.
    """
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.units import inch
    except ImportError:
        logger.warning("  Neither Playwright nor reportlab available for conversion")
        return False, "no_converter_available"

    from validate import extract_visible_text

    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        html_content = f.read()

    text = extract_visible_text(html_content)
    if len(text) < 200:
        return False, "insufficient_content"

    try:
        doc = SimpleDocTemplate(pdf_path, pagesize=letter,
                                leftMargin=0.75*inch, rightMargin=0.75*inch,
                                topMargin=0.75*inch, bottomMargin=0.75*inch)

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'DocTitle', parent=styles['Heading1'],
            fontSize=14, spaceAfter=12
        )
        body_style = ParagraphStyle(
            'DocBody', parent=styles['Normal'],
            fontSize=11, leading=14, spaceAfter=8
        )

        story = []

        # Add title from metadata
        doc_num = metadata.get("doc_number", "")
        title = metadata.get("title", "")
        if doc_num or title:
            header = f"{doc_num} — {title}" if doc_num and title else (doc_num or title)
            story.append(Paragraph(header, title_style))
            story.append(Spacer(1, 12))

        # Split text into paragraphs
        paragraphs = [p.strip() for p in text.split('\n') if p.strip()]
        for para in paragraphs:
            # Escape XML special chars for reportlab
            para = para.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            try:
                story.append(Paragraph(para, body_style))
            except Exception:
                # If paragraph causes issues, add as plain text
                story.append(Paragraph(para[:500], body_style))

        doc.build(story)
        return True, "ok"

    except Exception as e:
        return False, str(e)


def process_row(row_dir, dry_run=False, use_playwright=True, logger=None):
    """
    Process a single row directory: find HTML files that need conversion,
    convert them to PDF.
    """
    if logger is None:
        logger = logging.getLogger("acf_v2.convert")

    meta_path = os.path.join(row_dir, "metadata.json")
    if not os.path.exists(meta_path):
        return None

    with open(meta_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    # Check if any files need conversion
    files = metadata.get("files_downloaded", [])
    needs_conversion = any(f.get("needs_conversion") for f in files)

    # Also check for HTML files without the flag
    html_files = [f for f in os.listdir(row_dir)
                  if f.endswith((".html", ".htm")) and f != "metadata.json"
                  and not f.endswith(".clean.html")]

    has_pdf = any(f.endswith(".pdf") for f in os.listdir(row_dir) if f != "metadata.json")

    if not needs_conversion and (not html_files or has_pdf):
        return None  # Nothing to do

    row_id = os.path.basename(row_dir)

    if dry_run:
        logger.info(f"  [DRY] {row_id}: Would convert {len(html_files)} HTML file(s)")
        return {"row": row_id, "action": "would_convert", "files": len(html_files)}

    converted = 0
    failed = 0

    for html_file in html_files:
        html_path = os.path.join(row_dir, html_file)

        # Validate the HTML first
        vr = validate_content(html_path)
        if not vr.valid:
            logger.debug(f"  {row_id}: Skipping {html_file} — validation: {vr.reason}")
            failed += 1
            continue

        # Generate PDF filename
        doc_num = metadata.get("doc_number", "").strip()
        if doc_num:
            safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_num)
            pdf_name = f"{safe_name}.pdf"
        else:
            pdf_name = f"guidance_{row_id}.pdf"

        pdf_path = os.path.join(row_dir, pdf_name)

        # Convert
        if use_playwright:
            success, reason = convert_html_to_pdf_playwright(html_path, pdf_path, logger)
        else:
            success, reason = convert_html_to_pdf_simple(html_path, pdf_path, metadata, logger)

        if success:
            logger.info(f"  {row_id}: Converted → {pdf_name}")
            converted += 1

            # Update metadata
            metadata["files_downloaded"].append({
                "filename": pdf_name,
                "converted_from": html_file,
                "size_bytes": os.path.getsize(pdf_path),
                "sha256": sha256_file(pdf_path),
                "content_type": "application/pdf",
                "conversion_method": "playwright" if use_playwright else "reportlab",
            })

            # Remove needs_conversion flag from original
            for f in metadata["files_downloaded"]:
                if f.get("filename") == html_file:
                    f["needs_conversion"] = False
                    f["converted"] = True

            metadata["status"] = "success"
        else:
            logger.warning(f"  {row_id}: Conversion failed — {reason}")
            failed += 1

    # Save updated metadata
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

    return {"row": row_id, "converted": converted, "failed": failed}


def main():
    parser = argparse.ArgumentParser(description="Convert HTML pages to clean PDFs")
    parser.add_argument("--output", type=str, default=None, help="Override output directory")
    parser.add_argument("--row", type=str, default=None, help="Process specific row ID")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--no-playwright", action="store_true",
                        help="Use reportlab fallback instead of Playwright")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format=LOG_FORMAT)
    logger = logging.getLogger("acf_v2.convert")

    output_dir = args.output or OUTPUT_DIR
    use_playwright = not args.no_playwright

    if use_playwright:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.warning("Playwright not installed — falling back to reportlab")
            use_playwright = False

    stats = {"converted": 0, "failed": 0, "skipped": 0}
    results = []

    start_time = datetime.now()

    if args.row:
        # Single row
        row_dir = os.path.join(output_dir, args.row)
        if not os.path.isdir(row_dir):
            logger.error(f"Row directory not found: {row_dir}")
            sys.exit(1)
        result = process_row(row_dir, args.dry_run, use_playwright, logger)
        if result:
            results.append(result)
    else:
        # All rows
        folders = sorted(f for f in os.listdir(output_dir)
                         if os.path.isdir(os.path.join(output_dir, f)) and f.isdigit())
        total = len(folders)

        for i, folder in enumerate(folders):
            row_dir = os.path.join(output_dir, folder)
            result = process_row(row_dir, args.dry_run, use_playwright, logger)
            if result:
                results.append(result)
                if not args.dry_run:
                    c = result.get("converted", 0)
                    f = result.get("failed", 0)
                    stats["converted"] += c
                    stats["failed"] += f
            else:
                stats["skipped"] += 1

            if (i + 1) % 100 == 0:
                logger.info(f"  Progress: {i+1}/{total} rows processed")

    elapsed = datetime.now() - start_time

    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 60)
    if args.dry_run:
        print(f"  Would convert: {len(results)} rows")
    else:
        print(f"  Converted:     {stats['converted']} files")
        print(f"  Failed:        {stats['failed']} files")
        print(f"  Skipped:       {stats['skipped']} rows (no HTML to convert)")
    print(f"  Elapsed:       {elapsed}")

    # Save report
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR,
                               f"convert_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_path, "w") as f:
        json.dump({"stats": stats, "results": results,
                    "elapsed_seconds": elapsed.total_seconds()}, f, indent=2, default=str)
    print(f"  Report saved:  {report_path}")


if __name__ == "__main__":
    main()
