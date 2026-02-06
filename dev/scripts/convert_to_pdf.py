#!/usr/bin/env python3
"""
convert_to_pdf.py - Convert all non-PDF files in the harvest to PDF format.

Handles:
  - HTML/HTM files via Playwright (headless Chrome)
  - Office files (docx, doc, xlsx, xls, ppt, pptx) via LibreOffice
  - TXT files via LibreOffice
  - No-extension files via type detection
  - ZIP files are skipped (kept as-is)

Creates PDF copies alongside originals. Updates metadata.json with conversion info.
"""

import json
import os
import sys
import time
import subprocess
import shutil
import hashlib
from pathlib import Path
from datetime import datetime

# --- Configuration ---
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
SOFFICE = r"C:\Program Files\LibreOffice\program\soffice.exe"
REPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")

# File type groups
OFFICE_EXTS = {".docx", ".doc", ".xlsx", ".xls", ".ppt", ".pptx", ".txt"}
HTML_EXTS = {".html", ".htm"}
SKIP_EXTS = {".zip", ".pdf"}

# --- Helpers ---

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def detect_file_type(filepath):
    """Detect file type by reading magic bytes."""
    try:
        with open(filepath, "rb") as f:
            header = f.read(16)
    except:
        return "unknown"
    
    if header[:5] == b"%PDF-":
        return "pdf"
    elif header[:4] == b"PK\x03\x04":
        # Could be docx, xlsx, pptx, or zip
        return "zip_archive"  # will need further detection
    elif header[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return "ole"  # doc, xls, ppt
    elif b"<html" in header.lower() or b"<!doctype" in header.lower() or b"<!" in header[:5]:
        return "html"
    elif header[:4] == b"\x89PNG":
        return "png"
    elif header[:3] == b"\xff\xd8\xff":
        return "jpeg"
    else:
        # Try reading more for HTML detection
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read(500).lower()
            if "<html" in text or "<!doctype" in text or "<head" in text:
                return "html"
        except:
            pass
        return "unknown"


def convert_with_libreoffice(input_path, output_dir):
    """Convert a file to PDF using LibreOffice. Returns output PDF path or None."""
    try:
        result = subprocess.run(
            [SOFFICE, "--headless", "--convert-to", "pdf", "--outdir", output_dir, input_path],
            capture_output=True, text=True, timeout=120
        )
        # Find the output file
        stem = Path(input_path).stem
        pdf_path = os.path.join(output_dir, f"{stem}.pdf")
        if os.path.isfile(pdf_path):
            return pdf_path
        return None
    except subprocess.TimeoutExpired:
        return None
    except Exception as e:
        return None


def convert_html_batch_playwright(html_files, batch_size=50):
    """Convert HTML files to PDF using Playwright in batches.
    
    Args:
        html_files: list of (input_path, output_pdf_path) tuples
        batch_size: number of files per browser session
    
    Returns:
        dict mapping input_path -> output_pdf_path for successful conversions
    """
    from playwright.sync_api import sync_playwright
    
    results = {}
    total = len(html_files)
    
    for batch_start in range(0, total, batch_size):
        batch = html_files[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size
        print(f"  Playwright batch {batch_num}/{total_batches} ({len(batch)} files)...")
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                for input_path, output_pdf in batch:
                    try:
                        file_url = Path(input_path).as_uri()
                        page.goto(file_url, wait_until="load", timeout=15000)
                        page.pdf(path=output_pdf, format="Letter", print_background=True)
                        if os.path.isfile(output_pdf) and os.path.getsize(output_pdf) > 0:
                            results[input_path] = output_pdf
                    except Exception as e:
                        # Try with a fresh page
                        try:
                            page.close()
                            page = browser.new_page()
                        except:
                            pass
                
                browser.close()
        except Exception as e:
            print(f"    Playwright batch error: {e}")
    
    return results


# --- Main ---

def main():
    start_time = time.time()
    
    if not os.path.isdir(OUTPUT_DIR):
        print(f"ERROR: Output directory not found: {OUTPUT_DIR}")
        sys.exit(1)
    
    if not os.path.isfile(SOFFICE):
        print(f"ERROR: LibreOffice not found: {SOFFICE}")
        sys.exit(1)
    
    os.makedirs(REPORT_DIR, exist_ok=True)
    
    print("=" * 60)
    print("FILE CONVERSION TO PDF")
    print("=" * 60)
    
    # Phase 1: Scan all folders and categorize non-PDF files
    print("\nPhase 1: Scanning files...")
    
    html_files = []       # (input_path, output_pdf_path, folder, file_info)
    office_files = []     # (input_path, output_pdf_path, folder, file_info)
    noext_files = []      # (input_path, folder, file_info)
    skip_files = []
    already_pdf = []
    
    folders = sorted([d for d in os.listdir(OUTPUT_DIR) 
                      if os.path.isdir(os.path.join(OUTPUT_DIR, d)) and d.isdigit()])
    
    for folder in folders:
        meta_path = os.path.join(OUTPUT_DIR, folder, "metadata.json")
        if not os.path.isfile(meta_path):
            continue
        
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except:
            continue
        
        if meta.get("status") != "success":
            continue
        
        for file_info in meta.get("files_downloaded", []):
            filename = file_info.get("filename", "")
            filepath = os.path.join(OUTPUT_DIR, folder, filename)
            
            if not os.path.isfile(filepath):
                continue
            
            ext = os.path.splitext(filename)[1].lower()
            stem = os.path.splitext(filename)[0]
            pdf_name = f"{stem}.pdf"
            pdf_path = os.path.join(OUTPUT_DIR, folder, pdf_name)
            
            # Skip if already PDF
            if ext == ".pdf":
                already_pdf.append(filepath)
                continue
            
            # Skip if PDF version already exists
            if os.path.isfile(pdf_path) and os.path.getsize(pdf_path) > 0:
                already_pdf.append(filepath)
                continue
            
            if ext == ".zip":
                skip_files.append(filepath)
                continue
            
            if ext in HTML_EXTS:
                html_files.append((filepath, pdf_path, folder, file_info))
            elif ext in OFFICE_EXTS:
                office_files.append((filepath, pdf_path, folder, file_info))
            elif ext == "" or ext == ".p":
                noext_files.append((filepath, folder, file_info))
            else:
                skip_files.append(filepath)
    
    print(f"  Already PDF: {len(already_pdf)}")
    print(f"  HTML/HTM to convert: {len(html_files)}")
    print(f"  Office files to convert: {len(office_files)}")
    print(f"  No-extension to detect: {len(noext_files)}")
    print(f"  Skipped (zip/other): {len(skip_files)}")
    
    # Phase 1b: Detect and categorize no-extension files
    print(f"\nPhase 1b: Detecting {len(noext_files)} no-extension files...")
    for filepath, folder, file_info in noext_files:
        ftype = detect_file_type(filepath)
        stem = os.path.splitext(os.path.basename(filepath))[0]
        if not stem:
            stem = "document"
        pdf_path = os.path.join(OUTPUT_DIR, folder, f"{stem}.pdf")
        
        if ftype == "pdf":
            # It's already a PDF, just rename
            new_path = filepath + ".pdf" if not filepath.endswith(".pdf") else filepath
            if new_path != filepath:
                shutil.copy2(filepath, new_path)
            already_pdf.append(filepath)
            print(f"    {folder}: already PDF (no extension)")
        elif ftype == "html":
            html_files.append((filepath, pdf_path, folder, file_info))
        elif ftype in ("ole", "zip_archive"):
            office_files.append((filepath, pdf_path, folder, file_info))
        else:
            print(f"    {folder}: unknown type for {os.path.basename(filepath)}, skipping")
            skip_files.append(filepath)
    
    total_to_convert = len(html_files) + len(office_files)
    print(f"\n  Total files to convert: {total_to_convert}")
    
    # Tracking
    converted = []
    failed = []
    
    # Phase 2: Convert HTML files with Playwright
    if html_files:
        print(f"\nPhase 2: Converting {len(html_files)} HTML files with Playwright...")
        
        playwright_input = [(fp, pdf_p) for fp, pdf_p, _, _ in html_files]
        results = convert_html_batch_playwright(playwright_input)
        
        for filepath, pdf_path, folder, file_info in html_files:
            if filepath in results:
                converted.append({
                    "folder": folder,
                    "original": os.path.basename(filepath),
                    "pdf": os.path.basename(results[filepath]),
                    "method": "playwright",
                    "size": os.path.getsize(results[filepath])
                })
            else:
                failed.append({
                    "folder": folder,
                    "original": os.path.basename(filepath),
                    "method": "playwright",
                    "error": "conversion failed"
                })
        
        print(f"  HTML: {len([r for r in converted if r['method']=='playwright'])} converted, "
              f"{len([r for r in failed if r['method']=='playwright'])} failed")
    
    # Phase 3: Convert Office files with LibreOffice
    if office_files:
        print(f"\nPhase 3: Converting {len(office_files)} Office files with LibreOffice...")
        
        for i, (filepath, pdf_path, folder, file_info) in enumerate(office_files):
            if (i + 1) % 25 == 0 or i == 0:
                print(f"  [{i+1}/{len(office_files)}] Converting {os.path.basename(filepath)}...")
            
            folder_dir = os.path.join(OUTPUT_DIR, folder)
            result = convert_with_libreoffice(filepath, folder_dir)
            
            if result:
                converted.append({
                    "folder": folder,
                    "original": os.path.basename(filepath),
                    "pdf": os.path.basename(result),
                    "method": "libreoffice",
                    "size": os.path.getsize(result)
                })
            else:
                failed.append({
                    "folder": folder,
                    "original": os.path.basename(filepath),
                    "method": "libreoffice",
                    "error": "conversion failed"
                })
        
        print(f"  Office: {len([r for r in converted if r['method']=='libreoffice'])} converted, "
              f"{len([r for r in failed if r['method']=='libreoffice'])} failed")
    
    # Phase 4: Update metadata.json files
    print(f"\nPhase 4: Updating metadata files...")
    updated_count = 0
    
    for conv in converted:
        meta_path = os.path.join(OUTPUT_DIR, conv["folder"], "metadata.json")
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            
            # Add conversion info
            if "conversions" not in meta:
                meta["conversions"] = []
            
            pdf_full_path = os.path.join(OUTPUT_DIR, conv["folder"], conv["pdf"])
            meta["conversions"].append({
                "original_file": conv["original"],
                "pdf_file": conv["pdf"],
                "method": conv["method"],
                "size_bytes": conv["size"],
                "sha256": sha256_file(pdf_full_path),
                "converted_at": datetime.now().isoformat()
            })
            
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)
            
            updated_count += 1
        except Exception as e:
            print(f"  Warning: Could not update metadata for {conv['folder']}: {e}")
    
    print(f"  Updated {updated_count} metadata files")
    
    # Summary
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"CONVERSION COMPLETE ({elapsed:.1f} seconds)")
    print("=" * 60)
    print(f"  Converted: {len(converted)}")
    print(f"    - Playwright (HTML): {len([r for r in converted if r['method']=='playwright'])}")
    print(f"    - LibreOffice (Office): {len([r for r in converted if r['method']=='libreoffice'])}")
    print(f"  Failed: {len(failed)}")
    print(f"  Skipped: {len(skip_files)}")
    print(f"  Already PDF: {len(already_pdf)}")
    
    # Save report
    report = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": elapsed,
        "summary": {
            "converted": len(converted),
            "failed": len(failed),
            "skipped": len(skip_files),
            "already_pdf": len(already_pdf),
            "by_method": {
                "playwright": len([r for r in converted if r["method"] == "playwright"]),
                "libreoffice": len([r for r in converted if r["method"] == "libreoffice"]),
            }
        },
        "converted": converted,
        "failed": failed,
    }
    
    report_path = os.path.join(REPORT_DIR, f"conversion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\nReport saved: {report_path}")


if __name__ == "__main__":
    main()
