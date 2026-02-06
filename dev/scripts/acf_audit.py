#!/usr/bin/env python3
"""
ACF Guidance Harvest Auditor
==============================
Analyzes harvesting results to produce actionable reports:
  - Which rows succeeded, failed, or had no documents
  - Which failed rows might be recoverable with retries
  - File integrity verification via SHA256
  - Duplicate detection across downloaded files
  - Summary statistics by office, type, and status

Usage:
  python acf_audit.py                         # Full audit of output directory
  python acf_audit.py --verify-checksums      # Also verify file integrity
  python acf_audit.py --export-retry-list     # Generate list of rows to retry
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output")
DEFAULT_REPORTS = os.path.join(os.path.dirname(__file__), "..", "reports")


def sha256_file(filepath):
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_all_metadata(output_dir):
    """Load metadata.json from all row folders."""
    records = []
    output_path = Path(output_dir)

    for folder in sorted(output_path.iterdir()):
        if not folder.is_dir():
            continue
        meta_file = folder / "metadata.json"
        if meta_file.exists():
            with open(meta_file, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    records.append(data)
                except json.JSONDecodeError as e:
                    print(f"  Warning: Bad JSON in {meta_file}: {e}")
        else:
            # Folder exists but no metadata — might be partial
            records.append({
                "folder": folder.name,
                "status": "incomplete",
                "excel_row": int(folder.name) + 1 if folder.name.isdigit() else None,
                "files_downloaded": [],
                "errors": [{"error": "no_metadata_file"}]
            })

    return records


def audit_results(records, output_dir, verify_checksums=False):
    """Perform comprehensive audit of harvest results."""
    report = {
        "audit_timestamp": datetime.now().isoformat(),
        "total_rows_processed": len(records),
        "status_counts": Counter(),
        "by_office": defaultdict(lambda: {"total": 0, "success": 0, "failed": 0}),
        "total_files": 0,
        "total_bytes": 0,
        "file_types": Counter(),
        "failed_rows": [],
        "no_url_rows": [],
        "no_document_rows": [],
        "checksum_errors": [],
        "duplicate_files": [],
    }

    all_hashes = defaultdict(list)  # hash -> [(row, filename)]

    for rec in records:
        status = rec.get("status", "unknown")
        report["status_counts"][status] += 1

        office = rec.get("office", "unknown")
        report["by_office"][office]["total"] += 1
        if status == "success":
            report["by_office"][office]["success"] += 1
        elif status == "failed":
            report["by_office"][office]["failed"] += 1

        files = rec.get("files_downloaded", [])
        report["total_files"] += len(files)

        for f in files:
            size = f.get("size_bytes", 0)
            report["total_bytes"] += size

            ext = os.path.splitext(f.get("filename", ""))[1].lower()
            report["file_types"][ext] += 1

            # Track hashes for duplicate detection
            fhash = f.get("sha256", "")
            if fhash:
                all_hashes[fhash].append({
                    "row": rec.get("folder", "?"),
                    "filename": f.get("filename", "?")
                })

            # Verify checksums if requested
            if verify_checksums and fhash:
                folder = rec.get("folder", "")
                filepath = os.path.join(output_dir, folder, f.get("filename", ""))
                if os.path.exists(filepath):
                    actual = sha256_file(filepath)
                    if actual != fhash:
                        report["checksum_errors"].append({
                            "row": folder,
                            "file": f.get("filename"),
                            "expected": fhash,
                            "actual": actual
                        })

        # Collect problem rows
        if status == "failed":
            report["failed_rows"].append({
                "folder": rec.get("folder"),
                "excel_row": rec.get("excel_row"),
                "doc_number": rec.get("doc_number", ""),
                "title": rec.get("title", "")[:100],
                "errors": rec.get("errors", []),
                "urls": rec.get("urls", [])
            })
        elif status == "no_urls":
            report["no_url_rows"].append({
                "folder": rec.get("folder"),
                "excel_row": rec.get("excel_row"),
                "doc_number": rec.get("doc_number", ""),
                "title": rec.get("title", "")[:100]
            })
        elif status == "no_documents":
            report["no_document_rows"].append({
                "folder": rec.get("folder"),
                "excel_row": rec.get("excel_row"),
                "doc_number": rec.get("doc_number", ""),
                "title": rec.get("title", "")[:100],
                "urls": rec.get("urls", [])
            })

    # Find duplicates (same hash in multiple rows)
    for fhash, locations in all_hashes.items():
        if len(locations) > 1:
            report["duplicate_files"].append({
                "sha256": fhash,
                "count": len(locations),
                "locations": locations
            })

    return report


def print_report(report):
    """Print a formatted audit report to console."""
    print("=" * 70)
    print("ACF GUIDANCE HARVEST — AUDIT REPORT")
    print(f"Generated: {report['audit_timestamp']}")
    print("=" * 70)

    print(f"\nTotal rows processed: {report['total_rows_processed']}")
    print(f"Total files downloaded: {report['total_files']}")
    print(f"Total data: {report['total_bytes'] / (1024*1024):.1f} MB")

    print("\n── Status Breakdown ──")
    for status, count in sorted(report["status_counts"].items(),
                                 key=lambda x: -x[1]):
        pct = count / report["total_rows_processed"] * 100
        print(f"  {status:20s}: {count:5d} ({pct:.1f}%)")

    print("\n── File Types ──")
    for ext, count in sorted(report["file_types"].items(),
                              key=lambda x: -x[1]):
        print(f"  {ext or '(none)':10s}: {count}")

    print("\n── By Office ──")
    for office, data in sorted(report["by_office"].items()):
        rate = data["success"] / data["total"] * 100 if data["total"] else 0
        print(f"  {office:10s}: {data['total']:4d} total, "
              f"{data['success']:4d} success ({rate:.0f}%)")

    if report["failed_rows"]:
        print(f"\n── Failed Rows ({len(report['failed_rows'])}) ──")
        for row in report["failed_rows"][:20]:
            print(f"  Row {row['folder']}: {row['doc_number']} — "
                  f"{row['errors'][0].get('error', '?') if row['errors'] else '?'}")
        if len(report["failed_rows"]) > 20:
            print(f"  ... and {len(report['failed_rows']) - 20} more")

    if report["no_url_rows"]:
        print(f"\n── Rows With No URLs ({len(report['no_url_rows'])}) ──")
        for row in report["no_url_rows"]:
            print(f"  Row {row['folder']}: {row['doc_number']} — {row['title'][:60]}")

    if report["checksum_errors"]:
        print(f"\n── Checksum Errors ({len(report['checksum_errors'])}) ──")
        for err in report["checksum_errors"]:
            print(f"  Row {err['row']}: {err['file']}")

    if report["duplicate_files"]:
        print(f"\n── Duplicate Files ({len(report['duplicate_files'])} groups) ──")
        for dup in report["duplicate_files"][:10]:
            print(f"  SHA256 {dup['sha256'][:16]}... "
                  f"({dup['count']} copies)")

    print("\n" + "=" * 70)


def export_retry_list(report, reports_dir):
    """Export a CSV of rows that should be retried."""
    retry_rows = report["failed_rows"] + report["no_document_rows"]

    if not retry_rows:
        print("No rows to retry!")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(reports_dir, f"retry_list_{timestamp}.csv")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["folder", "excel_row", "doc_number", "title",
                         "original_status", "urls"])
        for row in retry_rows:
            urls = "; ".join(u.get("url", "") for u in row.get("urls", []))
            writer.writerow([
                row.get("folder", ""),
                row.get("excel_row", ""),
                row.get("doc_number", ""),
                row.get("title", ""),
                "failed" if row in report["failed_rows"] else "no_documents",
                urls
            ])

    print(f"\nRetry list saved: {csv_path}")
    print(f"  {len(retry_rows)} rows to retry")


def main():
    parser = argparse.ArgumentParser(
        description="ACF Guidance Harvest Auditor")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Output directory to audit")
    parser.add_argument("--reports", default=DEFAULT_REPORTS,
                        help="Directory for audit reports")
    parser.add_argument("--verify-checksums", action="store_true",
                        help="Verify SHA256 checksums of all files")
    parser.add_argument("--export-retry-list", action="store_true",
                        help="Export CSV list of rows to retry")
    args = parser.parse_args()

    output_dir = os.path.abspath(args.output)
    reports_dir = os.path.abspath(args.reports)
    os.makedirs(reports_dir, exist_ok=True)

    print(f"Loading metadata from: {output_dir}")
    records = load_all_metadata(output_dir)

    if not records:
        print("No processed rows found. Run the harvester first.")
        return 1

    print(f"Found {len(records)} processed rows\n")

    report = audit_results(records, output_dir,
                           verify_checksums=args.verify_checksums)
    print_report(report)

    # Save JSON report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"audit_report_{timestamp}.json")

    # Convert defaultdicts and Counters for JSON serialization
    report["status_counts"] = dict(report["status_counts"])
    report["by_office"] = {k: dict(v) for k, v in report["by_office"].items()}
    report["file_types"] = dict(report["file_types"])

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull audit report saved: {report_path}")

    if args.export_retry_list:
        export_retry_list(report, reports_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
