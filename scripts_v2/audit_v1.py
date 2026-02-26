#!/usr/bin/env python3
"""
ACF Guidance Harvester v2 — Audit Existing Collection
======================================================
Scans the original output/ directory and applies v2 validation
to quantify how many files have quality issues.

This answers the question: "How many of our 'successes' are actually
junk HTML pages?"

Usage:
  python audit_v1.py                      # Scan output/
  python audit_v1.py --dir output         # Specify directory
  python audit_v1.py --sample 100         # Quick sample scan
"""

import os
import sys
import json
import argparse
import logging
import random
from datetime import datetime
from collections import defaultdict

from validate import validate_content, detect_file_type

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not os.path.exists(os.path.join(BASE_DIR, "output")):
    BASE_DIR = os.path.dirname(BASE_DIR)

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


def audit_collection(scan_dir, sample_size=None):
    """
    Scan every row folder and validate all files.
    Returns detailed stats on quality issues.
    """
    logger = logging.getLogger("acf_v2.audit")

    folders = sorted(f for f in os.listdir(scan_dir)
                     if os.path.isdir(os.path.join(scan_dir, f)) and f.isdigit())

    if sample_size and sample_size < len(folders):
        logger.info(f"Sampling {sample_size} of {len(folders)} folders")
        folders = random.sample(folders, sample_size)
        folders.sort()

    total = len(folders)

    stats = {
        "total_rows": total,
        "rows_with_valid_content": 0,
        "rows_with_only_junk": 0,
        "rows_empty": 0,
        "total_files": 0,
        "valid_files": 0,
        "invalid_files": 0,
        "file_types": defaultdict(int),
        "failure_reasons": defaultdict(int),
        "failure_by_office": defaultdict(lambda: defaultdict(int)),
    }

    junk_rows = []   # rows with ONLY invalid files
    mixed_rows = []  # rows with both valid and invalid files

    for i, folder in enumerate(folders):
        row_dir = os.path.join(scan_dir, folder)
        meta_path = os.path.join(row_dir, "metadata.json")

        # Load metadata for office info
        office = "unknown"
        status = "unknown"
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                office = meta.get("office", "unknown")
                status = meta.get("status", "unknown")
            except Exception:
                pass

        # Scan all non-metadata files
        files = [f for f in os.listdir(row_dir)
                 if os.path.isfile(os.path.join(row_dir, f)) and f != "metadata.json"]

        if not files:
            stats["rows_empty"] += 1
            continue

        row_has_valid = False
        row_invalids = []

        for fname in files:
            fpath = os.path.join(row_dir, fname)
            stats["total_files"] += 1

            # Detect file type
            ftype = detect_file_type(fpath)
            stats["file_types"][ftype] += 1

            # Validate
            vr = validate_content(fpath)

            if vr.valid:
                stats["valid_files"] += 1
                row_has_valid = True
            else:
                stats["invalid_files"] += 1
                stats["failure_reasons"][vr.reason] += 1
                stats["failure_by_office"][office][vr.reason] += 1
                row_invalids.append({
                    "file": fname,
                    "reason": vr.reason,
                    "details": vr.details,
                })

        if row_has_valid:
            stats["rows_with_valid_content"] += 1
            if row_invalids:
                mixed_rows.append({
                    "row": folder, "office": office, "invalids": row_invalids
                })
        else:
            stats["rows_with_only_junk"] += 1
            junk_rows.append({
                "row": folder, "office": office, "status": status,
                "files": [{"file": f, **r} for f, r in zip(
                    [x["file"] for x in row_invalids],
                    row_invalids
                )],
            })

        # Progress
        if (i + 1) % 200 == 0:
            logger.info(f"  Scanned {i+1}/{total} rows...")

    return stats, junk_rows, mixed_rows


def main():
    parser = argparse.ArgumentParser(description="Audit existing v1 collection")
    parser.add_argument("--dir", type=str, default=None, help="Directory to scan")
    parser.add_argument("--sample", type=int, default=None, help="Sample N random folders")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    logger = logging.getLogger("acf_v2.audit")

    scan_dir = args.dir or OUTPUT_DIR
    if not os.path.isdir(scan_dir):
        logger.error(f"Directory not found: {scan_dir}")
        sys.exit(1)

    logger.info(f"Auditing collection in: {scan_dir}")
    start = datetime.now()

    stats, junk_rows, mixed_rows = audit_collection(scan_dir, args.sample)

    elapsed = datetime.now() - start

    # ── Report ──
    print("\n" + "=" * 70)
    print("v1 COLLECTION AUDIT REPORT")
    print("=" * 70)

    print(f"\n  Rows scanned:              {stats['total_rows']}")
    print(f"  Rows with valid content:   {stats['rows_with_valid_content']}"
          f"  ({stats['rows_with_valid_content']/max(stats['total_rows'],1)*100:.1f}%)")
    print(f"  Rows with ONLY junk:       {stats['rows_with_only_junk']}"
          f"  ({stats['rows_with_only_junk']/max(stats['total_rows'],1)*100:.1f}%)")
    print(f"  Rows empty:                {stats['rows_empty']}")

    print(f"\n  Total files scanned:       {stats['total_files']}")
    print(f"  Valid files:               {stats['valid_files']}")
    print(f"  Invalid files:             {stats['invalid_files']}")

    print(f"\n  File types found:")
    for ftype, count in sorted(stats["file_types"].items(), key=lambda x: -x[1]):
        print(f"    {ftype:20s}  {count}")

    print(f"\n  Failure reasons:")
    for reason, count in sorted(stats["failure_reasons"].items(), key=lambda x: -x[1]):
        print(f"    {reason:35s}  {count}")

    if stats["failure_by_office"]:
        print(f"\n  Failures by office:")
        for office in sorted(stats["failure_by_office"]):
            reasons = stats["failure_by_office"][office]
            total_office = sum(reasons.values())
            print(f"    {office} ({total_office} total):")
            for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
                print(f"      {reason:33s}  {count}")

    print(f"\n  Elapsed: {elapsed}")

    # ── Save detailed report ──
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # Convert defaultdicts to regular dicts for JSON
    stats_clean = dict(stats)
    stats_clean["file_types"] = dict(stats["file_types"])
    stats_clean["failure_reasons"] = dict(stats["failure_reasons"])
    stats_clean["failure_by_office"] = {
        k: dict(v) for k, v in stats["failure_by_office"].items()
    }

    report = {
        "stats": stats_clean,
        "junk_rows": junk_rows[:200],  # cap for file size
        "junk_row_count": len(junk_rows),
        "mixed_rows": mixed_rows[:100],
        "elapsed_seconds": elapsed.total_seconds(),
        "scan_dir": scan_dir,
        "sampled": args.sample is not None,
    }

    report_path = os.path.join(REPORTS_DIR,
                               f"audit_v1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n  Full report saved: {report_path}")

    # ── Quick summary of junk rows ──
    if junk_rows:
        print(f"\n  Sample junk rows (first 20):")
        for jr in junk_rows[:20]:
            files_str = ", ".join(f["file"] for f in jr["files"][:3])
            print(f"    {jr['row']} | {jr['office']:6s} | {files_str}")

    print("=" * 70)


if __name__ == "__main__":
    main()
