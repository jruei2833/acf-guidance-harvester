# ACF Sub-Regulatory Guidance Document Collection

Complete collection of sub-regulatory guidance documents from the Administration for Children and Families (ACF), U.S. Department of Health and Human Services.

**3,314 of 3,351 documents collected (98.9%)** across 13 ACF offices, totaling 8.9 GB.

---

## What's Here

| File | Description |
|------|-------------|
| **ACF_Inventory_Linked.xlsx** | Original ACF inventory with a "File Location" column linking to each document. Missing documents have a note explaining why. |
| **missing_documents_manifest.docx** | Detailed report on the 37 unavailable documents with failure reasons and recommended next steps. |
| **organized/** | All 3,314 guidance documents organized by Office → Doc Type → Row ID. *(Stored on SharePoint — too large for GitHub.)* |

## How to Use

1. Download `ACF_Inventory_Linked.xlsx` and the `organized/` folder from SharePoint
2. Place them in the same parent folder:
   ```
   ACF Guidance/
     ACF_Inventory_Linked.xlsx    ← open this
     organized/                   ← 8.9 GB, all documents
   ```
3. Open the Excel file and click **"Open Document"** on any row to open that guidance document

## Coverage

| Office | Documents | Coverage |
|--------|-----------|----------|
| ANA | 2 | 100% |
| CB | 762 | 100% |
| ECD | 45 | 100% |
| FYSB | 22 | 100% |
| OCC | 307 | 99.0% |
| OCS | 378 | 98.7% |
| OCSE | 767 | 100% |
| OFA | 348 | 100% |
| OFVPS | 16 | 100% |
| OHS | 140 | 98.6% |
| OHSEPR | 20 | 100% |
| ORR | 489 | 94.8% |
| OTIP | 18 | 100% |

## Development

All harvesting scripts, recovery tools, audit reports, and technical methodology are in the [`dev/`](dev/) folder. See [`dev/METHODOLOGY.md`](dev/METHODOLOGY.md) for the full replication guide.
