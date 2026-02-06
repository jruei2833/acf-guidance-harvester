"""Reorganize the repo: deliverables at root, everything else in dev/."""
import os
import subprocess

def run(cmd):
    print(f"  > {cmd}")
    subprocess.run(cmd, shell=True, check=True)

print("Reorganizing repository...\n")

# Create dev folder
os.makedirs("dev", exist_ok=True)

# Move development artifacts into dev/
moves = [
    ("scripts", "dev/scripts"),
    ("reports", "dev/reports"),
    ("catalog", "dev/catalog"),
    ("input", "dev/input"),
]

for src, dst in moves:
    if os.path.exists(src):
        run(f'git mv "{src}" "{dst}"')

# Move deliverables to root
if os.path.exists("dev/catalog/ACF_Inventory_Linked.xlsx"):
    run('git mv "dev/catalog/ACF_Inventory_Linked.xlsx" "ACF_Inventory_Linked.xlsx"')

if os.path.exists("dev/catalog/ACF_Guidance_Master_Catalog.xlsx"):
    run('git mv "dev/catalog/ACF_Guidance_Master_Catalog.xlsx" "dev/ACF_Guidance_Master_Catalog.xlsx"')

# Move manifest to root if it exists in reports
for f in os.listdir("dev/reports") if os.path.exists("dev/reports") else []:
    if "missing" in f.lower() and f.endswith(".docx"):
        run(f'git mv "dev/reports/{f}" "missing_documents_manifest.docx"')
        break

print("\nDone. Now update README and commit.")
