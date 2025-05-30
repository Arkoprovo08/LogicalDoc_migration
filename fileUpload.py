import os
import requests
import oracledb
from datetime import datetime

DOCUMENT_FOLDER = r'C:\Users\Administrator.DGH\Desktop\dgh\Files'
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/documentManagement/uploadDocument"

# Oracle DB connection
conn = oracledb.connect(
    user='sys',
    password='Dgh1234',
    dsn='192.168.0.133:1521/orcl',
    mode=oracledb.SYSDBA
)
cursor = conn.cursor()

# Static values
process_name = "Appointment of Auditor"
regime = "PSC"
block = "CB-ONN-2005-10"  # hardcoded
module = "Upstream Data Management"
financialYear = "2024-2025"
label = "Scope of Work"
created_by = 5

# Step 1: Get all REFIDs from DB
cursor.execute("""
    SELECT REFID 
    FROM FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR 
    WHERE logical_doc_id IS NULL
""")
refid_rows = cursor.fetchall()
refids = [row[0] for row in refid_rows]

# Step 2: Get all PDF files in folder
pdf_files = [f for f in os.listdir(DOCUMENT_FOLDER) if f.lower().endswith(".pdf")]

# Step 3: Assign document to REFID sequentially
assignments = list(zip(refids, pdf_files))

if len(pdf_files) < len(refids):
    print(f"[WARNING] Only {len(pdf_files)} files for {len(refids)} REFIDs. Some REFIDs will remain unassigned.")
elif len(pdf_files) > len(refids):
    print(f"[WARNING] {len(pdf_files)} files for only {len(refids)} REFIDs. Some files will not be assigned.")

for refid, file_name in assignments:
    file_path = os.path.join(DOCUMENT_FOLDER, file_name)

    try:
        with open(file_path, "rb") as f:
            files = {'file': (file_name, f, 'application/pdf')}
            data = {
                "businessId": "",
                "processName": process_name,
                "regime": regime,
                "block": block,
                "module": module,
                "financialYear": financialYear,
                "label": label
            }

            response = requests.post(API_URL, files=files, data=data)

            if response.status_code == 200:
                doc_id = response.json().get("documentId", None)
                cursor.execute("""
                    UPDATE FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR 
                    SET logical_doc_id = :1
                    WHERE REFID = :2
                """, (doc_id, refid))
                print(f"[SUCCESS] REFID {refid} -> doc ID {doc_id}")
            else:
                print(f"[ERROR] API upload failed for {file_name}: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"[EXCEPTION] Failed processing {file_name} for REFID {refid}: {str(e)}")

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()
