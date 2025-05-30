import os
import requests
import oracledb
from datetime import datetime

DOCUMENT_FOLDER = r'C:\Users\Administrator.DGH\Desktop\dgh\Files'
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/documentManagement/uploadDocument"

# Oracle DB connection using oracledb in thin mode
conn = oracledb.connect(
    user='sys',
    password='Dgh1234',
    dsn='192.168.0.133:1521/orcl',
    mode=oracledb.SYSDBA
)

cursor = conn.cursor()

process_name = "Appointment of Auditor"
regime = "PSC"
module = "Upstream Data Management"
financialYear = "2024-2025"
label = "Scope of Work"
created_by = 5

cursor.execute("""
    SELECT a.refid, b.block_name
    FROM FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR a
    JOIN PSC.WS_BLOCK_MASTER b
    ON a.BLOCK_ID = b.BLOCK_ID
    WHERE b.is_psc = 1
""")
records = cursor.fetchall()  

for refid, block in records:
    file_name = f"{refid}.pdf"
    file_path = os.path.join(DOCUMENT_FOLDER, file_name)

    if os.path.isfile(file_path):
        try:
            with open(file_path, "rb") as f:
                files = {
                    'file': (file_name, f, 'application/pdf')
                }
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
                    print(f"Updated REFID {refid} with doc ID {doc_id}")
                else:
                    print(f"Error uploading {file_name}: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Exception while processing {file_name}: {str(e)}")
    else:
        print(f"File not found for REFID {refid}: Expected {file_name}")

conn.commit()
cursor.close()
conn.close()
