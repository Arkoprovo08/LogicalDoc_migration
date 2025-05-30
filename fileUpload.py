import os
import requests
import psycopg2
from datetime import datetime

DOCUMENT_FOLDER = r'C:\Users\Administrator.DGH\Desktop\dgh\Files'
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/documentManagement/uploadDocument"

conn = psycopg2.connect(
    dbname='orcl',
    user='sys',
    password='Dgh1234',
    host='192.168.0.133',
    port='1521'
)
cursor = conn.cursor()

process_name = "Appointment of Auditor"
regime = "RSC"
block = "CB-ONN-2005-10"
module = "Upstream Data Management"
financialYear = "2024-2025"
label = "Scope of Work"

created_by = 5

for file_name in os.listdir(DOCUMENT_FOLDER):
    if file_name.lower().endswith(".pdf"):
        file_path = os.path.join(DOCUMENT_FOLDER, file_name)
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
                        INSERT INTO global_master.t_document_migration_details 
                        (document_name, logical_doc_id, is_migrated, created_date, created_by)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        file_name, doc_id, 1, datetime.now(), created_by
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO global_master.t_doc_migration_error_logs 
                        (doc_name, error_message, timestamp)
                        VALUES (%s, %s, %s)
                    """, (
                        file_name, response.text, datetime.now()
                    ))

        except Exception as e:
            cursor.execute("""
                INSERT INTO global_master.t_doc_migration_error_logs 
                (doc_name, error_message, timestamp)
                VALUES (%s, %s, %s)
            """, (
                file_name, str(e), datetime.now()
            ))

conn.commit()
cursor.close()
conn.close()
