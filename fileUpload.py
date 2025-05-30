import os
import requests
import psycopg2
from datetime import datetime

DOCUMENT_FOLDER = r'C:\Users\dghvmuser05\Desktop\DGH FILES\LogicalDoc\PDF'
API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/documentManagement/uploadDocument"

conn = psycopg2.connect(
    dbname='ims',
    user='imsadmin',
    password='Dghims!2025',
    host='13.127.174.112',
    port='5432'
)
cursor = conn.cursor()

folder_name = "BGDoc"
process_name = "Bank Gurantee"
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
                    "folderName": folder_name,
                    "businessId": "",  
                    "processName": process_name
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
