import os
import psycopg2
from datetime import datetime

DOCUMENT_FOLDER = r'C:\Users\dghvmuser05\Desktop\DGH FILES\LogicalDoc\PDF'

# DB connection setup
conn = psycopg2.connect(
    dbname='ims',
    user='imsadmin',
    password='Dghims!2025',
    host='13.127.174.112',
    port='5432'
)
cursor = conn.cursor()

# Static values
folder_name = "BGDoc"
process_name = "Bank Gurantee"
created_by = 5

# Loop through files in the folder
for file_name in os.listdir(DOCUMENT_FOLDER):
    if file_name.lower().endswith(".pdf"):
        file_path = os.path.join(DOCUMENT_FOLDER, file_name)
        try:
            # Just check if file can be opened
            with open(file_path, "rb"):
                pass  # File exists and can be opened

            # Insert success entry in t_document_migration_details
            cursor.execute("""
                INSERT INTO global_master.t_document_migration_details 
                (document_name, logical_doc_id, is_migrated, created_date, created_by)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                file_name, None, 1, datetime.now(), created_by
            ))

        except Exception as e:
            # Log failure
            cursor.execute("""
                INSERT INTO global_master.t_doc_migration_error_logs 
                (doc_name, error_message, timestamp)
                VALUES (%s, %s, %s)
            """, (
                file_name, str(e), datetime.now()
            ))

# Commit and clean up
conn.commit()
cursor.close()
conn.close()
