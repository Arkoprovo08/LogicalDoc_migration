import os
import oracledb
import requests
from datetime import datetime

DB_USERNAME = "framework01"
DB_PASSWORD = "pwc"
DB_DSN = "localhost:1521/orclpdb.00xqf4jcs1uenkdowvnhyf0uze.rx.internal.cloudapp.net"

API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
FILES_DIR = r"C:\Users\dghvmuser05\Desktop\DGH FILES\LogicalDoc\PDF"

def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

try:
    conn = oracledb.connect(user=DB_USERNAME, password=DB_PASSWORD, dsn=DB_DSN)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            faao.REFID,
            cf.FILE_NAME,
            faao.BLOCKCATEGORY,
            faao.BLOCKNAME,
            faao.CREATED_ON,
            cf.FILE_ID
        FROM FORM_APPOINTMENT_AUDITOR_OPR faao
        JOIN CMS_MASTER_FILEREF cmf ON faao.SCOPE_WORK = cmf.FILEREF
        JOIN CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
    """)

    rows = cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(regime,block,refid,sep='\n')

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        files = {'files': open(file_path, 'rb')}
        data = {
            'regime': regime,
            'block': block,
            'module': 'Operator Contracts and Agreements',
            'process': 'Appointment of Auditor',
            'financialYear': get_financial_year(created_on),
            'referenceNumber': refid,
            'label': 'Scope of Work' 
        }
        try:
            response = requests.post(API_URL, files=files, data=data)
            print(f"\n--- API response for {file_name} ---\n{response.text}\n")
            response.raise_for_status()

            response_json = response.json()
            response_objects = response_json.get("responseObject", [])

            logical_doc_id = None
            for item in response_objects:
                if item.get("fileName") == file_name:
                    logical_doc_id = item.get("docId")
                    break

            print(f"Uploaded: {file_name} ➜ docId: {logical_doc_id}")

            if logical_doc_id:
                update_sql = "UPDATE CMS_FILES SET LOGICAL_DOC_ID = :1 WHERE FILE_ID = :2"
                cursor.execute(update_sql, (logical_doc_id, file_id))
                conn.commit()
            else:
                print(f"No docId found for {file_name} in responseObject")

        # try:
        #     response = requests.post(API_URL, files=files, data=data)
        #     response.raise_for_status()

        #     # Assuming API returns JSON with docId
        #     logical_doc_id = response.json().get('docId')
        #     print(f"Uploaded: {file_name} ➜ docId: {logical_doc_id}")

        #     # Step 3: Update CMS_FILES
        #     if logical_doc_id:
        #         update_sql = "UPDATE CMS_FILES SET LOGICAL_DOC_ID = :1 WHERE FILE_ID = :2"
        #         cursor.execute(update_sql, (logical_doc_id, file_id))
        #         conn.commit()
        #     else:
        #         print(f"No docId returned for {file_name}")

        except Exception as upload_err:
            print(f"Upload failed for {file_name}: {upload_err}")

    cursor.close()
    conn.close()

except Exception as db_err:
    print(f"Database error: {db_err}")
