import os
import sys
import oracledb
import requests
from datetime import datetime

log_file = "migration_output.txt"
sys.stdout = open(log_file, "w", encoding="utf-8")
sys.stderr = sys.stdout

DB_USERNAME = "sys"
DB_PASSWORD = "Dgh12345"
DB_HOST = "192.168.0.133"
DB_PORT = 1521
DB_SID = "ORCL"
DB_DSN = oracledb.makedsn(DB_HOST, DB_PORT, sid=DB_SID)

API_URL = "http://k8s-ingressn-ingressn-1628ed6eec-bd2bc8d22bd4aed8.elb.ap-south-1.amazonaws.com/docs/documentManagement/uploadMultipleDocument"
FILES_DIR = r"C:\Users\Administrator.DGH\Desktop\dgh\Files\CMS\Uploads"

def get_financial_year(created_on):
    if isinstance(created_on, str):
        created_on = datetime.strptime(created_on, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    return f"{year}-{year + 1}" if created_on.month > 3 else f"{year - 1}-{year}"

def process_documents(cursor, query, label):
    cursor.execute(query)
    rows = cursor.fetchall()

    for refid, file_name, regime, block, created_on, file_id in rows:
        file_path = os.path.join(FILES_DIR, file_name)

        print(f"\nProcessing {label}:")
        print(regime, block, refid, sep='\n')

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
            'label': label
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

            if logical_doc_id:
                print(f"✅ Uploaded: {file_name} ➜ docId: {logical_doc_id}")
                update_sql = "UPDATE FRAMEWORK01.CMS_FILES SET LOGICAL_DOC_ID = :1 WHERE FILE_ID = :2"
                cursor.execute(update_sql, (logical_doc_id, file_id))
            else:
                print(f"⚠️ No docId found for {file_name} in responseObject")
        except Exception as upload_err:
            print(f"❌ Upload failed for {file_name}: {upload_err}")

try:
    conn = oracledb.connect(
        user=DB_USERNAME,
        password=DB_PASSWORD,
        dsn=DB_DSN,
        mode=oracledb.SYSDBA
    )
    cursor = conn.cursor()
    print("✅ Connected to Oracle database.")

    # Scope of Work
    query_scope = """
        SELECT 
            faao.REFID,
            cf.FILE_NAME,
            faao.BLOCKCATEGORY,
            faao.BLOCKNAME,
            faao.CREATED_ON,
            cf.FILE_ID
        FROM FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.SCOPE_WORK = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(cursor, query_scope, "Scope of Work")

    # Upload OCR
    query_ocr = """
        SELECT 
            faao.REFID,
            cf.FILE_NAME,
            faao.BLOCKCATEGORY,
            faao.BLOCKNAME,
            faao.CREATED_ON,
            cf.FILE_ID
        FROM FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.UPLOAD_OCR = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(cursor, query_ocr, "Upload OCR (if Yes is selected in S No. 26)")

    # MC Approved Auditors
    query_mc = """
        SELECT 
            faao.REFID,
            cf.FILE_NAME,
            faao.BLOCKCATEGORY,
            faao.BLOCKNAME,
            faao.CREATED_ON,
            cf.FILE_ID
        FROM FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.UPLOAD_MC = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(cursor, query_mc, "MC Approved Auditors")

    #Upload OCR Not Available
    
    query_ocr_no = """
        SELECT 
            faao.REFID,
            cf.FILE_NAME,
            cmf.fileref,
            faao.BLOCKCATEGORY,
            faao.BLOCKNAME,
            faao.CREATED_ON,
            cf.FILE_ID
        FROM FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR faao
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf ON faao.OCR_UNAVAIABLE_FILE  = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILE_REF cfr ON cfr.REF_ID = cmf.FILEREF
        JOIN FRAMEWORK01.CMS_FILES cf ON cf.FILE_ID = cfr.FILE_ID
        WHERE cmf.ACTIVE = 1
    """
    process_documents(cursor, query_ocr_no, "OCR Unaivalable")
    
    #Upload Additional Documents
    
    query_additional = """
        SELECT FAAO.REFID ,CF.FILE_NAME ,faao.BLOCKCATEGORY,faao.BLOCKNAME,faao.CREATED_ON,cfr.FILE_ID
        FROM FRAMEWORK01.FORM_APPOINTMENT_AUDITOR_OPR faao 
        JOIN FRAMEWORK01.CMS_MASTER_FILEREF cmf 
        ON FAAO.REFID  = CMF.REFID 
        JOIN FRAMEWORK01.CMS_FILE_REF cfr 
        ON CFR.REF_ID  = CMF.FILEREF 
        JOIN FRAMEWORK01.CMS_FILES cf 
        ON CF.FILE_ID = CFR.FILE_ID 
        WHERE cmf.ACTIVE = 1
    """
    process_documents(cursor, query_additional, "Upload Additional Documents")    
    
    conn.commit()
    print("✅ All files processed and committed to database.")

    cursor.close()
    conn.close()
    print("✅ Database connection closed.")

except Exception as db_err:
    print(f"❌ Database error: {db_err}")

# Restore terminal output (optional cleanup)
sys.stdout.close()
sys.stdout = sys.__stdout__
