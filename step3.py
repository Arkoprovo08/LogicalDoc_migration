import oracledb
import psycopg2

ORACLE_CONFIG = {
    "user": "sys",
    "password": "Dgh12345",
    "dsn": oracledb.makedsn("192.168.0.133", 1521, sid="ORCL"),
    "mode": oracledb.SYSDBA
}

POSTGRES_CONFIG = {
    "host": "13.127.174.112",
    "port": 5432,
    "dbname": "ims",
    "user": "imsadmin",
    "password": "Dghims!2025"
}

try:
    oracle_conn = oracledb.connect(**ORACLE_CONFIG)
    oracle_cursor = oracle_conn.cursor()
    print("✅ Connected to Oracle")

    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cursor = pg_conn.cursor()
    print("✅ Connected to PostgreSQL")

    pg_cursor.execute("""
        SELECT appointment_auditor_id, appointment_auditor_application_number
        FROM operator_contracts_agreements.t_appointment_auditor_details
        WHERE is_migrated = 1
    """)
    applications = pg_cursor.fetchall()

    print(f"🔍 Found {len(applications)} migrated application numbers")

    for appointment_auditor_id, app_number in applications:
        oracle_cursor.execute("""
            SELECT LOGICAL_DOC_ID, LABEL_ID
            FROM FRAMEWORK01.CMS_FILES
            WHERE REFID = :1
        """, (app_number,))
        results = oracle_cursor.fetchall()

        for logical_doc_id, label_id in results:
            pg_cursor.execute("""
                INSERT INTO operator_contracts_agreements.t_appointment_auditor_document_details (
                    document_ref_number,
                    document_type_id,
                    document_name,
                    appointment_auditor_id
                )
                VALUES (%s, %s, %s, %s)
            """, (logical_doc_id, label_id, f"Doc_{logical_doc_id}", appointment_auditor_id))
            print(f"📥 Inserted document for App#: {app_number}, LogicalDocID: {logical_doc_id}")

    pg_conn.commit()
    print("✅ All data inserted and committed")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'oracle_cursor' in locals():
        oracle_cursor.close()
    if 'oracle_conn' in locals():
        oracle_conn.close()
    if 'pg_cursor' in locals():
        pg_cursor.close()
    if 'pg_conn' in locals():
        pg_conn.close()
    print("🔚 Connections closed")
