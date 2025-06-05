import oracledb
import gzip
import base64
import struct
from io import BytesIO

# Function to decompress a base64-encoded, GZIP-compressed string
def decompress_string(compressed_text: str) -> str:
    try:
        gZipBuffer = base64.b64decode(compressed_text)
        data_length = struct.unpack('I', gZipBuffer[:4])[0]
        compressed_data = gZipBuffer[4:]

        in_stream = BytesIO(compressed_data)
        with gzip.GzipFile(fileobj=in_stream, mode='rb') as gzip_file:
            buffer = gzip_file.read(data_length)

        return buffer.decode('utf-8')
    except Exception as e:
        return f"Error during decompression: {e}"

# Database connection details
DB_USERNAME = "sys"
DB_PASSWORD = "pwc"
DB_HOST = "localhost"
DB_PORT = 1521
DB_SID = "ORCL"
DB_DSN = oracledb.makedsn(DB_HOST, DB_PORT, sid=DB_SID)

try:
    # Connect as SYSDBA to access ANONYMOUS schema
    connection = oracledb.connect(user=DB_USERNAME, password=DB_PASSWORD, dsn=DB_DSN, mode=oracledb.SYSDBA)
    cursor = connection.cursor()

    # Access table from ANONYMOUS schema
    cursor.execute("""
    SELECT "ID", "FRM_QUERYs"
    FROM ANONYMOUS."FRM_WORKITEM_MASTER_NEW"
    WHERE "FRM_QUERYs" IS NOT NULL
""")


    rows = cursor.fetchall()

    for row in rows:
        record_id = row[0]
        compressed_value = row[1]

        decompressed_json = decompress_string(compressed_value)

        cursor.execute("""
    UPDATE ANONYMOUS."FRM_WORKITEM_MASTER_NEW"
    SET "FRM_QUERYS_JSON" = :json_val
    WHERE "ID" = :id
""", {"json_val": decompressed_json, "id": record_id})


    connection.commit()
    print(f"Updated {len(rows)} rows successfully.")

    cursor.close()
    connection.close()

except Exception as e:
    print("Error connecting to Oracle DB or processing data:", e)
