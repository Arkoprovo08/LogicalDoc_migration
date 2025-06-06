import oracledb
from datetime import datetime

def get_financial_year(created_on_str):
    created_on = datetime.strptime(created_on_str, "%Y-%m-%d %H:%M:%S.%f")
    
    year = created_on.year
    month = created_on.month

    if month > 3:
        fy_start = year
        fy_end = year + 1
    else:
        fy_start = year - 1
        fy_end = year

    return f"{fy_start}-{fy_end}"



# Database credentials and connection details
username = "framework01"
password = "pwc"
dsn = (
    "localhost:1521/orclpdb.00xqf4jcs1uenkdowvnhyf0uze.rx.internal.cloudapp.net"
)

try:
    # Connect to Oracle Database
    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )

    # Create a cursor and execute the query
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM FORM_APPOINTMENT_AUDITOR_OPR")

    # Fetch all rows
    rows = cursor.fetchall()

    # Print each row
    for row in rows:
        print(row[0])

    # Clean up
    cursor.close()
    connection.close()

except Exception as e:
    print("Error connecting to Oracle DB:", e)
