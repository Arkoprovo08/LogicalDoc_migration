import oracledb
from datetime import datetime

# Function to calculate financial year
def get_financial_year(created_on_str):
    created_on = datetime.strptime(created_on_str, "%Y-%m-%d %H:%M:%S.%f")
    year = created_on.year
    month = created_on.month
    if month > 3:
        return f"{year}-{year + 1}"
    else:
        return f"{year - 1}-{year}"

# Database credentials and connection details
username = "framework01"
password = "pwc"
dsn = "localhost:1521/orclpdb.00xqf4jcs1uenkdowvnhyf0uze.rx.internal.cloudapp.net"

try:
    # Connect to Oracle Database
    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )

    cursor = connection.cursor()
    cursor.execute("SELECT created_on FROM FORM_APPOINTMENT_AUDITOR_OPR")
    rows = cursor.fetchall()

    for row in rows:
        created_on_str = row[0]  # This is a string
        fy = get_financial_year(created_on_str)
        print(f"Created On: {created_on_str} ➜ Financial Year: {fy}")

    cursor.close()
    connection.close()

except Exception as e:
    print("Error connecting to Oracle DB:", e)
