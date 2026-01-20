import pyodbc

DB_CONFIG = {
    'server': '192.168.1.41',
    'database': 'DBInforma',
    'username': 'sa',
    'password': 'Informa2025$$',
    'port': 1433,
    'encrypt': 'yes',
    'trust_cert': 'yes'
}

conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={DB_CONFIG['server']},{DB_CONFIG['port']};"
    f"DATABASE={DB_CONFIG['database']};"
    f"UID={DB_CONFIG['username']};"
    f"PWD={DB_CONFIG['password']};"
    f"Encrypt={DB_CONFIG['encrypt']};"
    f"TrustServerCertificate={DB_CONFIG['trust_cert']};"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 0 * FROM Huancayo.GesstionDiaria")
    columns = [column[0] for column in cursor.description]
    print("Columns in Huancayo.GesstionDiaria:")
    for col in columns:
        print(col)
    conn.close()
except Exception as e:
    print(f"Error: {e}")
