import pyodbc

server = '192.168.1.204'
database = 'Olist'
driver = 'SQL Server Native Client 11.0'
username = 'sa'
password = 'password'

connection_string = (
    f'DRIVER={{{driver}}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    f'Encrypt=no;'
)

try:
    conn = pyodbc.connect(connection_string)
    print("Connection successful!")
    conn.close()
except Exception as e:
    print("Connection failed:", e)