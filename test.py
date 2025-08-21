import pymssql

conn = pymssql.connect(
    server='hopnic.database.windows.net',
    user='hopnic2629',
    password='Hopnic_0778',
    database='hopnic'
)
cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())
