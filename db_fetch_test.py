#Quick DB Check - returns red cars
import sqlite3

conn = sqlite3.connect('mot3.db')

c = conn.cursor()

c.execute("SELECT * FROM vehicles WHERE colour LIKE 'RED'")


items = c.fetchall()
for item in items:
     print(item)

conn.commit()
conn.close()

