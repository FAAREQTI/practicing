import pandas as pd
import psycopg2
from src.tools.utils import load_csv


conn,cur = connet_db()
df = load_csv("data/supermarket_sales.csv")
col_types, values = convert_types_to_sql_format(df=df)
create_table(col_types)

for i,row in df.iterrows(): 
    sql = f'INSERT INTO supermarket VALUES ({values})'
    cur.execute(sql, tuple(row))                
    conn.commit()

cur.execute("SELECT * FROM supermarket;")
print(cur.fetchall())