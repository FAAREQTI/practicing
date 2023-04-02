
import pandas as pd
import psycopg2


#function to connect to database
def connet_db():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="ARES"
    )
    cur = conn.cursor()
    return conn, cur


#function to create table
def create_table(col_type):
    # Connect to the database
    conn, cur = connet_db()
    # drop the table if exists
    cur.execute(f"DROP TABLE IF EXISTS {name_of_table}")
    # Create a new table
    cur.execute(f"CREATE TABLE {name_of_table} ({col_type})")   
    # Commit the changes
    conn.commit()  
    # Close the cursor and connection
    cur.close()
    conn.close()

#load csv
def load_csv(filename):
    df = pd.read_csv(filename)
    return df

#convert python to sql format
def convert_types_to_sql_format(df):
    types = []
    for i in df.dtypes:
        if i == 'int64':
            types.append('int')
        elif i == 'object':
            types.append('VARCHAR(255)')
        elif i == 'float':
            types.append("DECIMAL(6,2)")

    col_type = list(zip(df.columns.values, types))
    col_type = tuple([" ".join(i) for i in col_type])
    col_type = ', '.join(col_type)
    values = ', '.join(["%s" for i in range(len(df.columns))])
    return col_type, values