import psycopg2
import pandas as pd

from conf import DB_USERNAME, DB_PASSWORD

# Database connection parameters
conn_params = {
    'dbname': 'Finance_Tool',
    'user': DB_USERNAME,
    'password': DB_PASSWORD,
    'host': 'localhost'
}

# Connect to the database
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# Load the Excel file
excel_file = '..\data\Data.xlsx'
df = pd.read_excel(excel_file)

# Iterate through the DataFrame and insert data into the database
for index, row in df.iterrows():
    fund_name = row['Name']
    principal = row['Principal']
    start_date = row['Start Date']
    sell_date = row['Sell Date'] if not pd.isna(row['Sell Date']) else None
    units = row['Units']

    cur.execute("""
        INSERT INTO investments (fund_name, principal, start_date, sell_date, units) 
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (fund_name, start_date) DO NOTHING;
        """, (fund_name, principal, start_date, sell_date, units))

# Commit changes and close the cursor
conn.commit()
cur.close()
conn.close()

print("Investment data successfully inserted into the database.")
