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

#Function to add a new investment to the database.
def add_investment(fund_name, principal, start_date, sell_date, units):
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO investments (fund_name, principal, start_date, sell_date, units) 
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fund_name, start_date) DO NOTHING;
                """, (fund_name, principal, start_date, sell_date, units))
            conn.commit()
            print("New investment data successfully inserted into the database.")


def populate_initial_investments(file_path):
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            df = pd.read_excel(file_path)
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
            conn.commit()
            print("Initial investment data successfully inserted into the database.")

# Call the function to populate initial investments
populate_initial_investments('..\\data\\Data.xlsx')
