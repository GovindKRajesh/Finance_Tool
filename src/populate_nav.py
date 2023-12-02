import psycopg2
import requests
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

# Retrieve fund endpoints from the database
cur.execute("SELECT fund_name, fund_code FROM funds")
fund_endpoints = cur.fetchall()

# For each fund
for fund_name, fund_code in fund_endpoints:
    # Check the latest date in the database
    cur.execute("SELECT MAX(date) FROM nav_data WHERE fund_name = %s", (fund_name,))
    latest_date_result = cur.fetchone()
    latest_date = latest_date_result[0] if latest_date_result[0] else None

    # Call the API
    api_url = f"https://api.mfapi.in/mf/{fund_code}"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()['data']
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y').dt.date  # Convert to datetime.date
        df['nav'] = df['nav'].astype(float)

        updated = False  # Flag to check if any update was made

        # Insert new data into the database
        for _, row in df.iterrows():
            # Insert if there is no latest date or if the row date is greater than the latest date
            if latest_date is None or row['date'] > latest_date:
                cur.execute("INSERT INTO nav_data (fund_name, date, nav) VALUES (%s, %s, %s)", 
                            (fund_name, row['date'], row['nav']))
                updated = True
    
        if updated:
            print(f"Updated NAV data for {fund_name}.")
        else:
            print(f"No new NAV data to update for {fund_name}.")
    else:
        print(f"Failed to retrieve data for {fund_name}. Status code: {response.status_code}")

    # Commit changes and close the cursor
    conn.commit()

cur.close()
conn.close()