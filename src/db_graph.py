import psycopg2
import pandas as pd
import plotly.express as px

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

# Query to retrieve all investment data
cur.execute("SELECT * FROM investments")
investments_df = pd.DataFrame(cur.fetchall(), columns=['Fund Name', 'Principal', 'Start Date', 'Sell Date', 'Units'])
investments_df['Units'] = investments_df['Units'].astype(float)

# Query to retrieve all NAV data
cur.execute("SELECT * FROM nav_data")
nav_data_df = pd.DataFrame(cur.fetchall(), columns=['Fund Name', 'Date', 'NAV'])
nav_data_df['NAV'] = nav_data_df['NAV'].astype(float)

# Closing the database connection
cur.close()
conn.close()

# Extract unique fund names from the investments table
unique_funds = investments_df['Fund Name'].unique()

# Function to format value in lakhs
def format_in_lakhs(value):
    return '{:.2f}L'.format(value / 1e5)

# Determine the start date for the graph
start_date = investments_df['Start Date'].min()

# Functions to update and adjust cumulative investments (same as before)
def update_cumulative_investments(cumulative_df, investment_data):
    for index, row in investment_data.iterrows():
        cumulative_df.loc[row['Start Date']:,'Units'] += row['Units']
    return cumulative_df

def adjust_for_sales(cumulative_df, investment_data):
    for index, row in investment_data.iterrows():
        if pd.notna(row['Sell Date']):
            cumulative_df.loc[row['Sell Date']:, 'Units'] -= row['Units']
    return cumulative_df

# Initialize a DataFrame to store total portfolio value
end_date = nav_data_df['Date'].max()
total_portfolio_value = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D'), columns=['Value']).fillna(0)

# Iterate over unique funds
for fund_name in unique_funds:
    # Filter investment data for the current fund
    fund_data = investments_df[investments_df['Fund Name'] == fund_name].copy()
    fund_data['Start Date'] = pd.to_datetime(fund_data['Start Date'])

    # Get NAV data for the current fund
    nav_df = nav_data_df[nav_data_df['Fund Name'] == fund_name].copy()
    nav_df.set_index('Date', inplace=True)

    # Initialize cumulative DataFrame with all dates from start_date
    fund_cumulative = pd.DataFrame(0, index=total_portfolio_value.index, columns=['Units'])
    fund_cumulative['Units'] = fund_cumulative['Units'].astype(float)

    # Update cumulative investments and adjust for sales
    fund_cumulative = update_cumulative_investments(fund_cumulative, fund_data)
    fund_cumulative = adjust_for_sales(fund_cumulative, fund_data)

    # Calculate the daily value of investments
    daily_value = fund_cumulative['Units'] * nav_df['NAV'].reindex(total_portfolio_value.index).ffill()

    # Add to the total portfolio value
    total_portfolio_value_aligned = total_portfolio_value.reindex(daily_value.index)
    total_portfolio_value['Value'] = total_portfolio_value_aligned['Value'].add(daily_value, fill_value=0)

# Fill gaps in the total portfolio value
total_portfolio_value_filled = total_portfolio_value.ffill()

# Plotting the total portfolio value
fig = px.line(total_portfolio_value_filled, y='Value', title='Portfolio Value Over Time (Dynamic Funds)')
fig.update_xaxes(title_text='Date')
fig.update_yaxes(title_text='Value', tickvals=[i * 1e5 for i in range(int(total_portfolio_value_filled['Value'].max() / 1e5) + 1)],
                 ticktext=[format_in_lakhs(i * 1e5) for i in range(int(total_portfolio_value_filled['Value'].max() / 1e5) + 1)])
fig.show()