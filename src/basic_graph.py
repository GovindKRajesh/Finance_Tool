import pandas as pd
import plotly.express as px
import os

# Load the main Data.xlsx file
data_df = pd.read_excel('.\\Data.xlsx')

# Extract unique fund names from the Data.xlsx file
unique_funds = data_df['Name'].unique()

# Initialize a dictionary to hold the NAV data for each fund
nav_data = {}

def format_in_lakhs(value):
    return '{:.2f}L'.format(value / 1e5)

# Attempt to load the NAV history files corresponding to each fund name
for fund_name in unique_funds:
    # Adjusting file path to match the current environment
    file_path = f'.\\Funds\{fund_name}.csv'
    if os.path.exists(file_path):
        # Read the CSV file if it exists
        nav_data[fund_name] = pd.read_csv(file_path, parse_dates=['Date'], index_col='Date')
        # print(f"Loaded NAV data for {fund_name}")

# Determine the start date for the graph as the earliest investment date from Data.xlsx
start_date = data_df['Start Date'].min()

# Function to update cumulative investments over time
def update_cumulative_investments(cumulative_df, investment_data):
    for index, row in investment_data.iterrows():
        cumulative_df.loc[row['Start Date']:,'Units'] += row['Units']
    return cumulative_df

# Function to adjust for sales in cumulative investment data
def adjust_for_sales(cumulative_df, investment_data):
    for index, row in investment_data.iterrows():
        if pd.notna(row['Sell Date']):
            cumulative_df.loc[row['Sell Date']:, 'Units'] -= row['Units']
    return cumulative_df

# Initialize a DataFrame to store total portfolio value
total_portfolio_value = pd.DataFrame(index=pd.date_range(start=start_date, 
                                                         end=max(nav_df.index.max() for nav_df in nav_data.values()), 
                                                         freq='D'), columns=['Value']).fillna(0)

for fund_name, nav_df in nav_data.items():
    # Filter investment data for the current fund
    fund_data = data_df[data_df['Name'] == fund_name].copy()
    fund_data['Start Date'] = pd.to_datetime(fund_data['Start Date'])

    # Initialize cumulative DataFrame with all dates from start_date
    fund_cumulative = pd.DataFrame(0, index=total_portfolio_value.index, columns=['Units'])
    fund_cumulative['Units'] = fund_cumulative['Units'].astype(float)

    # Update cumulative investments and adjust for sales
    fund_cumulative = update_cumulative_investments(fund_cumulative, fund_data)
    fund_cumulative = adjust_for_sales(fund_cumulative, fund_data)

    # Calculate the daily value of investments
    daily_value = fund_cumulative['Units'] * nav_df['Close'].reindex(total_portfolio_value.index).ffill()

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