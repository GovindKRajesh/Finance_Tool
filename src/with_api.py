import requests
import pandas as pd

# Storing all relevant endpoint codes
endoints = {
    "Mirae Tax":"135781",
    "Canara Tax":"118285",
    "Quant Tax":"120847",
    "Quant Multi":"120821",
    "Quant Absolute":"120819",
    "360 Focused":"131580",
    "Mirae FAANG":"148928",
    "SBI Global":"119711",
    "ICICI MNC":"147346",
    "Quant Small":"120828",
    "Quant Flexi":"120843",
    "Parag Flexi":"122639",
    "Kotak Emerging":"119775",
    "SBI Conservative":"119839",
    "Kotak Debt":"120154"
}

# URL for the API endpoint
base_url = "https://api.mfapi.in/mf/"
api_url = base_url + endoints["Parag Flexi"]

# Send a GET request to the API
response = requests.get(api_url)

# Check if the request was successful
if response.status_code == 200:
    # Convert the JSON response to a Python dictionary
    data = response.json()
    
    # Extract the 'data' part which contains the NAV values
    nav_data = data['data']
    
    # Convert the 'nav_data' list to a pandas DataFrame
    df = pd.DataFrame(nav_data)
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
    df = pd.DataFrame()  # Empty DataFrame in case of failure

print(df.head())  # Display the first few rows of the DataFrame
