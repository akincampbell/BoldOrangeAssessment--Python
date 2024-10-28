import pandas as pd
import requests
import json
import os

### Constants
API_URL = "https://datalake.boldorange.com/api/v1"  # Base URL for the API
USERNAME = "acampbell"  # Replace with your username
PASSWORD = "rVZ3FkzqcGawjEM952PWL6"  # Replace with your password
COLLECTION_NAME = "AkinExample"  # Change this to your collection name

### Identify the CSV
# Define the path to the CSV file
file_path = os.path.join(os.path.dirname(__file__), "DeveloperAssessmentData.csv")  # Corrected filename

# Check the current directory for files
print("Files in the current directory:")
print(os.listdir(os.path.dirname(__file__)))  # Lists all files in the directory

# Check if the file exists before attempting to read it
if not os.path.isfile(file_path):
    raise FileNotFoundError(f"The file {file_path} does not exist. Please check the file name and path.")

### Step 1: Authenticate and get token
def authenticate():
    auth_url = f"{API_URL}/authenticate"  # Authentication URL
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
        "grant": "password"  # Set the grant type to "password"
    }
    response = requests.post(auth_url, json=payload)

    # Debugging output
    print(f"Response Status Code: {response.status_code}")  # Print the status code
    print(f"Response Content: {response.text}")  # Print the content of the response

    if response.status_code == 200:
        return response.json()["accesstoken"]  # Access the correct key for the token
    else:
        raise Exception("Authentication failed.")

### Step 2: Read the CSV file
def read_csv(file_path):
    data = pd.read_csv(file_path, delimiter='|')  # Specify '|' as the delimiter
    data.columns = data.columns.str.strip().str.replace(',', '')  # Clean column names and remove commas
    print("Columns in the DataFrame:", data.columns.tolist())  # Print column names
    print("Sample Data:\n", data.head())  # Print first few rows of data
    return data

# Helper function to convert to float and handle special cases
def to_float(value):
    if pd.isna(value) or value in ['-', '']:  # Handle NaN, '-' or empty string
        return 0.0  # Default value if invalid
    
    value_str = str(value).strip()
    
    # Check for special cases like ' $-   ' or any negative value represented as a string
    if value_str.startswith('(') and value_str.endswith(')'):
        value_str = '-' + value_str[1:-1].replace('$', '').replace(',', '').strip()
    elif ' $-' in value_str or value_str == '$-':
        return 0.0  # Handle cases where the value is a string indicating no value (e.g., ' $-   ')
    else:
        # Clean other dollar and comma formats
        value_str = value_str.replace('$', '').replace(',', '').strip()
        
    try:
        return float(value_str)
    except ValueError:
        print(f"Warning: Could not convert '{value}' to float. Setting to 0.0.")
        return 0.0  # or handle it as needed

# Helper function to clean year values
def clean_year(value):
    if pd.isna(value):
        return 0  # or use np.nan based on your needs
    try:
        return int(str(value).strip().replace(',', '').replace(' ', ''))  # Clean and convert year
    except ValueError:
        print(f"Warning: Could not convert '{value}' to int. Setting to 0.")
        return 0  # or handle it as needed

### Step 3: Prepare the data for API
def prepare_data(data):
    items = []
    for index, row in data.iterrows():
        item = {
            "Key": f"item{index + 1}",  # Unique key for each item
            "Attributes": [
                {"Name": "Segment", "Type": "string", "Value": row['Segment']},
                {"Name": "Country", "Type": "string", "Value": row['Country']},
                {"Name": "Product", "Type": "string", "Value": row['Product']},
                {"Name": "DiscountBand", "Type": "string", "Value": row['DiscountBand']},
                {"Name": "UnitsSold", "Type": "number", "Value": to_float(row['UnitsSold'])},  # Use helper function
                {"Name": "ManufacturingPrice", "Type": "number", "Value": to_float(row['ManufacturingPrice'])},  # Use helper function
                {"Name": "SalePrice", "Type": "number", "Value": to_float(row['SalePrice'])},  # Use helper function
                {"Name": "GrossSales", "Type": "number", "Value": to_float(row['GrossSales'])},  # Use helper function
                {"Name": "Discounts", "Type": "number", "Value": to_float(row['Discounts'])},  # Use helper function
                {"Name": "Sales", "Type": "number", "Value": to_float(row['Sales'])},  # Use helper function
                {"Name": "COGS", "Type": "number", "Value": to_float(row['COGS'])},  # Use helper function
                {"Name": "Profit", "Type": "number", "Value": to_float(row['Profit'])},  # Use helper function
                {"Name": "Date", "Type": "string", "Value": row['Date']},  # Keep as string
                {"Name": "MonthNumber", "Type": "number", "Value": int(row['MonthNumber'])},  # Convert to int
                {"Name": "MonthName", "Type": "string", "Value": row['MonthName']},
                {"Name": "Year", "Type": "number", "Value": clean_year(row['Year'])}  # Clean and convert year
            ]
        }
        items.append(item)
    
    # Construct the final payload
    payload = {
        "CollectionName": COLLECTION_NAME,
        "Items": items
    }
    return payload

### Step 4: Send data to Data Lake in batches
def send_data_to_datalake(data, token):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # Split the data into batches of 100 items
    for i in range(0, len(data['Items']), 100):
        batch = data['Items'][i:i + 100]
        batch_payload = {
            "CollectionName": COLLECTION_NAME,
            "Items": batch
        }
        response = requests.post(f"{API_URL}/additems", headers=headers, data=json.dumps(batch_payload))
        
        # Print the response details for debugging
        if response.status_code == 200:
            print(f"Batch {i//100 + 1} sent successfully.")
        else:
            print(f"Failed to send batch {i//100 + 1}: {response.status_code} - {response.text}")

### Main execution
def main(file_path):
    data = read_csv(file_path)
    prepared_data = prepare_data(data)
    token = authenticate()
    send_data_to_datalake(prepared_data, token)

if __name__ == "__main__":
    main(file_path)  # Use the full path defined above
