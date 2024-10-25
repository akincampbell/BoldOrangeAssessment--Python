import pandas as pd
import requests
import json

# Constants
API_URL = "https://datalake.apidocs.boldorange.com/collections/AkinExample"  # Replace with actual API URL
USERNAME = "acampbell"  # Replace with the provided username
PASSWORD = "rVZ3FkzqcGawjEM952PWL6"  # Replace with the provided password
COLLECTION_NAME = "AkinExample"  # Change this to your collection name

# Step 1: Read the CSV file
def read_csv(file_path):
    data = pd.read_csv(file_path)
    return data

# Step 2: Prepare the data for API
def prepare_data(data):
    # Assuming the data needs to be converted to a list of dictionaries
    return data.to_dict(orient='records')

# Step 3: Authenticate and get token
def authenticate():
    auth_url = f"{API_URL}/auth"  # Adjust based on actual endpoint
    response = requests.post(auth_url, json={"username": USERNAME, "password": PASSWORD})
    if response.status_code == 200:
        return response.json()["token"]
    else:
        raise Exception("Authentication failed.")

# Step 4: Send data to Data Lake
def send_data_to_datalake(data, token):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for record in data:
        response = requests.post(f"{API_URL}/data/{COLLECTION_NAME}", headers=headers, data=json.dumps(record))
        if response.status_code != 200:
            print(f"Failed to send record {record}: {response.text}")

# Main execution
def main(file_path):
    data = read_csv(file_path)
    prepared_data = prepare_data(data)
    token = authenticate()
    send_data_to_datalake(prepared_data, token)

# Example usage
if __name__ == "__main__":
    main("DeveloperAssessmentData.csv")  # Replace with actual path to your CSV file
