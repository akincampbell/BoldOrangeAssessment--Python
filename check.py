import requests

# Constants
API_URL = "https://datalake.boldorange.com/api/v1"  # Base URL for the API
USERNAME = "acampbell"  # Replace with your username
PASSWORD = "rVZ3FkzqcGawjEM952PWL6"  # Replace with your password
COLLECTION_NAME = "AkinExample"  # Change this to your collection name

# Create the authentication URL
auth_url = f"{API_URL}/authenticate"  # Adjust if the documentation specifies a different path

# Make the request
response = requests.post(auth_url, json={"username": USERNAME, "password": PASSWORD})

# Print the status and response
print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")

