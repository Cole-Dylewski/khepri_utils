# Import necessary libraries
import logging
import requests
import pandas as pd
from datetime import datetime
import time
import base64

# Configure logging to display information with timestamps and severity level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# BrokerClient class for interacting with the Alpaca broker API
class BrokerClient:
    def __init__(self, api_key, api_secret, base_url=r'https://broker-api.sandbox.alpaca.markets', api_version='v1'):
        """
        Initialize the BrokerClient with API credentials and base URL.
        
        :param api_key: API key provided by Alpaca
        :param api_secret: API secret provided by Alpaca
        :param base_url: Base URL for the Alpaca Broker API
        :param api_version: API version to use (default is 'v1')
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.api_version = api_version

        # Base64 encode the API key and secret
        credentials = f"{self.api_key}:{self.api_secret}".encode('utf-8')
        encoded_credentials = base64.b64encode(credentials).decode('utf-8')

        # Store the headers with base64-encoded credentials
        self.headers = {
            'Authorization': f"Basic {encoded_credentials}"
        }

    def get_all_accounts(self):
        """
        Retrieve all accounts associated with the API key.
        
        :return: A list of account details if the request is successful, otherwise None.
        """
        # Construct the endpoint URL
        endpoint = f"{self.base_url}/{self.api_version}/accounts"

        try:
            # Make the GET request to the API
            response = requests.get(endpoint, headers=self.headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                logging.info("Successfully retrieved all accounts.")
                return response.json()  # Return the response in JSON format
            else:
                logging.error(f"Failed to retrieve accounts. Status code: {response.status_code}, Response: {response.text}")
                return None  # Return None if the request failed
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred while retrieving accounts: {e}")
            return None  # Return None in case of an exception

# Example usage:
# client = BrokerClient(api_key='your_api_key', api_secret='your_api_secret')
# accounts = client.get_all_accounts()
# print(accounts)
git config --global user.name "FIRST_NAME LAST_NAME"
git config --global user.email "MY_NAME@example.com"