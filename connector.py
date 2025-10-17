"""
This connector module provides the necessary classes and functions to integrate with Fivetran's SDK to pull chat data from the LiveChat API. 
"""

# Import necessary modules
import json
import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

# Message to check key connectivity
CONNECTIVITY_MESSAGE = "Successfully connected to LiveChat API. Key is valid."

# Defines Schema for the connector
def schema(configuration: dict):
    # Validate configuration
    if 'my_key' not in configuration:
        # Log error if key is missing
        raise ValueError("Configuration must include 'my_key'.")
    
    # Return schema definition
    return [
        {
            "table": "lists_chats", 
            "primary_key": ["id"]
        }
    ]

# Defines Update function for the connector
def update(configuration: dict, state: dict):
    # Placeholder for update logic
    return state





# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)