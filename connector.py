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
    if 'LIVECHAT_ACCESS_TOKEN' not in configuration:
        # Log error if key is missing
        raise ValueError("Configuration must include 'my_key'.")
    
    # Known/Expected Resource list
    known_tables = [
        {"name": "chats", "pk": ["id"]}
        #{"name": "threads", "pk": ["thread_id"]},
        #{"name": "events", "pk": ["event_id"]},
    ]

    # Return schema definition
    schema_list = []
    for tbl in known_tables:
        schema_list.append({
            "table": tbl["name"],
            "primary_key": tbl["pk"]
        })
    return schema_list

# Defines Update function for the connector
def update(configuration: dict, table: str, state: dict):
    headers = {
        "Authorization": f"Bearer {configuration['LIVECHAT_ACCESS_TOKEN']}",
        "Accept": "application/json"
    }
    # Map table names to API endpoints
    endpoint_map = {
        "chats": "https://api.text.com/v2/chats" # Must be updated
        #"threads": "https://api.text.com/v2/threads",
        #"events": "https://api.text.com/v2/events"
    }

    # Validate table
    if table not in endpoint_map:
        raise ValueError(f"Unsupported table: {table}")

    # Make API request
    url = endpoint_map[table]
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()

    # You may need to extract the list of items from a key, depending on API shape:
    # e.g. if response looks like {"chats": [ ... ]}
    if table in data:
        records = data[table]
    else:
        records = data.get("data", data)  # fallback

    # Optionally handle pagination here if the API paginates results
    # (e.g. using next_page_token or links)

    return records, state

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