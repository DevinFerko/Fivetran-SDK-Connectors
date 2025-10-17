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
    return [{"table": tbl["name"], "primary_key": tbl["pk"]} for tbl in known_tables]

# Defines Update function for the connector
def update(configuration: dict, state: dict):
    headers = {
        "Authorization": f"Bearer {configuration['LIVECHAT_ACCESS_TOKEN']}",
        "Accept": "application/json"
    }
    # Map table names to API endpoints
    endpoint_map = {
        "chats": "https://api.livechatinc.com/v3.5/agent/action/list_archives" # Must be updated
        #"threads": "https://api.livechatinc.com/v3.5/agent/action/list_threads",
        #"events": "https://api.livechatinc.com/v3.5/agent/action/list_events"
    }

    # Loop through all tables defined in schema
    for table in op.tables():
        url = endpoint_map.get(table)
        if not url:
            log.error(f"No endpoint defined for table '{table}', skipping...")
            continue

        all_records = []
        page_id = state.get(f"{table}_page_id")  # separate pagination per table

        while True:
            params = {"page_id": page_id} if page_id else {}
            response = requests.post(url, headers=headers, json=params)
            response.raise_for_status()
            data = response.json()

            # Adjust if API wraps items differently
            records = data.get("items", [])
            all_records.extend(records)

            page_id = data.get("next_page_id")
            if not page_id:
                break

        # Send data to Fivetran
        op.upsert(table=table, records=all_records)

        # Update state per table
        state[f"{table}_page_id"] = page_id

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