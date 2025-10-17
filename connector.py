"""
LiveChat Connector for Fivetran SDK.
Fetches data from LiveChat API (chats, threads, events) and upserts into the destination.
Handles pagination and incremental state per table.
"""

import json
import requests
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# --------------------------
# Configuration validation
# --------------------------
def validate_configuration(configuration: dict):
    """
    Ensure the configuration has the required API token.
    """
    if "LIVECHAT_ACCESS_TOKEN" not in configuration:
        raise ValueError("Configuration must include 'LIVECHAT_ACCESS_TOKEN'")

# --------------------------
# Schema definition
# --------------------------
def schema(configuration: dict):
    """
    Define schema for all tables to be synced.
    """
    return [
        {"table": "chats", "primary_key": ["id"], "columns": {}},
        {"table": "threads", "primary_key": ["id"], "columns": {}},
        {"table": "events", "primary_key": ["id"], "columns": {}},
    ]

# --------------------------
# Update function
# --------------------------
def update(configuration: dict, state: dict):
    """
    Fetch data from LiveChat API and upsert into destination.
    Handles pagination and state checkpointing.
    """
    validate_configuration(configuration)

    headers = {
        "Authorization": f"Bearer {configuration['LIVECHAT_ACCESS_TOKEN']}",
        "Accept": "application/json"
    }

    endpoint_map = {
        "chats": "https://api.livechatinc.com/v3.5/agent/action/list_archives",
        "threads": "https://api.livechatinc.com/v3.5/agent/action/list_threads",
        "events": "https://api.livechatinc.com/v3.5/agent/action/list_events"
    }

    for table, url in endpoint_map.items():
        all_records = []
        page_id = state.get(f"{table}_page_id")

        while True:
            params = {"page_id": page_id} if page_id else {}
            try:
                response = requests.post(url, headers=headers, json=params)
                response.raise_for_status()
            except requests.RequestException as e:
                log.warning(f"[{table}] Error fetching data from LiveChat API: {e}")
                break

            data = response.json()
            records = data.get("items", [])
            all_records.extend(records)

            page_id = data.get("next_page_id")
            if not page_id:
                break

        # Upsert each record for the current table
        for record in all_records:
            op.upsert(table=table, data=record)

        # Save pagination state per table
        state[f"{table}_page_id"] = page_id

    # Checkpoint state after all tables
    op.checkpoint(state)

# --------------------------
# Connector object
# --------------------------
connector = Connector(update=update, schema=schema)

# --------------------------
# Local debug/testing
# --------------------------
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
