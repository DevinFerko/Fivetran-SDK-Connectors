# -*- coding: utf-8 -*-

from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests
from datetime import datetime, timezone, timedelta

# --- 1. Define schema ---
def get_schema(config):
    return {
        "chats": {
            "primary_key": ["chat_id"],
            "columns": {
                "chat_id": "string",
                "start_time": "string",
                "end_time": "string",
                "agent_id": "string",
                "agent_name": "string",
                "group_id": "int",
                "tags": "string",
                "duration": "int",
                "rating": "int",
                "customer_email": "string",
                "customer_ip": "string"
            }
        }
    }

# --- 2. Define update function ---
def update(config, state):
    auth = requests.auth.HTTPBasicAuth(
        config["livechat_email"], config["livechat_api_key"]
    )

    last_synced = state.get("chats", {}).get(
        "last_synced_time",
        (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
    )

    response = requests.get(
        "https://api.livechatinc.com/v3.3/reporting/chats",
        auth=auth,
        params={"date_from": last_synced},
    )

    if response.status_code != 200:
        log.error(f"API request failed with status code {response.status_code}: {response.text}")
        return state

    try:
        data = response.json()
    except Exception as e:
        log.error(f"Failed to parse JSON response: {e}")
        return state

    for chat in data.get("chats", []):
        if isinstance(chat, dict):
            op.upsert("chats", chat)
        else:
            log.warning(f"Unexpected record format: {chat}")

    # Update sync state
    state["chats"] = {"last_synced_time": datetime.now(timezone.utc).isoformat()}
    return state

# --- 3. Define the connector object ---
connector = Connector(
    update=update,
    schema=get_schema
)
