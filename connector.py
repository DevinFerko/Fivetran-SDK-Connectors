from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests
from datetime import datetime, timezone, timedelta

# --- 1. Define schema ---
def get_schema():
    return {
        "name": "chats",
        "primary_key": ["chat_id"],
        "columns": [
            {"name": "chat_id", "type": "STRING"},
            {"name": "start_time", "type": "DATE_TIME"},
            {"name": "end_time", "type": "DATE_TIME"},
            {"name": "agent_id", "type": "STRING"},
            {"name": "agent_name", "type": "STRING"},
            {"name": "group_id", "type": "INT"},
            {"name": "tags", "type": "STRING"},
            {"name": "duration", "type": "INT"},
            {"name": "rating", "type": "INT"},
            {"name": "customer_email", "type": "STRING"},
            {"name": "customer_ip", "type": "STRING"},
        ]
    }

# --- 2. Define update function ---
def update(config, state):
    auth = requests.auth.HTTPBasicAuth(config["livechat_email"], config["livechat_api_key"])
    last_synced = state.get("chats", {}).get(
        "last_synced_time",
        (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    )

    # Example API fetch
    response = requests.get(
        "https://api.livechatinc.com/v3.3/reporting/chats",
        auth=auth,
        params={"date_from": last_synced}
    )
    data = response.json()

    for chat in data.get("chats", []):
        op.upsert("chats", chat)

    # Update state
    state["chats"] = {"last_synced_time": datetime.now(timezone.utc).isoformat()}
    return state

# --- 3. Define the connector object ---
connector = Connector(
    update=update,
    schema=get_schema()
)
