from fivetran_connector_sdk import Connector
import requests, json, os

CONFIG_PATH = "configuration.json"
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

LIVECHAT_ACCESS_TOKEN = cfg["LIVECHAT_ACCESS_TOKEN"]
LIVECHAT_API_BASE = "https://api.livechatinc.com/v3.5/agent"

connector = Connector(update=True)

# Helper functions for responses
def success_response(data):
    return {"status": "success", "data": data}

def failure_response(message):
    return {"status": "failure", "message": message}

@connector.Schema
def schema(config, context):
    return success_response({
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
    })

@connector.Read
def read(config, state, context):
    headers = {"Authorization": f"Bearer {LIVECHAT_ACCESS_TOKEN}"}
    try:
        resp = requests.get(f"{LIVECHAT_API_BASE}/chats", headers=headers)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        return failure_response(f"Failed to fetch chats: {str(e)}")

    chats = []
    for c in data.get("chats", []):
        chats.append({
            "chat_id": c.get("id"),
            "start_time": c.get("started_at"),
            "end_time": c.get("ended_at"),
            "agent_id": c.get("agents", [{}])[0].get("id"),
            "agent_name": c.get("agents", [{}])[0].get("name"),
            "group_id": c.get("group", {}).get("id", 0),
            "tags": ",".join(c.get("tags", [])),
            "duration": c.get("duration", 0),
            "rating": c.get("rating", {}).get("score"),
            "customer_email": c.get("visitor", {}).get("email"),
            "customer_ip": c.get("visitor", {}).get("ip")
        })

    return success_response({"chats": chats})

@connector.Test
def test(config, context):
    headers = {"Authorization": f"Bearer {LIVECHAT_ACCESS_TOKEN}"}
    try:
        r = requests.get(f"{LIVECHAT_API_BASE}/chats", headers=headers)
        r.raise_for_status()
        return success_response({"message": "LiveChat connection OK"})
    except requests.RequestException as e:
        return failure_response(f"LiveChat test failed: {str(e)}")

app = connector.app
