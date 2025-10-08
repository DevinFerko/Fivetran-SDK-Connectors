from fivetran_connector_sdk import Connector, Response
import requests, json, os

CONFIG_PATH = "configuration.json"
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

LIVECHAT_ACCESS_TOKEN = cfg["LIVECHAT_ACCESS_TOKEN"]
LIVECHAT_API_BASE = "https://api.livechatinc.com/v3.5/agent"

connector = Connector()

@connector.Schema
def schema(config):
    return Response.success({
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
def read(config, state):
    headers = {"Authorization": f"Bearer {LIVECHAT_ACCESS_TOKEN}"}
    resp = requests.get(f"{LIVECHAT_API_BASE}/chats", headers=headers)
    resp.raise_for_status()
    data = resp.json()

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

    return Response.success(records={"chats": chats})

@connector.Test
def test(config):
    headers = {"Authorization": f"Bearer {LIVECHAT_ACCESS_TOKEN}"}
    r = requests.get(f"{LIVECHAT_API_BASE}/chats", headers=headers)
    if r.status_code == 200:
        return Response.success(message="LiveChat connection OK")
    return Response.failure(message=f"LiveChat returned {r.status_code}: {r.text}")

app = connector.app