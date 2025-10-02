"""This connector demonstrates how to fetch data from Livechat API and upsert it into destination using the Fivetran Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

from fivetran_sdk.engine import run

from fivetran_sdk import (
    BaseConnector,
    Schema,
    Table,
    Column,
    SyncType,
    DataType,
    Connector,
    ReadResult,
)

# Import required libraries
import os
from datetime import datetime, timezone, timedelta
from typing import Iterator, Dict, Any, Optional
import json  # For JSON data handling and serialization
import requests  # For making HTTP requests to the Checkly API
import time  # For handling time-related functions like sleep for rate limiting

# --- 1. Schema and Stream Definition ---

class LiveChatChatsStream:

    """
    Defines the schema and reading logic for the 'chats' table.
    This stream will perform an incremental sync based on a timestamp.
    """
    TABLE_NAME = "chats"
    PRIMARY_KEY = ["chat_id"]
    API_ENDPOINT = "https://api.livechatinc.com/v3.3/reporting/chats"

    # Defines the structure for the chats table
    SCHEMA = Schema(
        name=TABLE_NAME,
        primary_key=PRIMARY_KEY,
        sync_type=SyncType.INCREMENTAL,
        columns=[
            Column(name="chat_id", type=DataType.STRING, primary_key=True),
            Column(name="start_time", type=DataType.DATE_TIME),
            Column(name="end_time", type=DataType.DATE_TIME),
            Column(name="agent_id", type=DataType.STRING),
            Column(name="agent_name", type=DataType.STRING),
            Column(name="group_id", type=DataType.INT),
            Column(name="tags", type=DataType.STRING), # Store tags as a comma-separated string
            Column(name="duration", type=DataType.INT),
            Column(name="rating", type=DataType.INT),
            Column(name="customer_email", type=DataType.STRING),
            Column(name="customer_ip", type=DataType.STRING),
        ]
    )

    def get_schema(self) -> Schema:
        """Returns the schema definition for the chats table."""
        return self.SCHEMA
    
    def read_stream(self, connector: Connector) -> Iterator[Dict[str, Any]]:

        # Pulls data from the LiveChat API and yields records.
        config = connector
        state = connector.state or {}

        # Livechat API credentials
        auth_header = requests.auth.HTTPBasicAuth(
            username=config["livechat_email"],
            password=config['"livechat_api_key']
        )

        # Determine the starting point for incremental sync - Fivetran recommends UTC
        last_synced_dt_str = state.get(self.TABLE_NAME, {}).get("last_synced_time")
        if last_synced_dt_str:
            last_synced_dt = datetime.fromisoformat(last_synced_dt_str).replace(tzinfo=timezone.utc)
        else:
            # Default to pulling the last 30days on the first run
            last_synced_dt = datetime.now(timezone.utc) - timedelta(days=30)
        
        start_date_param = last_synced_dt.isoformat().replace('+00:00', 'Z')

        log.info(f"Starting incremental sync for {self.TABLE_NAME} from {start_date_param}")

        # Today as the end date for the initial sync
        end_date_param = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

        # api request parameters
        params = {
            "date_from": start_date_param,
            "date_to": end_date_param,
            "limit": 100,  # Max limit per LiveChat API docs
            "page": 1
        }

        new_last_synced_time = last_synced_dt

        while True:
            log.info(f"Fetching page {params['page']} with date_from={params['date_from']}")

            try:
                response = request.get(
                    url = self.API_ENDPOINT,
                    auth = auth_header,
                    params = params,
                )
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.HTTPError as e:
                log.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
                break
            except requests.exceptions.RequestException as e:
                log.error(f"Request Exception: {str(e)}")
                break
            
            chats = data.get("chats", [])
            
            if not chats:
                log.info("No more chats found or reached last page.")
                break

            # Yield each chat record
            for chat in chats:
                record = {
                    "chat_id": chat.get("chat_id"),
                    "start_time": chat.get("start_time"),
                    "end_time": chat.get("end_time"),
                    "agent_id": chat.get("agent_id"),
                    "agent_name": chat.get("agent_name"),
                    "group_id": chat.get("group_id"),
                    # Join list of tags into a single string
                    "tags": ", ".join(chat.get("tags", [])),
                    "duration": chat.get("duration"),
                    "rating": chat.get("rate"),
                    "customer_email": chat.get("visitor", {}).get("email"),
                    "customer_ip": chat.get("visitor", {}).get("ip"),
                }