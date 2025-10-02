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
        name=TABLOE_NAME,
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

        