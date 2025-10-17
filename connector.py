"""
This connector module provides the necessary classes and functions to integrate with Fivetran's SDK to pull chat data from the LiveChat API. 
"""

# Import necessary modules
import json
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

# Message to check key connectivity
CONNECTIVITY_MESSAGE = "Successfully connected to LiveChat API. Key is valid."