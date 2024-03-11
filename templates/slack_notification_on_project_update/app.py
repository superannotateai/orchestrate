"""
Python Version Compatibility: 3.8, 3.9, 3.10, 3.11
Dependencies:
    superannotate library, version 4.4.19 or higher
    slack-sdk library, version 3.27.1 or higher

This script sends a message to a Slack channel using the Slack API.

Before running the script, make sure to set the following environment variables:
- SLACK_TOKEN: The Slack API token.
- CHANNEL_ID: The ID of the Slack channel where the message will be sent.
- SA_TOKEN: The SuperAnnotate SDK token.

You can define key-value variables from the Secrets page of the Actions tab in Orchestrate. You can then mount this secret to a custom action in your pipeline.

Please refer to the documentation for more details:
https://doc.superannotate.com/docs/create-automation#secrets.


The `handler` function  triggers the script upon an event [Project Status Update].

"""

import os
import logging

from superannotate import SAClient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logging.basicConfig(level=logging.INFO)

SLACK_TOKEN = os.environ['SLACK_TOKEN']
CHANNEL_ID = os.environ['CHANNEL_ID']


def handler(event, context):
    sa = SAClient()

    current_state = context['after']
    project_name = sa.get_project_by_id(current_state['project_id'])['name']

    # Change the message to be sent
    message = f"Ready for annotation: {project_name} has been updated"

    slack_client = WebClient(token=SLACK_TOKEN)
    # Call the conversations.list method using the WebClient
    slack_client.chat_postMessage(
        channel=CHANNEL_ID,
        text=message
        # You could also use a blocks[] array to send richer content
    )
