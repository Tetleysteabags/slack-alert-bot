import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

slack_token = os.getenv('SLACK_TOKEN')
slack_client = WebClient(token=slack_token)

def send_to_slack(insights):
    try:
        channel_id = 'C079KNDP8NM'  # Replace with your actual Slack channel ID
        message = "\n".join(insights)
        slack_client.chat_postMessage(channel=channel_id, text=message)
        return True
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e.response['error']}")
        return False
