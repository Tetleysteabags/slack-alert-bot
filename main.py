# main.py
from dotenv import load_dotenv
import os

load_dotenv()

from slackbot import update_daily_data, create_daily_annotations, send_to_slack

def main(request):
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Fetch and process data
    updated_data = update_daily_data()
    
    if updated_data:
        # Create daily annotations
        insights = create_daily_annotations(updated_data)
        
        # Send insights to Slack
        if send_to_slack(insights):
            print("Insights sent to Slack successfully.")
        else:
            print("Failed to send insights to Slack.")
    else:
        print("Failed to fetch and process data.")

if __name__ == '__main__':
    main(None)
