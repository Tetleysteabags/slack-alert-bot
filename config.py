import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SLACK_TOKEN = os.getenv('SLACK_TOKEN')
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

