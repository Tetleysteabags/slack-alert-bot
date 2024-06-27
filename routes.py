from flask import Blueprint, request, jsonify
from .slackbot.data_processing import update_daily_data, create_daily_annotations
from .slackbot.slack_notifications import send_to_slack


def update_all():
    updated_data = update_daily_data()  

    if not updated_data:
        return jsonify({'status': 'error', 'message': 'Error fetching or processing data'}), 500

    insights = create_daily_annotations(updated_data)

    if send_to_slack(insights):
        return jsonify({'status': 'success', 'message': 'Data updated and message sent to Slack'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Data updated but failed to send message to Slack'}), 500
