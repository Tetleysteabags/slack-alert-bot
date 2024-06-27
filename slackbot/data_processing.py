import os
from google.cloud import bigquery, bigquery_storage
import google.auth
import pandas as pd
from datetime import timedelta

# Set the credentials for BQ
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google-auth-creds.json"

credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
bqclient = bigquery.Client(credentials=credentials, project=your_project_id)
bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)

def fetch_data_from_bigquery():
    query_string = """SELECT * FROM `bigqpr.table_name_here`  
    WHERE
    DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 WEEK)
    AND DATE(date) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND ad_network IN ('Google Ads', 'Meta Ads') 
    """

    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return df

def process_data(df):
    try:
        df['date'] = pd.to_datetime(df['date'], format='%Y/%m/%d')
        df.rename(columns={'spend': 'Spend'}, inplace=True)
        df = df.groupby([pd.Grouper(key='date', axis=0, freq='1D', sort=True), 'ad_network']).sum(numeric_only=True).reset_index().fillna(0)
        # df_network = df.groupby(['date', 'ad_network']).sum(numeric_only=True).reset_index().fillna(0)

        #### LAST WEEK DATA
        spend_last_week = df[df['date'] >= df['date'].max()]['Spend'].sum(numeric_only=True).round(1)
        revenue_last_week = df[df['date'] >= df['date'].max()]['Revenue_D7'].sum(numeric_only=True).round(1)
        trials_last_week = df[df['date'] >= df['date'].max()]['Trials_D3'].sum(numeric_only=True).round(1)
        cpt_last_week = df[df['date'] >= df['date'].max()]['Spend'].sum(numeric_only=True) / df[df['date'] >= df['date'].max()]['Trials_D3'].sum().round(1)
        roas_last_week = ((df[df['date'] >= df['date'].max()]['Revenue_D7'].sum(numeric_only=True) / df[df['date'] >= df['date'].max()]['Spend'].sum()) * 100).round(1)

        spend_prev_last_week = df[(df['date'] < df['date'].max()) & (df['date'] >= df['date'].max() - timedelta(days=7))]['Spend'].sum().round(1)
        revenue_prev_last_week = df[(df['date'] < df['date'].max()) & (df['date'] >= df['date'].max() - timedelta(days=7))]['Revenue_D7'].sum().round(1)
        trials_prev_last_week = df[(df['date'] < df['date'].max()) & (df['date'] >= df['date'].max() - timedelta(days=7))]['Trials_D3'].sum().round(1)
        cpt_prev_last_week = df[(df['date'] < df['date'].max()) & (df['date'] >= df['date'].max() - timedelta(days=7))]['Spend'].sum() / df[(df['date'] < df['date'].max()) & (df['date'] >= df['date'].max() - timedelta(days=7))]['Trials_D3'].sum().round(1)
        roas_prev_last_week = ((df[(df['date'] < df['date'].max()) & (df['date'] >= df['date'].max() - timedelta(days=7))]['Revenue_D7'].sum() / df[(df['date'] < df['date'].max()) & (df['date'] >= df['date'].max() - timedelta(days=7))]['Spend'].sum()) * 100).round(1)

        spend_comparison = (spend_last_week - spend_prev_last_week) / spend_prev_last_week
        cpt_comparison = (cpt_last_week - cpt_prev_last_week) / cpt_prev_last_week
        roas_comparison = (roas_last_week - roas_prev_last_week) / roas_prev_last_week
        revenue_comparison = (revenue_last_week - revenue_prev_last_week) / revenue_prev_last_week
        trials_comparison = (trials_last_week - trials_prev_last_week) / trials_prev_last_week

        df['CPT'] = df['Spend'] / df['Trials_D3']
        df['ROAS'] = df['Revenue_D7'] / df['Spend']

        df.dropna(subset=['date'], inplace=True)
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(x).strftime("%Y-%m-%d"))

        return {
            'df': df,
            'spend_last_week': spend_last_week,
            'revenue_last_week': revenue_last_week,
            'trials_last_week': trials_last_week,
            'cpt_last_week': cpt_last_week,
            'roas_last_week': roas_last_week,
            'spend_prev_last_week': spend_prev_last_week,
            'revenue_prev_last_week': revenue_prev_last_week,
            'trials_prev_last_week': trials_prev_last_week,
            'cpt_prev_last_week': cpt_prev_last_week,
            'roas_prev_last_week': roas_prev_last_week,
            'spend_comparison': spend_comparison,
            'cpt_comparison': cpt_comparison,
            'roas_comparison': roas_comparison,
            'revenue_comparison': revenue_comparison,
            'trials_comparison': trials_comparison
        }
    
    except Exception as e:
        print(f'Error fetching or processing data: {e}')
        return None

def update_daily_data():
    try:
        df = fetch_data_from_bigquery()
        processed_data = process_data(df)
        return processed_data
    except Exception as e:
        print(f'Error fetching or processing data: {e}')
        return None

def create_daily_annotations(updated_data):
    df = updated_data.get('df')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    platform_emojis = {
        'Apple Search Ads': '🍏',
        'Google Ads': '🤖',
        'Meta Ads': '🔵',
        'TikTok Ads': '🎵'
    }

    network_emojis = {
        'android': '🤖',
        'ios': '🍏',
        'all': '🌍'
    }

    metric_emojis = {
        'Spend': '💰',
        'Trials_D3': '🔍',
        'Revenue_D7': '💸',
        'CPT': '📊',
        'ROAS': '📈'
    }
    positive_negative = {
        'negative': '🔴',
        'positive': '🟢'
    }

    insights = []
    insights.append(f"*Daily insights for {df['date'].max().strftime('%Y-%m-%d')}:*")

    for ad_network in df['ad_network'].unique():
        df_ad_network = df[df['ad_network'] == ad_network].sort_values(by='date')

        if len(df_ad_network) < 2:
            insights.append(f"Not enough data for {ad_network} to generate insights.")
            insights.append("---")
            continue

        df_ad_network['spend_moving_avg'] = df_ad_network['Spend'].rolling(window=7).mean()
        df_ad_network['trials_moving_avg'] = df_ad_network['Trials_D3'].rolling(window=7).mean()
        df_ad_network['cpt_moving_avg'] = df_ad_network['CPT'].rolling(window=7).mean()
        df_ad_network['revenue_moving_avg'] = df_ad_network['Revenue_D7'].rolling(window=7).mean()
        df_ad_network['roas_moving_avg'] = df_ad_network['ROAS'].rolling(window=7).mean()

        current_day = df_ad_network.iloc[-1]
        previous_day_index = -2
        while current_day['date'] == df_ad_network.iloc[previous_day_index]['date']:
            previous_day_index -= 1
            if abs(previous_day_index) > len(df_ad_network):
                break
        previous_day = df_ad_network.iloc[previous_day_index]

        platform_emoji = platform_emojis.get(ad_network, '')

        for metric in ['Spend', 'Trials_D3', 'CPT', 'Revenue_D7', 'ROAS']:
            insights.append(f"*{metric} insights for {platform_emoji} {ad_network}:*")

            actual = round(current_day[metric], 2)
            if metric == 'ROAS':
                actual_percentage = current_day[metric] * 100
                if actual_percentage > 0:
                    insights.append(f"{metric_emojis[metric]} {metric}: {actual_percentage:,.2f}%.")
                else:
                    insights.append(f"{metric_emojis[metric]} {metric} was below 0. Please investigate.")
            else:
                if actual >= 0:
                    if metric == 'Trials_D3':
                        insights.append(f"{metric_emojis[metric]} {metric}: {abs(actual):,}.")
                    else:
                        insights.append(f"{metric_emojis[metric]} {metric}: ${abs(actual):,.2f}.")
                else:
                    insights.append(f"{metric_emojis[metric]} {metric} was below 0. Please investigate.")

            try:
                if pd.isna(previous_day[metric]) or previous_day[metric] == 0:
                    diff_change = float('inf') if current_day[metric] > 0 else float('-inf')
                else:
                    diff_change = round((current_day[metric] - previous_day[metric]) / previous_day[metric] * 100, 2)
                diff = round(current_day[metric] - previous_day[metric], 2)
            except ZeroDivisionError:
                diff_change = float('inf') if current_day[metric] > 0 else float('-inf')
                diff = current_day[metric]

            emoji = positive_negative['positive'] if diff >= 0 else positive_negative['negative']

            if metric == 'ROAS':
                diff_percentage_points = round((current_day[metric] - previous_day[metric]) * 100, 2)
                if diff_percentage_points < 0:
                    insights.append(f"{emoji} {metric} decreased by {abs(diff_percentage_points):,.2f} percentage points compared to the previous day.")
                else:
                    insights.append(f"{emoji} {metric} increased by {diff_percentage_points:,.2f} percentage points compared to the previous day.")
            else:
                if diff < 0:
                    if metric in ['Spend', 'CPT']:
                        insights.append(f"{emoji} {metric} decreased by ${abs(diff):,.2f} ({abs(diff_change)}%) compared to the previous day.")
                    else:
                        insights.append(f"{emoji} {metric} decreased by {abs(diff):,.2f} ({abs(diff_change)}%) compared to the previous day.")
                else:
                    if metric in ['Spend', 'CPT']:
                        insights.append(f"{emoji} {metric} increased by ${diff:,.2f} ({diff_change}%) compared to the previous day.")
                    else:
                        insights.append(f"{emoji} {metric} increased by {diff:,.2f} ({diff_change}%) compared to the previous day.")

        insights.append("---")

    return insights
