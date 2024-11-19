# src/monitor_costs.py
import boto3
from datetime import datetime, timedelta

# AWS clients
ce_client = boto3.client('ce')
sns_client = boto3.client('sns')

# SNS Topic ARN for alerts
SNS_TOPIC_ARN = 'arn:aws:sns:your-region:my-account-id:your-sns-topic'

# Cost monitoring configuration
THRESHOLD = 10.00  # Set cost threshold in USD
ALERT_MESSAGE = "Alert: AWS cost threshold exceeded!"

# Fetches costs from AWS Cost Explorer
def get_costs(start_date, end_date):
    response = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='DAILY',
        Metrics=['UnblendedCost'],
        Filter={
            'Dimensions': {
                'Key': 'SERVICE',
                'Values': ['Amazon S3', 'Amazon Redshift']
            }
        }
    )
    return response['ResultsByTime']

# Sends an SNS alert if threshold is exceeded
def send_alert(total_cost):
    message = f"{ALERT_MESSAGE}\nTotal cost for the last week: ${total_cost:.2f}"
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject="AWS Cost Alert"
    )
    print("Alert sent via SNS")

# Main cost monitoring function
def monitor_costs():
    end_date = datetime.utcnow().date()
    start_date = (end_date - timedelta(days=7)).isoformat()  # Last 7 days
    cost_data = get_costs(start_date, end_date.isoformat())
    total_cost = sum(float(day['Total']['UnblendedCost']['Amount']) for day in cost_data)

    print(f"Total cost for the last week: ${total_cost:.2f}")
    if total_cost > THRESHOLD:
        print("Warning: Cost threshold exceeded!")
        send_alert(total_cost)
    else:
        print("Cost is within threshold.")

if __name__ == "__main__":
    monitor_costs()