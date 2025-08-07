import boto3
from datetime import datetime, timezone, timedelta
import re



# Create boto3 clients for EMR and SES
client = boto3.client('emr')
ses_client = boto3.client('ses')  # Replace with your SES region

# Define parameters
threshold_hours = 3
now = datetime.now(timezone.utc)
default_mail = 'ops@domain.com'  # Default email if no owner tag is found
email_pattern = r'^[^@]+@[^@]+\.[^@]+$'

# Create a function to send mail to the owner of the cluster
def send_email(cluster_id, cluster_name, duration, recipients):
    subject = f"[ALERT] Long Running EMR Cluster {cluster_name} ({cluster_id})"
    body_text = f"""
    EMR Cluster Alert:

    Cluster Name: {cluster_name}
    Cluster ID: {cluster_id}
    Running Duration: {duration}

    Kindly review the cluster and take appropriate action if it is no longer in use.

    Regards,
    XYZ
    """

    # If recipients found, send to them and CC default; else send to default
    if recipients:
        destination = {
            'ToAddresses': recipients,
            'CcAddresses': [default_mail]
        }
    else:
        destination = {
            'ToAddresses': [default_mail]
        }

    try:
        response = ses_client.send_email(
            Source='xyz@domain.com',  # Sender email is usually fixed. Must be verified in SES
            Destination=destination,
            Message={
                'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                'Body': {
                    'Text': {'Data': body_text, 'Charset': 'UTF-8'}
                }
            }
        )
        print(f"[INFO] Email sent for cluster {cluster_id}")
    except Exception as e:
        print(f"[ERROR] Failed to send email for cluster {cluster_id}: {e}")

# Create paginator for listing clusters
paginator = client.get_paginator('list_clusters')

# Get all non-terminated clusters
response_iterator = paginator.paginate(
    ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
)

# Iterate through clusters and check their duration
for page in response_iterator:
    for cluster in page['Clusters']:
        cluster_id = cluster['Id']
        cluster_name = cluster['Name']
        ready_time = cluster['Status']['Timeline'].get('ReadyDateTime')

        if not ready_time:
            continue  # Skip if ReadyDateTime is missing

        duration = now - ready_time
        if duration >= timedelta(hours=threshold_hours):
            # Describe the cluster to get tags to get Owner
            try:
                response = client.describe_cluster(ClusterId=cluster_id)
                tags = response['Cluster'].get('Tags', [])
                recipients = []
                for tag in tags:
                    if tag['Key'].lower() == 'owner' and tag['Value']:
                        if re.match(email_pattern, tag['Value']):
                            recipients.append(tag['Value'].strip())
                        break
                send_email(cluster_id, cluster_name, duration, recipients)
            except Exception as e:
                print(f"[ERROR] Failed to describe cluster {cluster_id}: {e}")
