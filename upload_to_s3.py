import boto3
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a session with the correct region
session = boto3.Session(region_name='us-west-1')
s3 = session.client('s3')

bucket_name = 'data-bucket-1988'
base_path = r"C:\Users\orand\OneDrive\Masters of Data Science\INFO-I535\Assignments\Final Project\customer-support-uploader"

def upload_file(local_path, s3_path):
    full_local_path = os.path.join(base_path, local_path)
    try:
        logging.info(f"Uploading {full_local_path} to s3://{bucket_name}/{s3_path}")
        s3.upload_file(full_local_path, bucket_name, s3_path)
    except Exception as e:
        logging.error(f"Failed to upload {full_local_path}: {e}")

def bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except s3.exceptions.ClientError as e:
        logging.error(f"Bucket '{bucket_name}' not found or inaccessible: {e}")
        return False

def main():
    if not bucket_exists(bucket_name):
        return

    # Upload all .csv files in customer_support_tickets folder
    tickets_path = os.path.join(base_path, 'customer_support_tickets')
    for file in os.listdir(tickets_path):
        if file.endswith('.csv'):
            upload_file(os.path.join('customer_support_tickets', file), f'raw/customer_support_tickets/{file}')

    # Upload all .csv files in ticket_sentiments folder
    sentiments_path = os.path.join(base_path, 'ticket_sentiments')
    for file in os.listdir(sentiments_path):
        if file.endswith('.csv'):
            upload_file(os.path.join('ticket_sentiments', file), f'raw/ticket_sentiments/{file}')

    # Upload all .json files in customer_service_reps folder
    reps_path = os.path.join(base_path, 'customer_service_reps')
    for file in os.listdir(reps_path):
        if file.endswith('.json'):
            upload_file(os.path.join('customer_service_reps', file), f'raw/customer_service_reps/{file}')

if __name__ == "__main__":
    main()
