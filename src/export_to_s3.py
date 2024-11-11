# src/export_to_s3.py
import os
import boto3
from hdfs import InsecureClient
from configparser import ConfigParser

# Load configurations
config = ConfigParser()
config.read('config/config.yaml')

# HDFS and S3 configuration variables
hdfs_url = config.get('hdfs', 'url')
hdfs_directory = config.get('hdfs', 'processed_directory')
s3_bucket_name = config.get('s3', 'bucket_name')
s3_directory = config.get('s3', 'export_directory')
aws_access_key_id = config.get('aws', 'access_key_id')
aws_secret_access_key = config.get('aws', 'secret_access_key')
region_name = config.get('aws', 'region_name')

# Initialize HDFS client
hdfs_client = InsecureClient(hdfs_url)

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

def export_files_to_s3():
    # List all files in the specified HDFS directory
    files = hdfs_client.list(hdfs_directory)
    
    # Iterate over each file and upload to S3
    for file_name in files:
        hdfs_path = f"{hdfs_directory}/{file_name}"
        s3_path = f"{s3_directory}/{file_name}"
        
        # Read file from HDFS
        with hdfs_client.read(hdfs_path) as reader:
            # Upload file to S3
            s3_client.upload_fileobj(reader, s3_bucket_name, s3_path)
        
        print(f"Successfully exported {file_name} from HDFS to S3 at {s3_path}")

if __name__ == "__main__":
    export_files_to_s3()
