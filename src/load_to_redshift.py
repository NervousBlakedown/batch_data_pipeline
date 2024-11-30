# src/load_to_redshift.py
import boto3
import psycopg2
from configparser import ConfigParser

# Load configurations
config = ConfigParser()
config.read('config/config.yaml')

# Redshift and S3 configuration variables
redshift_host = config.get('redshift', 'host')
redshift_db = config.get('redshift', 'db_name')
redshift_user = config.get('redshift', 'user')
redshift_password = config.get('redshift', 'password')
redshift_port = config.get('redshift', 'port')
s3_bucket_name = config.get('s3', 'bucket_name')
s3_export_directory = config.get('s3', 'export_directory')
aws_access_key_id = config.get('aws', 'access_key_id')
aws_secret_access_key = config.get('aws', 'secret_access_key')
iam_role = config.get('redshift', 'iam_role')  # IAM role with Redshift and S3 access

# Redshift connection function
def connect_redshift():
    conn = psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )
    return conn

def load_data_to_redshift():
    conn = connect_redshift()
    cursor = conn.cursor()
    
    # Define the S3 path and COPY command
    s3_path = f"s3://{s3_bucket_name}/{s3_export_directory}/"
    copy_query = f"""
    COPY your_table_name
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT AS CSV
    IGNOREHEADER 1;
    """
    
    # Execute COPY command
    try:
        cursor.execute(copy_query)
        conn.commit()
        print("Data successfully loaded into Redshift.")
    except Exception as e:
        print(f"Error loading data to Redshift: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_data_to_redshift()
