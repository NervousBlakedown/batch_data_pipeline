# src/archive_and_cleanup_s3.py
import boto3
from datetime import datetime, timedelta

s3_client = boto3.client('s3')
bucket_name = "my-s3-bucket-name"
archive_days = 30  # Days after which to archive data
delete_days = 365  # Days after which to delete data

def archive_or_delete_s3_objects():
    # Calculate cutoff dates
    archive_cutoff = datetime.now() - timedelta(days=archive_days)
    delete_cutoff = datetime.now() - timedelta(days=delete_days)
    
    # List objects in the bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        obj_date = obj['LastModified'].replace(tzinfo=None)
        if obj_date < delete_cutoff:
            # Delete old objects
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Deleted {obj['Key']}")
        elif obj_date < archive_cutoff:
            # Move to Glacier for archival
            s3_client.copy_object(
                Bucket=bucket_name,
                Key=obj['Key'],
                CopySource={'Bucket': bucket_name, 'Key': obj['Key']},
                StorageClass='GLACIER'
            )
            print(f"Archived {obj['Key']} to Glacier")

if __name__ == "__main__":
    archive_or_delete_s3_objects()
