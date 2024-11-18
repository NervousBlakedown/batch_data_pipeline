#!/bin/bash

# Define directories and tables to clean up
HDFS_PROCESSED_DIR="/data/processed/status_code_counts"
S3_BUCKET="my-s3-bucket-name"
S3_DIR="processed_data"
REDSHIFT_TABLE="my_table_name"

# Step 1: Clean up HDFS processed data
echo "Cleaning up processed data in HDFS..."
hadoop fs -rm -r -skipTrash ${HDFS_PROCESSED_DIR}
if [ $? -eq 0 ]; then
    echo "HDFS processed data cleaned up successfully."
else
    echo "Failed to clean up HDFS processed data. Please check HDFS logs."
fi

# Step 2: Optionally clean up S3 processed data
read -p "Do you want to delete data from S3? (y/n): " DELETE_S3
if [ "$DELETE_S3" == "y" ]; then
    echo "Cleaning up processed data in S3..."
    aws s3 rm s3://${S3_BUCKET}/${S3_DIR}/ --recursive
    if [ $? -eq 0 ]; then
        echo "S3 processed data cleaned up successfully."
    else
        echo "Failed to clean up S3 processed data. Please check AWS logs."
    fi
fi

# Step 3: Optionally clear Redshift table data
read -p "Do you want to delete data from Redshift? (y/n): " DELETE_REDSHIFT
if [ "$DELETE_REDSHIFT" == "y" ]; then
    echo "Cleaning up data in Redshift table ${REDSHIFT_TABLE}..."
    psql -h your-redshift-cluster.endpoint -p 5439 -U your_user -d your_db -c "TRUNCATE TABLE ${REDSHIFT_TABLE};"
    if [ $? -eq 0 ]; then
        echo "Redshift table data cleaned up successfully."
    else
        echo "Failed to clean up Redshift table data. Please check Redshift logs."
    fi
fi

echo "Cleanup completed."
