# src/manage_iam_roles.py
import boto3
from botocore.exceptions import ClientError

# Initialize boto3 clients
iam_client = boto3.client('iam')
s3_client = boto3.client('s3')
redshift_client = boto3.client('redshift')

def create_iam_role(role_name, assume_policy_document):
    """Create an IAM role with a specified trust policy."""
    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_policy_document,
            Description="IAM Role for secure access to AWS resources in the data pipeline"
        )
        print(f"Role '{role_name}' created successfully.")
        return response['Role']['Arn']
    except ClientError as e:
        print(f"Failed to create role {role_name}: {e}")
        return None

def attach_policy_to_role(role_name, policy_arn):
    """
    Attach an existing AWS managed or custom policy to the role.
    """
    try:
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        print(f"Policy '{policy_arn}' attached to role '{role_name}' successfully.")
    except ClientError as e:
        print(f"Failed to attach policy {policy_arn} to role {role_name}: {e}")

def create_custom_policy(policy_name, policy_document):
    """
    Create a custom policy.
    """
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=policy_document
        )
        print(f"Policy '{policy_name}' created successfully.")
        return response['Policy']['Arn']
    except ClientError as e:
        print(f"Failed to create policy {policy_name}: {e}")
        return None

def configure_redshift_role():
    """
    Create a role for Redshift with S3 access for COPY operations.
    """
    role_name = "RedshiftS3AccessRole"
    
    # Define the trust policy to allow Redshift to assume this role
    assume_policy_document = """
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Principal": {
            "Service": "redshift.amazonaws.com"
          },
          "Action": "sts:AssumeRole"
        }
      ]
    }
    """
    
    role_arn = create_iam_role(role_name, assume_policy_document)
    
    if role_arn:
        # Attach AmazonS3ReadOnlyAccess to allow Redshift access to S3 for COPY
        attach_policy_to_role(role_name, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        
        # Optionally, create and attach a custom policy with restricted permissions
        policy_document = """
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:ListBucket"
              ],
              "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
              ]
            }
          ]
        }
        """
        custom_policy_arn = create_custom_policy("RestrictedS3AccessForRedshift", policy_document)
        if custom_policy_arn:
            attach_policy_to_role(role_name, custom_policy_arn)
    
    return role_arn

if __name__ == "__main__":
    # Run the configuration for the Redshift role
    redshift_role_arn = configure_redshift_role()
    if redshift_role_arn:
        print(f"Redshift role created with ARN: {redshift_role_arn}")
