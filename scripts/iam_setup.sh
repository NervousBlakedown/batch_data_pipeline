#!/bin/bash
echo "Setting up IAM roles and policies..."
python3 src/manage_iam_roles.py
echo "IAM setup completed."
