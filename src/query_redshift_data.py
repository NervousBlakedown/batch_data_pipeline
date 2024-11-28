# src/query_redshift_data.py
import psycopg2
from configparser import ConfigParser

# Load configurations
config = ConfigParser()
config.read('config/config.yaml')

# Redshift configuration variables
redshift_host = config.get('redshift', 'host')
redshift_db = config.get('redshift', 'db_name')
redshift_user = config.get('redshift', 'user')
redshift_password = config.get('redshift', 'password')
redshift_port = config.get('redshift', 'port')

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

def execute_query(conn, query):
    """
    Execute a given query and print results.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    for row in rows:
        print(row)

if __name__ == "__main__":
    conn = connect_redshift()

    # Example Query: Total Row Count
    query_total_rows = "SELECT COUNT(*) FROM your_table_name"
    print("Total Rows in your_table_name:")
    execute_query(conn, query_total_rows)

    # Example Query: Top 5 Records
    query_top_5 = "SELECT * FROM your_table_name LIMIT 5"
    print("Top 5 Records in your_table_name:")
    execute_query(conn, query_top_5)

    # Additional queries can be added here as needed

    conn.close()
