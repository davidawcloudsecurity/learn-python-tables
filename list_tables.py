import boto3
import psycopg2
from psycopg2 import sql, OperationalError

def get_secret(secret_name, region_name):
    """Retrieve the secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]
        return eval(secret)  # Convert the JSON string to a dictionary
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def list_tables(secret):
    """Connect to the PostgreSQL database and list tables."""
    try:
        connection = psycopg2.connect(
            host=secret["host"],
            user=secret["username"],
            password=secret["password"],
            database=secret["dbname"],
            port=secret["port"]
        )
        cursor = connection.cursor()

        # Query to list all tables in the public schema
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(table[0])
    except OperationalError as e:
        print(f"Error connecting to database: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    # Replace with your secret name and region
    secret_name = "amazon-relational-database-secrets"
    region_name = "ap-southeast-1"
    
    secret = get_secret(secret_name, region_name)
    if secret:
        list_tables(secret)
