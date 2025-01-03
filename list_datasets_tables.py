import boto3
import psycopg2
from psycopg2 import sql, OperationalError

def get_secret(secret_name, region_name):
    """Retrieve the secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]
        return eval(secret)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def show_table_contents(secret):
    """Connect to the PostgreSQL database and show contents of all tables."""
    try:
        connection = psycopg2.connect(
            host=secret["host"],
            user=secret["username"],
            password=secret["password"],
            database=secret["dbname"],
            port=secret["port"]
        )
        cursor = connection.cursor()

        # First get list of tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()

        # For each table, show its contents
        for table in tables:
            table_name = table[0]
            print(f"\n=== Contents of {table_name} ===")
            
            # Get column names
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """)
            columns = [col[0] for col in cursor.fetchall()]
            print("Columns:", ", ".join(columns))

            # Get table contents
            cursor.execute(sql.SQL("SELECT * FROM {} LIMIT 5").format(
                sql.Identifier(table_name)
            ))
            rows = cursor.fetchall()
            
            if rows:
                print("\nFirst 5 rows:")
                for row in rows:
                    print(row)
            else:
                print("\nTable is empty")

            # Get row count
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            ))
            count = cursor.fetchone()[0]
            print(f"\nTotal rows: {count}")

    except OperationalError as e:
        print(f"Error connecting to database: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    secret_name = "amazon-relational-database-secrets"
    region_name = "ap-southeast-1"
    
    secret = get_secret(secret_name, region_name)
    if secret:
        show_table_contents(secret)
