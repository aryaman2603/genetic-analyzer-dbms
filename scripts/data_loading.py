import pandas as pd
import psycopg2
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get credentials from .env file
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

CSV_FILE_PATH = 'data/processed_sequences.csv'

def populate_genomes():
    """
    Reads genome data from a CSV and inserts it row-by-row to handle
    potential formatting issues in the source file.
    """
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT]):
        print("❌ Error: One or more database environment variables are not set.")
        sys.exit(1)

    conn = None
    try:
        print(f"Connecting to the Supabase pooler at {DB_HOST}...")
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT)
        cur = conn.cursor()
        print("✅ Connection successful!")

        print(f"Reading data from {CSV_FILE_PATH}...")
        df = pd.read_csv(CSV_FILE_PATH)
        total_rows = len(df)
        print(f"Found {total_rows} rows to insert.")

        # --- SWITCHING TO ROW-BY-ROW INSERTION FOR ROBUSTNESS ---
        # This is safer than bulk-copying if the CSV has formatting errors.
        
        for index, row in df.iterrows():
            try:
                # Print progress to the console
                print(f"Inserting row {index + 1}/{total_rows}...", end='\r')
                
                # Define the SQL command for inserting a single row
                sql = "INSERT INTO genomes (genome_id, description, sequence) VALUES (%s, %s, %s)"
                
                # Execute the command with data from the current row
                cur.execute(sql, (row['genome_id'], row['description'], row['sequence']))

            except Exception as row_error:
                print(f"\n❌ Error on row {index + 1} (genome_id: {row.get('genome_id', 'N/A')}). Skipping.")
                print(f"   Details: {row_error}")
                conn.rollback() # Rollback the single failed insert

        # Commit the entire transaction after the loop is done
        conn.commit()
        
        print(f"\n🚀 Successfully finished inserting data into the 'genomes' table.")

    except FileNotFoundError:
        print(f"❌ Error: The file '{CSV_FILE_PATH}' was not found.")
        sys.exit(1)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"\n❌ A major database error occurred: {error}")
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    populate_genomes()