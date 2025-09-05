import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Global Connection Pool ---
connection_pool = None

def init_connection_pool():
    """Initializes the database connection pool using credentials from the .env file."""
    global connection_pool
    if connection_pool is None:
        try:
            print("Initializing database connection pool...")
            connection_pool = pool.SimpleConnectionPool(
                1,  # minconn: start with 1 connection
                5,  # maxconn: can grow to 5 connections
                host=os.getenv("DB_HOST"),
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                port=os.getenv("DB_PORT")
            )
            print("✅ Connection pool initialized successfully.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"❌ Error while initializing connection pool: {error}")
            raise error

def get_connection():
    """Gets a connection from the pool."""
    if connection_pool is None:
        raise Exception("Connection pool is not initialized. Call init_connection_pool() first.")
    return connection_pool.getconn()

def release_connection(conn):
    """Releases a connection back to the pool."""
    if connection_pool:
        connection_pool.putconn(conn)

def close_connection_pool():
    """Closes all connections in the pool at the end of the script."""
    if connection_pool:
        connection_pool.closeall()
        print("Connection pool closed.")