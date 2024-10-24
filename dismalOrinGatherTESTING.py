import mysql.connector
from mysql.connector import Error
from jtop import jtop, JtopException
import socket
from configparser import ConfigParser
from datetime import datetime
import psutil
import subprocess
import re
import time
import logging
from typing import Dict, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jetson_monitor.log'),
        logging.StreamHandler()
    ]
)

def run_command(command: str) -> str:
    """Utility function to run a shell command and return its output."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            output = subprocess.check_output(command, shell=True, text=True).strip()
            return output
        except subprocess.CalledProcessError as e:
            logging.warning(f"Command failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retrying
    return ""

def create_connection(max_retries: int = -1) -> mysql.connector.MySQLConnection:
    """Create database connection with retry mechanism."""
    retry_count = 0
    while max_retries == -1 or retry_count < max_retries:
        try:
            db_config = read_db_config()
            connection = mysql.connector.connect(**db_config)
            if connection.is_connected():
                logging.info("Connected to MySQL database")
                return connection
        except Error as e:
            retry_count += 1
            logging.error(f"MySQL Connection Error (attempt {retry_count}): {e}")
            time.sleep(5)  # Wait 5 seconds before retrying
    return None

def safe_insert_data(cursor, table_name: str, data: Dict[str, Any]) -> bool:
    """Safely insert data into the specified table with error handling."""
    try:
        columns = ", ".join([f"`{key}`" for key in data.keys()])
        placeholders = ", ".join(["%s"] * len(data))
        insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, list(data.values()))
        return True
    except Error as e:
        logging.error(f"Failed to insert data into {table_name}: {e}")
        return False

def safe_trim_table(cursor, table_name: str, row_limit: int = 50) -> bool:
    """Safely trim the table with error handling."""
    try:
        trim_query = f"""
        DELETE FROM `{table_name}`
        WHERE id NOT IN (
            SELECT id FROM (
                SELECT id FROM `{table_name}`
                ORDER BY `time` DESC
                LIMIT {row_limit}
            ) temp_table
        );
        """
        cursor.execute(trim_query)
        return True
    except Error as e:
        logging.error(f"Failed to trim table {table_name}: {e}")
        return False

def main():
    hostname = socket.gethostname()
    storage_table_name = f"{hostname}_storage"
    
    while True:  # Main program loop
        try:
            connection = create_connection()
            if not connection:
                logging.error("Failed to establish database connection. Retrying in 30 seconds...")
                time.sleep(30)
                continue

            cursor = connection.cursor()
            create_table(cursor, hostname, storage_table_name)
            device_info = gather_device_info()

            with jtop() as jetson:
                while jetson.ok():
                    try:
                        # Ensure connection is still alive
                        if not connection.is_connected():
                            raise Error("Database connection lost")

                        stats = jetson.stats
                        disk_space_gb = get_disk_space_gb()

                        data = {
                            'time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                            'uptime': stats.get('uptime'),
                            # ... (rest of your data dictionary remains the same)
                            'opencv': device_info.get('opencv')
                        }

                        # Insert into current table
                        if safe_insert_data(cursor, hostname, data):
                            safe_trim_table(cursor, hostname, row_limit=50)
                        
                        # Insert into storage table
                        safe_insert_data(cursor, storage_table_name, data)

                        connection.commit()
                        time.sleep(1)  # Prevent too frequent updates

                    except Error as e:
                        logging.error(f"Database error during operation: {e}")
                        # Attempt to reconnect
                        connection = create_connection()
                        if connection:
                            cursor = connection.cursor()
                    except Exception as e:
                        logging.error(f"Unexpected error during operation: {e}")
                        time.sleep(5)  # Wait before continuing

        except JtopException as e:
            logging.error(f"JTOP error: {e}")
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(5)  # Wait before retrying
        finally:
            try:
                if connection and connection.is_connected():
                    cursor.close()
                    connection.close()
                    logging.info("MySQL connection closed")
            except Exception as e:
                logging.error(f"Error while closing connection: {e}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Program terminated by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")