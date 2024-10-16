import mysql.connector
from configparser import ConfigParser
from datetime import datetime

# Function to read the database configuration
def read_db_config(filename='config2.ini', section='database'):
    # Create a parser
    parser = ConfigParser()
    # Read the config file
    parser.read(filename)

    # Get section, default to database
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(f'{section} not found in the {filename} file')

    return db

# Function to delete rows from the specified table
def delete_rows_from_table(conn, table, condition, limit=20000):
    cursor = conn.cursor()

    try:
        # Define the DELETE query
        delete_query = f"DELETE FROM {table} WHERE {condition} LIMIT {limit}"

        while True:
            # Execute the DELETE query
            cursor.execute(delete_query)
            deleted_count = cursor.rowcount  # Get the number of rows deleted
            conn.commit()  # Commit the transaction

            if deleted_count == 0:  # No more rows to delete
                print(f"Finished deleting rows from table: {table}")
                break

            print(f"Deleted {deleted_count} rows from table: {table}")

    except mysql.connector.Error as error:
        print(f"Failed to delete rows from table {table}: {error}")
    finally:
        cursor.close()

# Function to handle deletion across multiple tables
def delete_rows_from_multiple_tables(tables_with_conditions):
    # Read the database configuration
    db_config = read_db_config()

    # Establish the database connection
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['username'],
        password=db_config['password'],
        database=db_config['database'],
        port=db_config['port']
    )

    try:
        # Loop through each table and its corresponding condition
        for table, condition in tables_with_conditions.items():
            print(f"Starting deletion from table: {table}")
            delete_rows_from_table(conn, table, condition, limit=20000)
            print(f"Completed deletion from table: {table}")

    except mysql.connector.Error as error:
        print(f"Error during deletion process: {error}")
    finally:
        # Close the connection
        if conn.is_connected():
            conn.close()
            print("MySQL connection closed")

# Main function to define the tables and conditions
if __name__ == "__main__":
    # Get today's date and time in MySQL format
    today_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Define the tables and conditions (where timestamp is older than today)
    tables_with_conditions = {
        "sfvis01": f"time < '{today_timestamp}'",  
        "sfvis02": f"time < '{today_timestamp}'"   
    }

    # Call the function to delete rows from multiple tables
    delete_rows_from_multiple_tables(tables_with_conditions)
