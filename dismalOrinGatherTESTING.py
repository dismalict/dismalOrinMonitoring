import mysql.connector
from mysql.connector import Error
from jtop import jtop, JtopException
import socket
from configparser import ConfigParser
from datetime import datetime
import psutil
import subprocess
import re
import logging
import time

# Set up logging configuration
logging.basicConfig(
    filename='dismalOrinGather.log',  # Log file name
    level=logging.INFO,  # Log level
    format='%(asctime)s %(levelname)s: %(message)s',  # Format includes timestamp
    datefmt='%Y-%m-%d %H:%M:%S'  # Timestamp format
)

def run_command(command):
    """Utility function to run a shell command and return its output."""
    try:
        output = subprocess.check_output(command, shell=True, text=True).strip()
    except subprocess.CalledProcessError:
        output = None
    return output

def remove_ansi_escape_sequences(text):
    """Remove ANSI escape sequences from text."""
    ansi_escape = re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)

def parse_jetson_release(output):
    """Parse the output of jetson_release and return relevant information."""
    info = {}
    for line in output.split('\n'):
        line = remove_ansi_escape_sequences(line)  # Clean up ANSI escape sequences
        if 'Model:' in line:
            info['model'] = line.split(':', 1)[1].strip()
        elif 'Jetpack' in line:
            info['jetpack'] = line.split('[', 1)[1].split(']')[0].strip()
        elif 'L4T' in line:
            info['l4t'] = line.split('L4T ', 1)[1].strip()
        elif 'NV Power Mode' in line:
            info['nv_power_mode'] = line.split(':', 1)[1].strip()
        elif 'Serial Number' in line:
            info['serial_number'] = line.split(':', 1)[1].strip()
        elif 'P-Number' in line:
            info['p_number'] = line.split(':', 1)[1].strip()
        elif 'Module' in line:
            info['module'] = line.split(':', 1)[1].strip()
        elif 'Distribution' in line:
            info['distribution'] = line.split(':', 1)[1].strip()
        elif 'Release' in line:
            info['release'] = line.split(':', 1)[1].strip()
        elif 'CUDA' in line:
            info['cuda'] = line.split(':', 1)[1].strip()
        elif 'cuDNN' in line:
            info['cudnn'] = line.split(':', 1)[1].strip()
        elif 'TensorRT' in line:
            info['tensorrt'] = line.split(':', 1)[1].strip()
        elif 'VPI' in line:
            info['vpi'] = line.split(':', 1)[1].strip()
        elif 'Vulkan' in line:
            info['vulkan'] = line.split(':', 1)[1].strip()
        elif 'OpenCV' in line:
            info['opencv'] = line.split(':', 1)[1].strip()
    return info

def gather_device_info():
    """Gather detailed device information."""
    jetson_release_output = run_command('jetson_release -s')
    jetson_info = parse_jetson_release(jetson_release_output)
    
    return {
        'hostname': socket.gethostname(),
        'ip_address': socket.gethostbyname(socket.gethostname()),
        **jetson_info
    }

def read_db_config(filename='backendItems/config.ini', section='database'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(f'Section {section} not found in {filename}')
    return db

def create_connection():
    db_config = read_db_config()
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            logging.info("Connected to MySQL database")
            return connection
    except Error as e:
        logging.error(f"MySQL Error: {e}")
        return None

def create_table(cursor, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        `time` DATETIME,
        `uptime` VARCHAR(50),
        `CPU1` INT,
        `CPU2` INT,
        `CPU3` INT,
        `CPU4` INT,
        `CPU5` INT,
        `CPU6` INT,
        `RAM` FLOAT,
        `SWAP` INT,
        `EMC` INT,
        `GPU` INT,
        `APE` VARCHAR(10),
        `NVDEC` VARCHAR(10),
        `NVJPG` VARCHAR(10),
        `NVJPG1` VARCHAR(10),
        `OFA` VARCHAR(10),
        `SE` VARCHAR(10),
        `VIC` VARCHAR(10),
        `Fan pwmfan0` FLOAT,
        `Temp CPU` FLOAT,
        `Temp CV0` FLOAT,
        `Temp CV1` FLOAT,
        `Temp CV2` FLOAT,
        `Temp GPU` FLOAT,
        `Temp SOC0` FLOAT,
        `Temp SOC1` FLOAT,
        `Temp SOC2` FLOAT,
        `Temp tj` FLOAT,
        `Power CPU` INT,
        `Power CV` INT,
        `Power GPU` INT,
        `Power SOC` INT,
        `Power SYS5v` INT,
        `Power VDDRQ` INT,
        `Power tj` INT,
        `Power TOT` INT,
        `jetson_clocks` VARCHAR(10),
        `nvp model` VARCHAR(50),
        `disk_available_gb` FLOAT,
        `hostname` VARCHAR(255),
        `ip_address` VARCHAR(50),
        `model` TEXT,
        `jetpack` TEXT,
        `l4t` TEXT,
        `nv_power_mode` TEXT,
        `serial_number` TEXT,
        `p_number` TEXT,
        `module` TEXT,
        `distribution` TEXT,
        `release` TEXT,
        `cuda` TEXT,
        `cudnn` TEXT,
        `tensorrt` TEXT,
        `vpi` TEXT,
        `vulkan` TEXT,
        `opencv` TEXT
    )
    """
    cursor.execute(create_table_query)
    logging.info(f"Table `{table_name}` is ready.")

def create_long_term_storage_table(cursor, hostname):
    storage_table_name = f"{hostname}_storage"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{storage_table_name}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        `time` DATETIME,
        `uptime` VARCHAR(50),
        `CPU1` INT,
        `CPU2` INT,
        `CPU3` INT,
        `CPU4` INT,
        `CPU5` INT,
        `CPU6` INT,
        `RAM` FLOAT,
        `SWAP` INT,
        `EMC` INT,
        `GPU` INT,
        `APE` VARCHAR(10),
        `NVDEC` VARCHAR(10),
        `NVJPG` VARCHAR(10),
        `NVJPG1` VARCHAR(10),
        `OFA` VARCHAR(10),
        `SE` VARCHAR(10),
        `VIC` VARCHAR(10),
        `Fan pwmfan0` FLOAT,
        `Temp CPU` FLOAT,
        `Temp CV0` FLOAT,
        `Temp CV1` FLOAT,
        `Temp CV2` FLOAT,
        `Temp GPU` FLOAT,
        `Temp SOC0` FLOAT,
        `Temp SOC1` FLOAT,
        `Temp SOC2` FLOAT,
        `Temp tj` FLOAT,
        `Power CPU` INT,
        `Power CV` INT,
        `Power GPU` INT,
        `Power SOC` INT,
        `Power SYS5v` INT,
        `Power VDDRQ` INT,
        `Power tj` INT,
        `Power TOT` INT,
        `jetson_clocks` VARCHAR(10),
        `nvp model` VARCHAR(50),
        `disk_available_gb` FLOAT,
        `hostname` VARCHAR(255),
        `ip_address` VARCHAR(50),
        `model` TEXT,
        `jetpack` TEXT,
        `l4t` TEXT,
        `nv_power_mode` TEXT,
        `serial_number` TEXT,
        `p_number` TEXT,
        `module` TEXT,
        `distribution` TEXT,
        `release` TEXT,
        `cuda` TEXT,
        `cudnn` TEXT,
        `tensorrt` TEXT,
        `vpi` TEXT,
        `vulkan` TEXT,
        `opencv` TEXT
    )
    """
    cursor.execute(create_table_query)
    logging.info(f"Long-term storage table `{storage_table_name}` is ready.")

def get_disk_space_gb():
    """Returns the available disk space in GB."""
    disk_usage = psutil.disk_usage('/')
    return disk_usage.free / (1024 ** 3)

def insert_data(cursor, table_name, data):
    columns = ", ".join([f"`{key}`" for key in data.keys()])
    placeholders = ", ".join(["%s"] * len(data))
    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
    cursor.execute(insert_query, tuple(data.values()))

def insert_data_with_retry(connection, table_name, data, retries=5, delay=2):
    cursor = connection.cursor()
    for attempt in range(retries):
        try:
            insert_data(cursor, table_name, data)
            connection.commit()
            logging.info(f"Data successfully inserted into `{table_name}` on attempt {attempt + 1}.")
            return
        except mysql.connector.Error as err:
            logging.error(f"Attempt {attempt + 1} failed with error: {err}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    logging.error(f"All {retries} attempts failed. Data not inserted into `{table_name}`.")
    cursor.close()

def cleanup_old_data(cursor, table_name, max_rows=50):
    """Keep only the most recent `max_rows` entries in the specified table."""
    delete_query = f"""
    DELETE FROM `{table_name}`
    WHERE id NOT IN (
        SELECT id FROM (
            SELECT id FROM `{table_name}`
            ORDER BY id DESC
            LIMIT {max_rows}
        ) AS temp
    )
    """
    cursor.execute(delete_query)

def main():
    connection = create_connection()
    if connection is None:
        return

    cursor = connection.cursor()
    
    hostname = socket.gethostname()
    table_name = f"{hostname}"
    
    create_table(cursor, table_name)
    create_long_term_storage_table(cursor, hostname)

    logging.info("Simple jtop logger")
    while True:
        try:
            stats = jtop().get_stats()  # gather stats
            data = {
                'time': datetime.now(),
                'uptime': stats['uptime'],
                'CPU1': stats['CPU'][0],
                'CPU2': stats['CPU'][1],
                'CPU3': stats['CPU'][2],
                'CPU4': stats['CPU'][3],
                'CPU5': stats['CPU'][4],
                'CPU6': stats['CPU'][5],
                'RAM': stats['RAM']['used'],
                'SWAP': stats['SWAP']['used'],
                'EMC': stats['EMC'],
                'GPU': stats['GPU'],
                'APE': stats['APE'],
                'NVDEC': stats['NVDEC'],
                'NVJPG': stats['NVJPG'],
                'NVJPG1': stats['NVJPG1'],
                'OFA': stats['OFA'],
                'SE': stats['SE'],
                'VIC': stats['VIC'],
                'Fan pwmfan0': stats['Fan pwmfan0'],
                'Temp CPU': stats['Temp CPU'],
                'Temp CV0': stats['Temp CV0'],
                'Temp CV1': stats['Temp CV1'],
                'Temp CV2': stats['Temp CV2'],
                'Temp GPU': stats['Temp GPU'],
                'Temp SOC0': stats['Temp SOC0'],
                'Temp SOC1': stats['Temp SOC1'],
                'Temp SOC2': stats['Temp SOC2'],
                'Temp tj': stats['Temp tj'],
                'Power CPU': stats['Power CPU'],
                'Power CV': stats['Power CV'],
                'Power GPU': stats['Power GPU'],
                'Power SOC': stats['Power SOC'],
                'Power SYS5v': stats['Power SYS5v'],
                'Power VDDRQ': stats['Power VDDRQ'],
                'Power tj': stats['Power tj'],
                'Power TOT': stats['Power TOT'],
                'jetson_clocks': stats['jetson_clocks'],
                'nvp model': stats['nvp model'],
                'disk_available_gb': get_disk_space_gb(),
                'hostname': socket.gethostname(),
                'ip_address': socket.gethostbyname(socket.gethostname()),
                **gather_device_info()  # Gather additional device info
            }

            # Insert data into the current table
            insert_data_with_retry(connection, table_name, data)

            # Cleanup old data in the current table
            cleanup_old_data(cursor, table_name)

            time.sleep(2)

        except JtopException as e:
            logging.error(f"Jtop Exception: {e}")
            time.sleep(5)  # Wait before retrying if jtop fails
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    cursor.close()
    connection.close()

if __name__ == '__main__':
    main()
