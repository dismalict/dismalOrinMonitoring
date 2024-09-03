from jtop import jtop, JtopException
import mysql.connector
import configparser
from mysql.connector import Error
import socket
from datetime import datetime

def main():
    config = configparser.ConfigParser()
    config.read('config.ini')

    db_config = {
        'user': config.get('database', 'username'),
        'password': config.get('database', 'password'),
        'host': config.get('database', 'host'),
        'database': config.get('database', 'database'),
        'port': config.getint('database', 'port')
    }

    hostname = socket.gethostname()
    try:
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            print("Connected to MySQL database")

            cursor = connection.cursor()

            table_name = f"`{hostname}`"
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                time DATETIME,
                uptime VARCHAR(50),
                CPU1 INT,
                CPU2 INT,
                CPU3 INT,
                CPU4 INT,
                CPU5 INT,
                CPU6 INT,
                RAM FLOAT,
                SWAP INT,
                EMC INT,
                GPU INT,
                APE VARCHAR(10),
                NVDEC VARCHAR(10),
                NVJPG VARCHAR(10),
                NVJPG1 VARCHAR(10),
                OFA VARCHAR(10),
                SE VARCHAR(10),
                VIC INT,
                Fan_pwnfan0 FLOAT,
                Temp_CPU FLOAT,
                Temp_CV0 FLOAT,
                Temp_CV1 FLOAT,
                Temp_CV2 FLOAT,
                Temp_GPU FLOAT,
                Temp_SOC0 FLOAT,
                Temp_SOC1 FLOAT,
                Temp_SOC2 FLOAT,
                Temp_tj FLOAT,
                Power_tj INT,
                Power_TOT INT,
                jetson_clocks VARCHAR(10),
                nvp_model VARCHAR(50)
            )
            """
            cursor.execute(create_table_query)
            print(f"Table `{hostname}` is ready.")

            print("Simple jtop logger")
            print("Logging data to MySQL database")

            with jtop() as jetson:
                while jetson.ok():
                    stats = jetson.stats
                    stats['time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Formatting time for MySQL
                    data_tuple = (
                        stats['time'], stats['uptime'], stats['CPU1'], stats['CPU2'], stats['CPU3'],
                        stats['CPU4'], stats['CPU5'], stats['CPU6'], stats['RAM'], stats['SWAP'],
                        stats['EMC'], stats['GPU'], stats['APE'], stats['NVDEC'], stats['NVJPG'],
                        stats['NVJPG1'], stats['OFA'], stats['SE'], stats['VIC'], stats['Fan pwmfan0'],
                        stats['Temp CPU'], stats['Temp CV0'], stats['Temp CV1'], stats['Temp CV2'],
                        stats['Temp GPU'], stats['Temp SOC0'], stats['Temp SOC1'], stats['Temp SOC2'],
                        stats['Temp tj'], stats['Power tj'], stats['Power TOT'],
                        stats['jetson_clocks'], stats['nvp model']
                    )
                    insert_query = f"""
                    INSERT INTO {table_name} (
                        time, uptime, CPU1, CPU2, CPU3, CPU4, CPU5, CPU6, RAM, SWAP,
                        EMC, GPU, APE, NVDEC, NVJPG, NVJPG1, OFA, SE, VIC, Fan_pwnfan0,
                        Temp_CPU, Temp_CV0, Temp_CV1, Temp_CV2, Temp_GPU, Temp_SOC0,
                        Temp_SOC1, Temp_SOC2, Temp_tj, Power_tj, Power_TOT,
                        jetson_clocks, nvp_model
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, data_tuple)
                    connection.commit()
                    print(f"Logged data at {stats['time']}")
                    
    except Error as e:
        print(f"MySQL Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

if __name__ == "__main__":
    main()
