import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text

# config.py
database_config = {
    "username": "your_username",
    "password": "your_password",
    "host": "localhost",
    "database": "source_database"
}

# สร้าง connection string จาก config
connection_string = f"postgresql://{database_config['username']}:{database_config['password']}@{database_config['host']}/{database_config['database']}"

# สร้าง engine สำหรับการเชื่อมต่อกับฐานข้อมูล
source_db_engine = create_engine(connection_string)

# ตั้งค่า REST API ปลายทาง
destination_api = 'https://example.com/api/endpoint'

# ตั้งค่า logging
logging.basicConfig(filename='data_migration.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def get_last_offset(log_file='data_migration.log'):
    last_offset = 0
    try:
        with open(log_file, 'r') as file:
            lines = file.readlines()
            for line in reversed(lines):
                if 'Current offset:' in line:
                    last_offset = int(line.split(':')[-1].strip())
                    break
    except FileNotFoundError:
        pass
    return last_offset

def fetch_data(batch_size=10000, start_offset=0):
    try:
        with source_db_engine.connect() as connection:
            offset = start_offset
            while True:
                result = connection.execute(
                    text('SELECT * FROM your_table LIMIT :limit OFFSET :offset'),
                    {'limit': batch_size, 'offset': offset}
                )
                data = result.fetchall()
                if not data:
                    break
                
                logging.info(f'Current offset: {offset}')
                offset += batch_size
                yield data
    except Exception as e:
        print(f"Error fetching data: {e}")
        logging.error(f"Error fetching data: {e}")

def send_data_to_destination_api(data):
    try:
        response = requests.post(destination_api, json=data)
        return response.status_code == 201
    except requests.exceptions.RequestException as e:
        print(f"Error sending data: {e}")
        logging.error(f"Error sending data: {e}")
        return False

def process_batch(batch_data):
    return send_data_to_destination_api(batch_data)

batch_size = 10000
concurrent_workers = 10
start_offset = get_last_offset()

successful = 0
failed = 0

with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
    futures = [executor.submit(process_batch, batch_data) for batch_data in fetch_data(batch_size, start_offset)]
    for future in as_completed(futures):
        if future.result():
            successful += 1
        else:
            failed += 1

print(f'Data migration completed: {successful} successful, {failed} failed')
