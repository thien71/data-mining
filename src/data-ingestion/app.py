import requests
import os
import mysql.connector
import datetime

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', '1234'),
    'database': os.getenv('DB_NAME', 'nuclear_outages')
}

API_KEY = 'p7CVKPTamXQJchOn1d3C6mksnpNvFommnZnLHwRx'

API_URLS = {
    'national_outages': 'https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/',
    'facility_outages': 'https://api.eia.gov/v2/nuclear-outages/facility-nuclear-outages/data/',
    'generator_outages': 'https://api.eia.gov/v2/nuclear-outages/generator-nuclear-outages/data/'
}

def create_url(api_url, start_date, end_date, offset=0, length=5000):
    return f"{api_url}?api_key={API_KEY}&frequency=daily&data[0]=capacity&data[1]=outage&data[2]=percentOutage&start={start_date}&end={end_date}&sort[0][column]=period&sort[0][direction]=desc&offset={offset}&length={length}"

def fetch_data_from_api(api_url, start_date, end_date):
    offset = 0
    length = 5000
    all_data = []
    
    while True:
        url = create_url(api_url, start_date, end_date, offset, length)
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if 'response' in data and 'data' in data['response']:
                page_data = data['response']['data']
                if page_data:
                    all_data.extend(page_data)  # Thêm dữ liệu vào danh sách
                if len(page_data) < length:
                    break  # Nếu dữ liệu ít hơn limit thì dừng lại
                else:
                    offset += length  # Lặp qua trang tiếp theo
            else:
                break
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
            break
    
    return all_data

def save_to_mysql(json_data, table_name):
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()

    # SQL query based on table name
    sql = get_insert_sql(table_name)

    row_count = 0
    if isinstance(json_data, list):
        data = json_data  # Nếu là list, không cần gọi .get() nữa
    else:
        data = json_data.get('response', {}).get('data', [])

    for entry in data:
        try:
            cursor.execute(sql, extract_values(entry, table_name))
            row_count += 1
            print(f"Inserted {row_count} row into {table_name} - Period: {entry['period']}")  # Log after each insert
        except Exception as e:
            print(f"Error inserting entry: {entry}, Error: {e}")
    
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Inserted {row_count} rows into {table_name}.")

def get_insert_sql(table_name):
    if table_name == 'national_outages':
        return """INSERT INTO national_outages (period, capacity, outage, percent_outage, capacity_units, outage_units, percent_outage_units)
                 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    elif table_name == 'facility_outages':
        return """INSERT INTO facility_outages (period, facility_id, facility_name, capacity, outage, percent_outage, capacity_units, outage_units, percent_outage_units)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    elif table_name == 'generator_outages':
        return """INSERT INTO generator_outages (period, facility_id, facility_name, generator_id, percent_outage, percent_outage_units)
                 VALUES (%s, %s, %s, %s, %s, %s)"""
    return None

def extract_values(entry, table_name):
    if table_name == 'national_outages':
        return (
            entry['period'],
            entry.get('capacity', None),
            entry.get('outage', None),
            entry.get('percentOutage', None),
            entry.get('capacity-units', None),
            entry.get('outage-units', None),
            entry.get('percentOutage-units', None)
        )
    elif table_name == 'facility_outages':
        return (
            entry['period'],
            entry.get('facility', None),  
            entry.get('facilityName', None),
            entry.get('capacity', None),
            entry.get('outage', None),
            entry.get('percentOutage', None),
            entry.get('capacity-units', None),
            entry.get('outage-units', None),
            entry.get('percentOutage-units', None)
        )
    elif table_name == 'generator_outages':
        return (
            entry['period'],
            entry.get('facility', None),  
            entry.get('facilityName', None),
            entry.get('generator', None),
            entry.get('percentOutage', None),
            entry.get('percentOutage-units', None)
        )
    return None

def process_data(api_url, start_date, end_date, table_name):
    data = fetch_data_from_api(api_url, start_date, end_date)
    if data:
        save_to_mysql(data, table_name)

def first_run():
    start_date = '2023-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    for api_name, api_url in API_URLS.items():
        print(f"Fetching data from {api_name}...")
        process_data(api_url, start_date, end_date, api_name)

def main():
    first_run()

if __name__ == "__main__":
    main()
