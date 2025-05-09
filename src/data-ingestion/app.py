import requests
import os
import mysql.connector
import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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
                    all_data.extend(page_data) 
                if len(page_data) < length:
                    break 
                else:
                    offset += length  
            else:
                break
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
            break
    
    return all_data

def save_to_mysql(json_data, table_name):
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()

    sql = get_insert_sql(table_name)

    row_count = 0
    if isinstance(json_data, list):
        data = json_data 
    else:
        data = json_data.get('response', {}).get('data', [])

    for entry in data:
        try:
            cursor.execute(sql, extract_values(entry, table_name))
            row_count += 1
            print(f"Inserted {row_count} row into {table_name} - Period: {entry['period']}") 
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
    return None

def process_data(api_url, start_date, end_date, table_name):
    data = fetch_data_from_api(api_url, start_date, end_date)
    if data:
        save_to_mysql(data, table_name)

def get_last_period():
    # Get the latest period from the database to avoid fetching older data
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute("SELECT MAX(period) FROM national_outages")
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result[0] if result[0] else None


def first_run():
    start_date = '2020-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    print(end_date)

    for api_name, api_url in API_URLS.items():
        print(f"Fetching data from {api_name}...")
        process_data(api_url, start_date, end_date, api_name)

@app.post("/api/crawl")
async def crawl():
    try:
        first_run() 
        return {"message": "Data fetch completed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during data fetch: {str(e)}")
    
# def main():
#     # Run the initial data fetch
#     # first_run()

#     # Start the scheduler
#     scheduler = BackgroundScheduler()
#     scheduler.add_job(scheduled_task, 'interval', minutes=2)
#     scheduler.start()

#     try:
#         # Keep the main thread alive to allow scheduled tasks to run
#         while True:
#             pass
#     except (KeyboardInterrupt, SystemExit):
#         scheduler.shutdown()

# if __name__ == "__main__":
#     main()
