from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import mysql.connector
import requests
import pandas as pd
import numpy as np
from decimal import Decimal
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os
import datetime
import asyncio
from pydantic import BaseModel
from sklearn.linear_model import LinearRegression

app = FastAPI()

origins = [
    "http://localhost:5173",  
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

DB_CONFIG = {
    'host': 'mysql',
    'port': 3306,
    'user': 'root',
    'password': '1234',
    'database': 'nuclear_outages'
}

OUTAGE_FILE = "outage_clusters.csv"
SEASON_FILE = "season_counts.csv"
SCHEDULE_FILE = "schedule_time.txt"
HISTORY_FILE = 'history_clustering.txt'

def fetch_data_from_db(query: str):
    """Lấy dữ liệu từ DB"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    finally:
        connection.close()

def process_trend(data):
    """Tính toán và trả về trend từ dữ liệu"""
    df = pd.DataFrame(data)
    df['period'] = pd.to_datetime(df['period'])
    df.set_index('period', inplace=True)
    
    decomposition = seasonal_decompose(df['percent_outage'], model='multiplicative', period=12)
    trend = decomposition.trend.dropna()
    return trend

def process_seasonal(data):
    """Tính toán và trả về seasonal từ dữ liệu"""
    df = pd.DataFrame(data)
    df['period'] = pd.to_datetime(df['period'])
    df.set_index('period', inplace=True)
    
    decomposition = seasonal_decompose(df['percent_outage'], model='multiplicative', period=12)
    seasonal = decomposition.seasonal.dropna()
    return seasonal


API_KEY = 'p7CVKPTamXQJchOn1d3C6mksnpNvFommnZnLHwRx'

API_URLS = {
    'national_outages': 'https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/',
}

app = FastAPI()

# CORS setup (optional)
origins = [
    "http://localhost:5173",  
    "http://127.0.0.1:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/crawl")
async def crawl():
    try:
        start_date = '2020-01-01'  # Static start date (can be modified if needed)
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')

        all_data = []

        # Step 2: Fetch data from the API
        for api_name, api_url in API_URLS.items():
            print(f"Fetching data from {api_name}...")
            offset = 0
            length = 5000
            while True:
                url = f"{api_url}?api_key={API_KEY}&frequency=daily&data[0]=capacity&data[1]=outage&data[2]=percentOutage&start={start_date}&end={end_date}&sort[0][column]=period&sort[0][direction]=desc&offset={offset}&length={length}"
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
        
        # Step 3: Save fetched data to MySQL
        if all_data:
            connection = mysql.connector.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            # SQL insert statement
            insert_sql = """INSERT INTO national_outages (period, capacity, outage, percent_outage, capacity_units, outage_units, percent_outage_units)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            row_count = 0

            for entry in all_data:
                try:
                    cursor.execute(insert_sql, (
                        entry['period'],
                        entry.get('capacity', None),
                        entry.get('outage', None),
                        entry.get('percentOutage', None),
                        entry.get('capacity-units', None),
                        entry.get('outage-units', None),
                        entry.get('percentOutage-units', None)
                    ))
                    row_count += 1
                    print(f"Inserted {row_count} row into national_outages - Period: {entry['period']}")
                except Exception as e:
                    print(f"Error inserting entry: {entry}, Error: {e}")

            connection.commit()
            cursor.close()
            connection.close()
            print(f"Inserted {row_count} rows into national_outages.")

        return {"message": "Data fetch and save completed successfully."}

    except Exception as e:
        print(f"Error during data fetch: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error during data fetch: {str(e)}")
    

@app.get("/api/national_outages")
async def get_facility_outages():
    query = "SELECT * FROM national_outages"
    data = fetch_data_from_db(query)
    
    return data

@app.get("/api/time_series/day")
async def get_time_series_day(year: int, month: int):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        # Query to get the daily percent outage for the given month and year
        query = """
            SELECT DATE_FORMAT(period, '%Y-%m-%d') AS day, 
                   percent_outage
            FROM national_outages
            WHERE YEAR(period) = %s AND MONTH(period) = %s
            ORDER BY period
        """
        cursor.execute(query, (year, month))
        result = cursor.fetchall()

        print(f"Fetched {len(result)} records.")
        return result
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/time_series/month")
async def get_time_series_month(year: int):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        query = """
            SELECT MONTH(period) AS month, 
                   AVG(percent_outage) AS avg_percent_outage
            FROM national_outages
            WHERE YEAR(period) = %s
            GROUP BY MONTH(period)
            ORDER BY MONTH(period)
        """
        cursor.execute(query, (year,))
        result = cursor.fetchall()

        print(f"Fetched {len(result)} records.")
        return result
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/trending")
async def get_trend():
    """API trả về dữ liệu trend"""
    query = "SELECT period, percent_outage FROM national_outages ORDER BY period"
    data = fetch_data_from_db(query)
    trend = process_trend(data)
    
    trend_data = [{"date": str(date), "value": value} for date, value in trend.items()]
    
    return {"trend": trend_data}

@app.get("/api/seasonal")
async def get_seasonal():
    """API trả về dữ liệu seasonal"""
    query = "SELECT period, percent_outage FROM national_outages ORDER BY period"
    data = fetch_data_from_db(query)
    seasonal = process_seasonal(data)
    
    seasonal_data = [{"date": str(date), "value": value} for date, value in seasonal.items()]
    
    return {"seasonal": seasonal_data}

@app.get("/api/correlation_matrix")
async def get_correlation_matrix():
    query = """
        SELECT period, capacity, outage, percent_outage
        FROM national_outages
    """
    data = fetch_data_from_db(query)
    
    if not data:
        return JSONResponse(status_code=404, content={"message": "No data found"})

    df = pd.DataFrame(data)
    df['percent_outage'] = (df['outage'] / df['capacity']) * 100
    correlation_matrix = df[['capacity', 'outage', 'percent_outage']].corr()
    correlation_matrix_dict = correlation_matrix.to_dict()

    return JSONResponse(content=correlation_matrix_dict)

# ----------------------------------------------------------------

def append_to_history(message):
    try:
        with open(HISTORY_FILE, 'a') as file:
            file.write(f"{datetime.datetime.now()} - {message}\n")
    except Exception as e:
        print(f"Error appending to history file: {e}")

def get_schedule_time():
    try:
        with open(SCHEDULE_FILE, "r") as file:
            time_str = file.read().strip()
            hour, minute = map(int, time_str.split(":"))
            message = f"Schedule time read from file: {hour:02d}:{minute:02d}"
            print(message)
            append_to_history(message)
            return datetime.time(hour=hour, minute=minute)
    except Exception as e:
        error_message = f"Error reading schedule time: {e}"
        print(error_message)
        return datetime.time(hour=22, minute=15)

# async def daily_scheduler():
#     while True:
#         now = datetime.datetime.now()
#         scheduled_time = get_schedule_time()
#         target_time = datetime.datetime.combine(now.date(), scheduled_time)

#         if now > target_time:
#             target_time += datetime.timedelta(days=1)

#         sleep_duration = (target_time - now).total_seconds()
#         print(f"Next clustering scheduled at: {target_time}")
        
#         await asyncio.sleep(sleep_duration)
        
#         print("Running K-Means clustering...") 
#         run_kmeans_clustering()


def run_kmeans_clustering():
    query = "SELECT period, percent_outage FROM national_outages ORDER BY period"
    data = fetch_data_from_db(query)

    df = pd.DataFrame(data)

    X = df[["percent_outage"]]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=4, random_state=0)
    df["cluster"] = kmeans.fit_predict(X_scaled)

    try:
        df.to_csv(OUTAGE_FILE, index=False)
    except Exception as e:
        print(f"Error saving to CSV: {e}")

    generate_season_data(df)
    print(f"[{datetime.datetime.now()}] Clustering completed.")


def generate_season_data(df):
    df['period'] = pd.to_datetime(df['period']).dt.strftime('%Y-%m-%d')
    df['year'] = pd.to_datetime(df['period']).dt.year
    df['season'] = df['cluster'].map({
        0: 'Low', 
        1: 'High', 
        2: 'Very low', 
        3: 'Medium'
    })

    season_counts = df.groupby(['year', 'season']).size().unstack(fill_value=0)
    season_counts = season_counts.reset_index()
    season_counts.to_csv(SEASON_FILE, index=False)


@app.get("/api/clusters/chart")
async def get_cluster_chart():
    """API trả về dữ liệu để vẽ biểu đồ phân cụm."""
    if not os.path.exists(OUTAGE_FILE):
        return JSONResponse(status_code=404, content={"message": "No clustering data found."})

    df = pd.read_csv(OUTAGE_FILE)
    chart_data = df[["period", "percent_outage", "cluster"]].to_dict(orient="records")
    return JSONResponse(content=chart_data)


@app.get("/api/seasons/chart")
async def get_season_chart():
    """API trả về dữ liệu để vẽ biểu đồ số ngày theo mùa."""
    if not os.path.exists(SEASON_FILE):
        return JSONResponse(status_code=404, content={"message": "No season data found."})

    df = pd.read_csv(SEASON_FILE)
    return JSONResponse(content=df.to_dict(orient="records"))

class PredictionResult(BaseModel):
    predicted_date: str
    predicted_percent_outage: float
    df_result: dict
model = LinearRegression()
@app.get("/api/predict-outage", response_model=PredictionResult)
def predict_outage():
    # Fetch data
    query = "SELECT * FROM national_outages ORDER BY period"
    data = fetch_data_from_db(query)
    for row in data:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)
    df = pd.DataFrame(data)

    # Process data
    df['date'] = pd.to_datetime(df['period'])
    df = df.sort_values('date')
    df['avg_outage_7d'] = df['outage'].rolling(window=7).mean()
    df['avg_percent_outage_7d'] = df['percent_outage'].rolling(window=7).mean()
    df = df.dropna()

    # Train model
    X = df[['avg_outage_7d', 'avg_percent_outage_7d']].values
    y = df['percent_outage'].values
    model.fit(X, y)

    # Predict next day
    last_row = df.iloc[-1]
    next_day_features = np.array([[last_row['avg_outage_7d'], last_row['avg_percent_outage_7d']]])
    predicted_percent_outage = model.predict(next_day_features)[0]
    predicted_date = last_row['date'] + pd.Timedelta(days=1)

    # Prepare result DataFrame
    df_result = pd.DataFrame({
        'date': df['date'].dt.strftime('%Y-%m-%d').tolist() + [predicted_date.strftime('%Y-%m-%d')],
        'percent_outage': df['percent_outage'].round(2).tolist() + [round(predicted_percent_outage, 2)],
    })

    # Serialize DataFrame to list of dicts
    result_data = {
        "predicted_date": predicted_date.strftime('%Y-%m-%d'),
        "predicted_percent_outage": round(float(predicted_percent_outage), 2),
        "df_result": df_result.to_dict(orient="records"),
    }

    # Return response
    return JSONResponse(content=result_data)