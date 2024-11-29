from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import mysql.connector
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os
import datetime
import asyncio

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

@app.get("/api/national_outages")
async def get_national_outages():
    query = "SELECT * FROM national_outages"
    data = fetch_data_from_db(query)
    
    return data

@app.get("/api/facility_outages")
async def get_facility_outages():
    query = "SELECT * FROM facility_outages"
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
    # query = """
    #     SELECT period, facility_id, facility_name, capacity, outage, percent_outage
    #     FROM facility_outages
    # """
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

async def daily_scheduler():
    while True:
        now = datetime.datetime.now()
        scheduled_time = get_schedule_time()
        target_time = datetime.datetime.combine(now.date(), scheduled_time)

        if now > target_time:
            target_time += datetime.timedelta(days=1)

        sleep_duration = (target_time - now).total_seconds()
        print(f"Next clustering scheduled at: {target_time}")
        
        await asyncio.sleep(sleep_duration)
        
        print("Running K-Means clustering...") 
        run_kmeans_clustering()

@app.on_event("startup")
async def startup_event():
    print("Running K-Means clustering immediately...")
    run_kmeans_clustering()

    asyncio.create_task(daily_scheduler())
    print("Starting API server...")


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