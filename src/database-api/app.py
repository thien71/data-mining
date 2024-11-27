from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import mysql.connector

app = FastAPI()

# Cấu hình CORS
origins = [
    "http://localhost:5173",  # Địa chỉ frontend của bạn
    "http://127.0.0.1:5173",  # Địa chỉ loopback frontend
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Cho phép các nguồn gốc (frontend)
    allow_credentials=True,
    allow_methods=["*"],  # Cho phép tất cả phương thức (GET, POST,...)
    allow_headers=["*"],  # Cho phép tất cả header
)

DB_CONFIG = {
    'host': 'mysql',
    'port': 3306,
    'user': 'root',
    'password': '1234',
    'database': 'nuclear_outages'
}

@app.get("/api/national_outages")
async def get_national_outages():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM national_outages")
        results = cursor.fetchall()
        print(f"Fetched {len(results)} records from the database.")
        return results
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/facility_outages")
async def get_facility_outages():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM facility_outages")
        results = cursor.fetchall()
        print(f"Fetched {len(results)} records from the database.")
        return results
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/generator_outages")
async def get_generator_outages():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM generator_outages")
        results = cursor.fetchall()
        print(f"Fetched {len(results)} records from the database.")
        return results
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/time_series")
async def get_time_series():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        # Query to get the average percent outage by month from national_outages
        query = """
            SELECT DATE_FORMAT(period, '%Y-%m') AS month, 
                   AVG(percent_outage) AS avg_percent_outage
            FROM national_outages
            GROUP BY month
        """
        cursor.execute(query)
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
async def get_trending():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        # Query to get the average percent outage for each month across all years
        query = """
            SELECT MONTH(period) AS month, 
                   AVG(percent_outage) AS avg_percent_outage
            FROM national_outages
            GROUP BY month
            ORDER BY month
        """
        cursor.execute(query)
        result = cursor.fetchall()

        trending_data = [{"month": i, "avg_percent_outage": 0.0} for i in range(1, 13)]
        
        for row in result:
            month_index = row['month'] - 1 
            trending_data[month_index]["avg_percent_outage"] = row['avg_percent_outage']
        
        print(f"Fetched {len(result)} records.")
        return trending_data

    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/seasonal")
async def get_seasonal():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        # Query to get the average percent outage for each season
        query = """
            SELECT 
                CASE 
                    WHEN MONTH(period) BETWEEN 3 AND 5 THEN 'Spring'
                    WHEN MONTH(period) BETWEEN 6 AND 8 THEN 'Summer'
                    WHEN MONTH(period) BETWEEN 9 AND 11 THEN 'Fall'
                    ELSE 'Winter'  -- Includes December, January, February
                END AS season,
                AVG(percent_outage) AS avg_percent_outage
            FROM national_outages
            GROUP BY season
            ORDER BY FIELD(season, 'Spring', 'Summer', 'Fall', 'Winter')
        """
        cursor.execute(query)
        result = cursor.fetchall()

        # Prepare the data to return in a consistent format
        seasonal_data = {
            "Spring": 0.0,
            "Summer": 0.0,
            "Fall": 0.0,
            "Winter": 0.0
        }

        for row in result:
            seasonal_data[row['season']] = row['avg_percent_outage']

        print(f"Fetched {len(result)} records.")
        return seasonal_data

    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()


@app.get("/api/correlation_matrix")
async def get_correlation_matrix():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        # Query to get data from all three tables for correlation analysis
        query = """
            SELECT
                n.percent_outage AS national_percent_outage,
                f.percent_outage AS facility_percent_outage,
                g.percent_outage AS generator_percent_outage,
                n.capacity AS national_capacity,
                f.capacity AS facility_capacity,
                n.outage AS national_outage,
                f.outage AS facility_outage,
                g.percent_outage AS generator_percent_outage
            FROM
                national_outages n
                JOIN facility_outages f ON MONTH(n.period) = MONTH(f.period) AND YEAR(n.period) = YEAR(f.period)
                LEFT JOIN generator_outages g ON MONTH(n.period) = MONTH(g.period) AND YEAR(n.period) = YEAR(g.period)
        """
        cursor.execute(query)
        result = cursor.fetchall()

        # Convert result into a pandas DataFrame for correlation calculation
        df = pd.DataFrame(result)

        # Calculate correlation matrix
        correlation_matrix = df.corr()

        # Convert the correlation matrix to a dictionary or JSON-friendly format
        correlation_matrix_dict = correlation_matrix.to_dict()

        print(f"Fetched {len(result)} records.")
        return correlation_matrix_dict

    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/spider")
async def get_spider():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)

        # Query to get average outage percent by season and year
        query = """
            SELECT
                YEAR(period) AS year,
                CASE
                    WHEN MONTH(period) BETWEEN 3 AND 5 THEN 'Spring'
                    WHEN MONTH(period) BETWEEN 6 AND 8 THEN 'Summer'
                    WHEN MONTH(period) BETWEEN 9 AND 11 THEN 'Fall'
                    WHEN MONTH(period) = 12 OR MONTH(period) BETWEEN 1 AND 2 THEN 'Winter'
                END AS season,
                AVG(percent_outage) AS avg_percent_outage
            FROM national_outages
            WHERE YEAR(period) BETWEEN 2020 AND 2024
            GROUP BY YEAR(period), season
            ORDER BY year, FIELD(season, 'Winter', 'Spring', 'Summer', 'Fall');
        """
        cursor.execute(query)
        result = cursor.fetchall()

        # Process data into a format suitable for Spider Chart
        data_by_year = {}
        for row in result:
            year = row['year']
            season = row['season']
            avg_percent_outage = row['avg_percent_outage']

            if year not in data_by_year:
                data_by_year[year] = {'Spring': 0, 'Summer': 0, 'Fall': 0, 'Winter': 0}
            
            data_by_year[year][season] = avg_percent_outage

        print(f"Fetched {len(result)} records.")
        return data_by_year

    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()

@app.get("/api/prediction")
async def get_prediction():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)


    except Exception as e:
        print(f"Error fetching data: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        connection.close()
