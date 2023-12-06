import requests
import schedule
import time
import rts_logger
from pymongo import MongoClient

def fetch_rts_data(url, max_retries=10, backoff_factor=2):
    retry_delay = 1  # Initial retry delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            transform_and_store(response.json())
            return response.json()
        except requests.RequestException as e:
            log = rts_logger.get_logger("fetch_rts_data")
            log.debug(f"Request failed: {e}, retrying in {retry_delay} seconds.")
            time.sleep(retry_delay)
            retry_delay *= backoff_factor  # Increase the delay for the next retry
            if attempt == max_retries - 1:
                raise  # Re-raise the last exception if all retries fail
#EtAg6QW5qYQbY5Z


def transform_and_store(solar_wind_data):
    field_names = solar_wind_data[0]
    field_names[0] = "time"
    transformed_data = [dict(zip(field_names, record)) for record in solar_wind_data[1:]]
    print(transformed_data)

    

    # Connect to MongoDB (adjust the connection string as needed)
    client = MongoClient('mongodb+srv://becnel:EtAg6QW5qYQbY5Z@solarweather.tvtmziw.mongodb.net/')
    db = client['SolarWeather']  # Database name
    collection = db['SolarWeatherData']  # Collection name



def acquire_rts_data():
    # Schedule the function to run every hour
    schedule.every(1).hours.do(fetch_rts_data)

    while True:
        schedule.run_pending()
        time.sleep(1)

