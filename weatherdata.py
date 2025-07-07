import pandas as pd  # Import pandas for data manipulation
import requests  # Import requests to make HTTP requests to the weather API
from datetime import datetime  # Import datetime to parse and format dates
from concurrent.futures import ThreadPoolExecutor, as_completed  # For running tasks in parallel threads
from tqdm import tqdm  # For displaying progress bars during iterations
import warnings  # To control warnings display

# Suppress pandas SettingWithCopyWarning to avoid cluttering output
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)


def get_weather(lat, lon, date):
    """Get weather data for one location and date from Open-Meteo archive API."""
    try:
        # Format date string to ensure it's in YYYY-MM-DD format
        formatted_date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
        # Construct the URL for Open-Meteo API request with specified parameters
        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={formatted_date}&end_date={formatted_date}"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
            f"&timezone=UTC"
        )
        # Make GET request to the API with 10-second timeout
        response = requests.get(url, timeout=10)
        # Check if response status is OK (200)
        if response.status_code == 200:
            # Parse JSON and extract 'daily' data section
            data = response.json().get("daily", {})
            # Check if all required keys exist and have data
            if all(key in data and data[key] for key in ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "windspeed_10m_max"]):
                # Return a dictionary with weather data (first entry for the date)
                return {
                    "temp_max": data["temperature_2m_max"][0],
                    "temp_min": data["temperature_2m_min"][0],
                    "precip_mm": data["precipitation_sum"][0],
                    "wind_max": data["windspeed_10m_max"][0],
                }
        # Return None values if data is missing or incomplete
        return {"temp_max": None, "temp_min": None, "precip_mm": None, "wind_max": None}
    except Exception:
        # In case of any error (e.g., network issues), return None values
        return {"temp_max": None, "temp_min": None, "precip_mm": None, "wind_max": None}


def fetch_weather_parallel(unique_keys, max_workers=32):
    """Fetch weather data in parallel for a list of (lat, lon, date) tuples."""
    weather_cache = {}  # Dictionary to store weather data results keyed by (lat, lon, date)
    # Use ThreadPoolExecutor to run multiple requests concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks to executor for each unique (lat, lon, date)
        futures = {
            executor.submit(get_weather, lat, lon, date): (lat, lon, date)
            for (lat, lon, date) in unique_keys
        }
        # Create a tqdm progress bar to track completion of all futures
        with tqdm(total=len(futures), desc="Fetching Weather") as pbar:
            # Iterate over futures as they complete
            for future in as_completed(futures):
                key = futures[future]  # Get the (lat, lon, date) tuple for this future
                try:
                    # Store the weather data result in the cache
                    weather_cache[key] = future.result()
                except Exception:
                    # If error, store None values for this key
                    weather_cache[key] = {"temp_max": None, "temp_min": None, "precip_mm": None, "wind_max": None}
                pbar.update(1)  # Update progress bar
    return weather_cache  # Return the complete weather data dictionary


def enrich_dataframe(df):
    """
    Enrich a DataFrame with weather data for each (lat, lon, date) combination.
    Expects df to have columns: 'checkin_date', 'business_lat', 'business_long'
    """
    # Convert 'checkin_date' to string format YYYY-MM-DD
    df['date_str'] = pd.to_datetime(df['checkin_date']).dt.strftime("%Y-%m-%d")
    # Round latitude to 4 decimal places to reduce API calls for very close locations
    df['lat_rounded'] = df['business_lat'].round(4)
    # Round longitude similarly
    df['lon_rounded'] = df['business_long'].round(4)

    # Create a list of unique (lat, lon, date) tuples to fetch weather for
    unique_keys = list(set(zip(df['lat_rounded'], df['lon_rounded'], df['date_str'])))

    # Fetch weather data for all unique keys in parallel
    weather_cache = fetch_weather_parallel(unique_keys)

    # Create a new column with keys to map weather data back to rows
    df['weather_key'] = list(zip(df['lat_rounded'], df['lon_rounded'], df['date_str']))
    # Map weather data dictionary to each row and expand the dictionary into columns
    weather_data = df['weather_key'].map(weather_cache).apply(pd.Series)

    # Drop intermediate columns used for lookup
    df = df.drop(columns=['lat_rounded', 'lon_rounded', 'date_str', 'weather_key'])
    # Concatenate the original DataFrame with the new weather data columns
    df = pd.concat([df.reset_index(drop=True), weather_data.reset_index(drop=True)], axis=1)

    return df  # Return the enriched DataFrame
