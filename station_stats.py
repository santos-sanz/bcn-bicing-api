from utils import *
import os
import io
import logging
import pandas as pd
from datetime import datetime, timedelta
import psutil
import gc
import pytz
import json  # Added to parse JSON strings

# Configure logging
logger = logging.getLogger(__name__)

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / 1024 / 1024  # Convert to MB
    logger.info(f"Current memory usage: {mem:.2f} MB")
    return mem

def force_garbage_collection():
    """Force garbage collection and log memory usage"""
    before = get_memory_usage()
    gc.collect()
    after = get_memory_usage()
    logger.info(f"Garbage collection freed {before - after:.2f} MB")

def process_stations_chunk(chunk):
    """Helper function to process station data"""
    try:
        all_stations = []
        for _, row in chunk.iterrows():
            try:
                # Parse 'data' from JSON string to dict if needed
                data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
                
                if 'stations' not in data:
                    continue
                
                for station in data['stations']:
                    station_record = {
                        'station_id': str(station.get('station_id', '')),
                        'num_bikes_available': station.get('num_bikes_available', 0),
                        'num_docks_available': station.get('num_docks_available', 0),
                        'timestamp_file': row['timestamp']
                    }
                    all_stations.append(station_record)
            except Exception as e:
                logger.warning(f"Error processing row: {str(e)}")
                continue
        
        return pd.DataFrame(all_stations)
    except Exception as e:
        logger.error(f"Error in process_stations_chunk: {str(e)}")
        return pd.DataFrame()

def get_station_stats(
    from_date: str,
    to_date: str,
    model: str,
    model_code: str
) -> pd.DataFrame:
    """Get station statistics for a given time period."""
    
    try:
        # Convert date strings to datetime objects with UTC timezone
        from_date_dt = pd.to_datetime(from_date)
        to_date_dt = pd.to_datetime(to_date)
        
        # Get list of stations to filter
        stations = get_stations(model, model_code)
        
        try:
            # Read only necessary columns from Parquet
            parquet_file = 'data/2023/data.parquet'
            if os.path.exists(parquet_file):
                df = pd.read_parquet(parquet_file, columns=['timestamp', 'data'])
                
                # Ensure timestamps are timezone-naive for comparison
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                from_date_dt = from_date_dt.tz_localize(None)
                to_date_dt = to_date_dt.tz_localize(None)
                
                # Filter by date range first to reduce memory usage
                date_mask = (df['timestamp'] >= from_date_dt) & (df['timestamp'] <= to_date_dt)
                df = df[date_mask]
                
                # Process filtered data
                stations_data = process_stations_chunk(df)
                del df
                force_garbage_collection()
                
                if stations_data.empty:
                    raise ValueError("No valid station data found")
                
                # Filter by stations
                stations_data = stations_data[stations_data['station_id'].isin(stations)]
                
                # Get station master data efficiently
                stations_master = get_station_information()[['station_id', 'capacity']]
                stations_master['station_id'] = stations_master['station_id'].astype(str)
                stations_master = stations_master[stations_master['station_id'].isin(stations)]
                
                # Merge and calculate metrics
                stations_data = pd.merge(stations_data, stations_master, on='station_id', how='inner')
                
                # Calculate aggregations
                agg_funcs = {
                    'num_bikes_available': ['mean', lambda x: (x == 0).mean()],
                    'num_docks_available': ['mean', lambda x: (x == 0).mean()]
                }
                
                stats = stations_data.groupby('station_id').agg(agg_funcs).reset_index()
                stats.columns = ['station_id', 'average_bikes_available', 'pct_time_zero_bikes',
                               'average_docks_available', 'pct_time_zero_docks']
                
                # Calculate percentiles
                stats['time_zero_bikes_percentile'] = stats['pct_time_zero_bikes'].rank(pct=True, method='dense')
                stats['time_zero_docks_percentile'] = stats['pct_time_zero_docks'].rank(pct=True, method='dense')
                
                # Calculate use events
                duration_segs = (to_date_dt - from_date_dt).total_seconds()
                events = calculate_use_events(stations_data, duration_segs)
                
                # Final merge and cleanup
                result = pd.merge(stats, events, on='station_id', how='inner')
                return result.to_dict(orient='records')
                
            else:
                raise ValueError("Parquet file not found")
                
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"Error in station_stats: {str(e)}")
        raise
    finally:
        force_garbage_collection()

def get_station_stats_by_timestamp(
    timestamp: str,
    model: str,
    model_code: str,
    file_format: str = 'parquet'
) -> pd.DataFrame:
    """
    Get station statistics for a given timestamp.
    :param timestamp: str: Timestamp in format 'YYYY-MM-DD HH:MM:SS'
    :param model: str: Model type ('city', 'district', 'suburb')
    :param model_code: str: Model code (empty for city, district code for district, suburb code for suburb)
    :param file_format: str: 'parquet' format for data storage
    :return: pd.DataFrame: Station statistics
    """
    main_folder = 'analytics/snapshots'
    single_parquet = 'data/2023/data.parquet'
    
    # Handle Parquet file reading
    if file_format == 'parquet':
        if os.path.exists(single_parquet):
            try:
                raw_data = pd.read_parquet(single_parquet)
                
                # Convert timestamp to datetime for comparison
                raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'])
                target_time = pd.to_datetime(timestamp)
                
                # Find closest timestamp
                raw_data['time_diff'] = abs(raw_data['timestamp'] - target_time)
                closest_row = raw_data.loc[raw_data['time_diff'].idxmin()]
                
                # Process station data from closest timestamp
                all_stations = []
                try:
                    stations = closest_row['data']['stations']
                    for station in stations:
                        station_record = {
                            'station_id': str(station.get('station_id', '')),
                            'num_bikes_available': station.get('num_bikes_available', 0),
                            'num_docks_available': station.get('num_docks_available', 0),
                            'timestamp_file': closest_row['timestamp']
                        }
                        all_stations.append(station_record)
                except Exception as e:
                    raise ValueError(f"Error processing station data: {str(e)}")
                
                if not all_stations:
                    raise ValueError("No valid station data found")
                
                stations_data = pd.DataFrame(all_stations)
                
            except Exception as e:
                raise ValueError(f"Error reading Parquet file: {str(e)}")
        else:
            raise ValueError(f"Parquet file not found at {single_parquet}")
    else:
        # Original JSON processing
        dates = list_folders(main_folder)
        files = list_all_files(main_folder, dates)
        files = [f for f in files if f.endswith('.json')]
        closest_file = filter_input_by_timestamp(files, timestamp)[0]
        stations_data = json_to_dataframe([closest_file])
    
    stations = get_stations(model, model_code)
    stations_data = stations_data[stations_data['station_id'].isin(stations)]
    
    return stations_data.to_dict(orient='records')

def calculate_use_events(df, duration_segs):
    """
    This function returns the number of use in and use out events by station.
    :param df: dataframe: dataframe with the station data
    :param duration_segs: int: duration in seconds
    :return: dataframe: dataframe with the number of use in and use out events by station
    """

    df['use_in'] = df.groupby('station_id')['num_bikes_available'].transform(lambda x: abs(x.diff().shift(-1)) * (x.diff().shift(-1) < 0))
    df['use_out'] = df.groupby('station_id')['num_bikes_available'].transform(lambda x: abs(x.diff().shift(-1)) * (x.diff().shift(-1) > 0))
    df['events'] = df['use_in'] + df['use_out']
    df = df[['station_id', 'events', 'use_in', 'use_out']]
    df = df.groupby('station_id').sum().reset_index()
    day = 24 * 60 * 60
    df['use_in_per_day'] = (df['use_in'] / duration_segs) * day
    df['use_out_per_day'] = (df['use_out'] / duration_segs) * day
    df['events_per_day'] = (df['events'] / duration_segs) * day

    stations_master = get_station_information()[['station_id', 'capacity']]
    stations_master['station_id'] = stations_master['station_id'].astype(int).astype(str)
    df = pd.merge(df, stations_master, on='station_id', how='inner')

    df['use_in_per_day_capacity'] = df['use_in_per_day'] / df['capacity']
    df['use_out_per_day_capacity'] = df['use_out_per_day'] / df['capacity']
    df['events_per_day_capacity'] = df['events_per_day'] / df['capacity']

    # Calculate the percentile of use 

    df['events_percentile'] = df['events'].rank(pct=True, method='dense')
    df['use_in_percentile'] = df['use_in'].rank(pct=True, method='dense')
    df['use_out_percentile'] = df['use_out'].rank(pct=True, method='dense')

    df['events_per_day_capacity_percentile'] = df['events_per_day_capacity'].rank(pct=True, method='dense')
    df['use_in_per_day_capacity_percentile'] = df['use_in_per_day_capacity'].rank(pct=True, method='dense')
    df['use_out_per_day_capacity_percentile'] = df['use_out_per_day_capacity'].rank(pct=True, method='dense')


    return df
    




