from utils_local import *
import os
import io
import logging
import pandas as pd
from datetime import datetime, timedelta
import psutil
import gc

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

def process_stations_chunk(chunk, from_date_dt, to_date_dt):
    """Helper function to process a chunk of data"""
    try:
        # Convert timestamp and filter by date range
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'])
        date_mask = (chunk['timestamp'] >= from_date_dt) & \
                    (chunk['timestamp'] <= to_date_dt)
        chunk = chunk[date_mask]
        
        if len(chunk) == 0:
            return []

        # Process each row and extract station data
        all_stations = []
        for _, row in chunk.iterrows():
            try:
                stations = row['data']['stations']
                for station in stations:
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
        
        return all_stations
    except Exception as e:
        logger.error(f"Error in process_stations_chunk: {str(e)}")
        return []
    finally:
        # Clean up references
        if 'chunk' in locals():
            del chunk
        if 'stations' in locals():
            del stations
        if 'row' in locals():
            del row

def station_stats(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str,
        file_format: str = 'json'
):
    """
    This function returns station statistics for a given time period and model.
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    :param file_format: str: 'json' or 'parquet' (default: 'json')
    """
    
    initial_memory = get_memory_usage()
    logger.info(f"Starting station_stats with {initial_memory:.2f} MB memory usage")
    
    if initial_memory > 400:  # 400MB threshold
        logger.warning("High initial memory usage detected")
        force_garbage_collection()
    
    try:
        # model types: station_level, postcode_level, suburb_level, district_level, city_level
        # model codes: station_id, postcode, suburb, district, city

        # Load data
        main_folder = 'analytics/snapshots'
        
        # Handle Parquet file reading
        if file_format == 'parquet':
            try:
                logger.info("Attempting to connect to S3...")
                # Read parquet file from S3
                try:
                    response = s3_client.get_object(
                        Bucket='bicingdata',
                        Key='2023/data.parquet'
                    )
                    logger.info("Successfully retrieved object from S3")
                except Exception as s3_error:
                    logger.error(f"S3 connection error: {str(s3_error)}")
                    raise ValueError(f"Failed to connect to S3: {str(s3_error)}")

                try:
                    logger.info("Reading response body...")
                    body_data = response['Body'].read()
                    body_size_mb = len(body_data) / 1024 / 1024
                    logger.info(f"Response body size: {body_size_mb:.2f} MB")
                    
                    if body_size_mb > 300:  # If body data is larger than 300MB
                        logger.warning("Large response body detected, may cause memory issues")
                    
                    parquet_file = io.BytesIO(body_data)
                    logger.info("Successfully read response body")
                    
                    # Clear body_data from memory
                    del body_data
                    force_garbage_collection()
                    
                except Exception as body_error:
                    logger.error(f"Error reading response body: {str(body_error)}")
                    raise ValueError(f"Failed to read response body: {str(body_error)}")

                # Convert date strings to datetime objects
                from_date_dt = pd.to_datetime(from_date)
                to_date_dt = pd.to_datetime(to_date)

                try:
                    logger.info("Starting Parquet parsing...")
                    # Process the file in chunks
                    all_stations = []
                    chunk_size = 100  # Reduced chunk size for memory constraints
                    processed_chunks = 0
                    
                    # Only read the columns we need
                    for chunk in pd.read_parquet(parquet_file, columns=['timestamp', 'data']):
                        chunk_stations = process_stations_chunk(chunk, from_date_dt, to_date_dt)
                        all_stations.extend(chunk_stations)
                        
                        processed_chunks += 1
                        if processed_chunks % 10 == 0:  # Log every 10 chunks
                            current_memory = get_memory_usage()
                            logger.info(f"Processed {processed_chunks} chunks, current memory: {current_memory:.2f} MB")
                            
                            if current_memory > 400:  # 400MB threshold
                                logger.warning("High memory usage detected during processing")
                                force_garbage_collection()
                        
                        # Free up memory
                        del chunk
                        
                    logger.info(f"Successfully processed all chunks. Total records: {len(all_stations)}")
                    
                    if not all_stations:
                        raise ValueError("No valid station data found after processing")
                    
                    # Create DataFrame with only the needed columns
                    stations_data = pd.DataFrame(all_stations)
                    
                    # Clear all_stations list from memory
                    del all_stations
                    force_garbage_collection()
                    
                except Exception as parquet_error:
                    logger.error(f"Error parsing Parquet file: {str(parquet_error)}")
                    logger.error(f"Error type: {type(parquet_error)}")
                    import traceback
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(f"Failed to parse Parquet file: {str(parquet_error)}")
                finally:
                    # Clean up
                    if 'parquet_file' in locals():
                        parquet_file.close()
                    force_garbage_collection()
                
            except Exception as e:
                logger.error(f"Error in S3 data retrieval process: {str(e)}")
                raise ValueError(f"Failed to retrieve data from S3: {str(e)}")
        else:
            # Original JSON processing
            dates = list_folders(main_folder)
            files = list_all_files(main_folder, dates)
            files = [f for f in files if f.endswith('.json')]
            files = filter_input_by_timeframe(files, from_date, to_date)
            stations_data = json_to_dataframe(files)

        stations = get_stations(model, model_code)
        stations_data = stations_data[stations_data['station_id'].isin(stations)]
        
        stations_master = get_station_information()
        stations_master['station_id'] = stations_master['station_id'].astype(int).astype(str)
        stations_master = stations_master[stations_master['station_id'].isin(stations)]

        stations_master = stations_master[['station_id', 'capacity']]
        stations_data = stations_data[['station_id', 'num_bikes_available', 'num_docks_available', 'timestamp_file']]

        # Monitor memory before merge operations
        get_memory_usage()
        
        stations_data = pd.merge(stations_data, stations_master, on='station_id', how='inner')

        #########################################################
        ### METRICS
        #########################################################

        stations_data_agg = stations_data.groupby('station_id').agg({
            'num_bikes_available': 'mean',
            'num_docks_available': 'mean'
        }).reset_index()
        stations_data_agg = stations_data_agg.rename(columns={
            'num_bikes_available': 'average_bikes_available',
            'num_docks_available': 'average_docks_available'
        })
        
        # Clear original stations_data if no longer needed
        if 'stations_data' in locals():
            del stations_data
            force_garbage_collection()
        
        #########################################################
        ### AVAILABILITY METRICS
        #########################################################
        # Calculate percentage and percentile of time with 0 bikes available
        zero_bikes_pct = stations_data.groupby('station_id').agg({
            'num_bikes_available': lambda x: (x == 0).mean() 
        }).reset_index()
        zero_bikes_pct = zero_bikes_pct.rename(columns={
            'num_bikes_available': 'pct_time_zero_bikes'
        })
        zero_bikes_pct['time_zero_bikes_percentile'] = zero_bikes_pct['pct_time_zero_bikes'].rank(pct=True, method='dense')
        
        # Calculate percentage and percentile of time with 0 docks available 
        zero_docks_pct = stations_data.groupby('station_id').agg({
            'num_docks_available': lambda x: (x == 0).mean() 
        }).reset_index()
        zero_docks_pct = zero_docks_pct.rename(columns={
            'num_docks_available': 'pct_time_zero_docks'
        })
        zero_docks_pct['time_zero_docks_percentile'] = zero_docks_pct['pct_time_zero_docks'].rank(pct=True, method='dense')
        
        # Merge with main dataframe
        stations_data_agg = pd.merge(stations_data_agg, zero_bikes_pct, on='station_id', how='inner')
        stations_data_agg = pd.merge(stations_data_agg, zero_docks_pct, on='station_id', how='inner')

        duration_segs = (pd.to_datetime(to_date) - pd.to_datetime(from_date)).total_seconds()
        events = calculate_use_events(stations_data, duration_segs)
        stations_data_agg = pd.merge(stations_data_agg, events, on='station_id', how='inner')
        
        # Convert DataFrame to dictionary with native Python types
        result = stations_data_agg.to_dict(orient='records')
        
        # Final cleanup
        del stations_data_agg
        force_garbage_collection()
        
        final_memory = get_memory_usage()
        logger.info(f"Finished station_stats with {final_memory:.2f} MB memory usage")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in station_stats: {str(e)}")
        raise
    finally:
        # Final garbage collection
        force_garbage_collection()

def station_stats_parquet(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str
):
    """
    Convenience function to get station statistics from Parquet files.
    This is equivalent to calling station_stats with file_format='parquet'.
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    """
    return station_stats(from_date, to_date, model, model_code, file_format='parquet')

def get_snapshot_stats(
        timestamp: str,
        model: str,
        model_code: str,
        file_format: str = 'json'
):
    """
    Get station statistics for a specific timestamp.
    :param timestamp: str: target timestamp
    :param model: str: model type
    :param model_code: str: model code
    :param file_format: str: 'json' or 'parquet' (default: 'json')
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

def get_snapshot_stats_parquet(
        timestamp: str,
        model: str,
        model_code: str
):
    """
    Convenience function to get snapshot statistics from Parquet files.
    This is equivalent to calling get_snapshot_stats with file_format='parquet'.
    :param timestamp: str: target timestamp
    :param model: str: model type
    :param model_code: str: model code
    """
    return get_snapshot_stats(timestamp, model, model_code, file_format='parquet')

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
    




