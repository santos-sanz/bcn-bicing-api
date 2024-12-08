from utils_local import *
import pytz
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import io
import logging
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

def flow(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str,
        output: str = 'both',    
        aggregation_timeframe: str = '1h',
        file_format: str = 'json'
):
    """
    This function returns the inflow, outflow or both of bikes for a given timeframe, model and model code.
    """
    
    initial_memory = get_memory_usage()
    logger.info(f"Starting flow with {initial_memory:.2f} MB memory usage")
    
    if initial_memory > 400:  # 400MB threshold
        logger.warning("High initial memory usage detected")
        force_garbage_collection()
    
    try:
        # Extract time adjustment from aggregation_timeframe
        value = int(aggregation_timeframe[:-1])
        unit = aggregation_timeframe[-1]
        
        from_date_dt = pd.to_datetime(from_date)
        
        # Calculate adjustment based on unit
        if unit == 'h':
            adjustment = timedelta(hours=value)
        elif unit == 'm':
            adjustment = timedelta(minutes=value)
        elif unit == 'd':
            adjustment = timedelta(days=value)
        elif unit == 'w':
            adjustment = timedelta(weeks=value)
        elif unit == 'M':
            adjustment = relativedelta(months=value)
        else:
            raise ValueError("aggregation_timeframe must be in format: '30m', '1h', '1d', '1w', or '1M'")
        
        # Adjust from_date based on aggregation window
        adjusted_from_date = (from_date_dt - adjustment).strftime('%Y-%m-%d %H:%M:%S')
        
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

                # Convert date strings to datetime objects and adjust for 4-hour offset
                from_date_dt = pd.to_datetime(from_date) - pd.Timedelta(hours=4)
                to_date_dt = pd.to_datetime(to_date) - pd.Timedelta(hours=4)

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
                    stations_data = stations_data[['timestamp_file', 'station_id', 'num_bikes_available']]
                    
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
            files = filter_input_by_timeframe(files, adjusted_from_date, to_date)
            stations_data = json_to_dataframe(files)

        stations = get_stations(model, model_code)
        stations_data = stations_data[stations_data['station_id'].isin(stations)]

        # Monitor memory before operations
        get_memory_usage()

        stations_data_filtered = stations_data[['timestamp_file', 'station_id', 'num_bikes_available']].sort_values(by=['station_id','timestamp_file'])
        del stations_data
        force_garbage_collection()

        # Convert timestamp_file to datetime if it's not already
        if pd.api.types.is_numeric_dtype(stations_data_filtered['timestamp_file']):
            stations_data_filtered['timestamp_file'] = pd.to_datetime(stations_data_filtered['timestamp_file'].astype(float), unit='s')

        stations_data_filtered['diff'] = stations_data_filtered.groupby('station_id')['num_bikes_available'].diff().fillna(0)
        stations_data_filtered['in_bikes'] = stations_data_filtered['diff'].apply(lambda x: x if x > 0 else 0)
        stations_data_filtered['out_bikes'] = stations_data_filtered['diff'].apply(lambda x: -x if x < 0 else 0)
        stations_data_filtered.drop('diff', axis=1, inplace=True)

        flow_agg = stations_data_filtered.groupby('timestamp_file')[['in_bikes', 'out_bikes']].sum().reset_index()
        del stations_data_filtered
        force_garbage_collection()
        
        # Calculate number of periods based on data frequency (assuming 5-minute intervals)
        if unit == 'h':
            periods = int(value * 12)  # 12 five-minute periods per hour
        elif unit == 'm':
            periods = int(value / 5)  # Convert minutes to number of 5-minute periods
        elif unit == 'd':
            periods = int(value * 24 * 12)  # 24 hours * 12 periods per hour
        elif unit == 'w':
            periods = int(value * 7 * 24 * 12)  # 7 days * 24 hours * 12 periods per hour
        elif unit == 'M':
            # Approximate a month as 30 days
            periods = int(value * 30 * 24 * 12)  # 30 days * 24 hours * 12 periods per hour
        
        # Set timestamp as index for rolling operations
        flow_agg.set_index('timestamp_file', inplace=True)
        
        flow_agg['in_bikes'] = flow_agg['in_bikes'].rolling(window=periods).mean()
        flow_agg['out_bikes'] = flow_agg['out_bikes'].rolling(window=periods).mean()
        
        # Fill NaN values with 0
        flow_agg['in_bikes'] = flow_agg['in_bikes'].fillna(0)
        flow_agg['out_bikes'] = flow_agg['out_bikes'].fillna(0)
        
        # Round values to 2 decimal places to avoid floating point issues
        flow_agg['in_bikes'] = flow_agg['in_bikes'].round(2)
        flow_agg['out_bikes'] = flow_agg['out_bikes'].round(2)
        
        # After all calculations are done, filter out the adjustment period we added at the beginning
        flow_agg = flow_agg[flow_agg.index >= from_date_dt]
        
        # Reset index and format time after filtering
        flow_agg.reset_index(inplace=True)
        flow_agg['time'] = flow_agg['timestamp_file'].dt.strftime('%H:%M')

        # Output in json format
        if output == 'inflow':
            result = flow_agg[['time', 'in_bikes']].to_dict('records')
        elif output == 'outflow':
            result = flow_agg[['time', 'out_bikes']].to_dict('records')
        else:
            result = flow_agg[['time', 'in_bikes', 'out_bikes']].to_dict('records')
        
        # Final cleanup
        del flow_agg
        force_garbage_collection()
        
        final_memory = get_memory_usage()
        logger.info(f"Finished flow with {final_memory:.2f} MB memory usage")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in flow: {str(e)}")
        raise
    finally:
        # Final garbage collection
        force_garbage_collection()

def flow_parquet(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str,
        output: str = 'both',    
        aggregation_timeframe: str = '1h'
):
    """
    Convenience function to get flow statistics from Parquet files.
    This is equivalent to calling flow with file_format='parquet'.
    """
    return flow(
        from_date=from_date,
        to_date=to_date,
        model=model,
        model_code=model_code,
        output=output,
        aggregation_timeframe=aggregation_timeframe,
        file_format='parquet'
    )


