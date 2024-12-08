from utils import *
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

def flow(
    from_date: str,
    to_date: str,
    model: str,
    model_code: str,
    output: str = 'both',
    aggregation_timeframe: str = '1h'
) -> dict:
    """
    Calculate flow statistics for a given time period.
    :param from_date: str: Start date in format 'YYYY-MM-DD HH:MM:SS'
    :param to_date: str: End date in format 'YYYY-MM-DD HH:MM:SS'
    :param model: str: Model type ('city', 'district', 'suburb')
    :param model_code: str: Model code (empty for city, district code for district, suburb code for suburb)
    :param output: str: Type of flow to analyze ('both', 'in', 'out')
    :param aggregation_timeframe: str: Time window for data aggregation (e.g., '1h', '30min')
    :return: dict: Flow statistics
    """
    
    initial_memory = get_memory_usage()
    logger.info(f"Starting flow with {initial_memory:.2f} MB memory usage")
    
    try:
        # Extract time adjustment from aggregation_timeframe
        value = int(aggregation_timeframe[:-1])
        unit = aggregation_timeframe[-1]
        
        # Convert date strings to datetime objects
        from_date_dt = pd.to_datetime(from_date)
        to_date_dt = pd.to_datetime(to_date)
        
        # Calculate adjustment based on unit for the window size
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
        
        # Adjust the start date to include one extra window before the requested from_date
        adjusted_from_date = from_date_dt - adjustment
        
        try:
            logger.info("Attempting to connect to S3...")
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
                parquet_file = io.BytesIO(response['Body'].read())
                logger.info("Successfully read response body")

                # Read the Parquet file directly into a DataFrame
                df = pd.read_parquet(parquet_file)
                logger.info(f"Successfully loaded Parquet file with {len(df)} records")

                # Convert timestamp and filter by date range (using adjusted start date)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df[(df['timestamp'] >= adjusted_from_date) & (df['timestamp'] <= to_date_dt)]
                
                if len(df) == 0:
                    raise ValueError("No data found for the specified date range")

                # Process the stations data
                stations_data = []
                for _, row in df.iterrows():
                    data = row['data']
                    if isinstance(data, str):
                        import json
                        data = json.loads(data)
                    
                    if 'stations' not in data:
                        continue
                        
                    for station in data['stations']:
                        stations_data.append({
                            'timestamp_file': row['timestamp'],
                            'station_id': str(station.get('station_id', '')),
                            'num_bikes_available': station.get('num_bikes_available', 0)
                        })

                if not stations_data:
                    raise ValueError("No valid station data found")

                # Convert to DataFrame
                stations_df = pd.DataFrame(stations_data)
                logger.info(f"Created stations DataFrame with {len(stations_df)} records")

                # Filter stations based on model and model_code
                stations = get_stations(model, model_code)
                stations_df = stations_df[stations_df['station_id'].isin(stations)]
                
                if stations_df.empty:
                    raise ValueError("No data found for the specified stations")

                # Sort and calculate differences
                stations_df = stations_df.sort_values(['station_id', 'timestamp_file'])
                stations_df['diff'] = stations_df.groupby('station_id')['num_bikes_available'].diff().fillna(0)
                stations_df['in_bikes'] = stations_df['diff'].apply(lambda x: x if x > 0 else 0)
                stations_df['out_bikes'] = stations_df['diff'].apply(lambda x: -x if x < 0 else 0)

                # Aggregate flow data
                flow_agg = stations_df.groupby('timestamp_file')[['in_bikes', 'out_bikes']].sum().reset_index()

                # Calculate number of periods for rolling average
                if unit == 'h':
                    periods = value * 12  # 12 five-minute periods per hour
                elif unit == 'm':
                    periods = value // 5  # Convert minutes to number of 5-minute periods
                elif unit == 'd':
                    periods = value * 24 * 12
                elif unit == 'w':
                    periods = value * 7 * 24 * 12
                elif unit == 'M':
                    periods = value * 30 * 24 * 12

                # Calculate rolling averages
                flow_agg.set_index('timestamp_file', inplace=True)
                flow_agg['in_bikes'] = flow_agg['in_bikes'].rolling(window=periods, min_periods=1).mean().fillna(0).round(2)
                flow_agg['out_bikes'] = flow_agg['out_bikes'].rolling(window=periods, min_periods=1).mean().fillna(0).round(2)

                # Filter to only include data from the original from_date to to_date
                flow_agg = flow_agg[flow_agg.index >= from_date_dt]
                flow_agg = flow_agg[flow_agg.index <= to_date_dt]

                # Format output
                flow_agg.reset_index(inplace=True)
                flow_agg['time'] = flow_agg['timestamp_file'].dt.strftime('%H:%M')

                # Return requested output format
                if output == 'inflow':
                    result = flow_agg[['time', 'in_bikes']].to_dict('records')
                elif output == 'outflow':
                    result = flow_agg[['time', 'out_bikes']].to_dict('records')
                else:
                    result = flow_agg[['time', 'in_bikes', 'out_bikes']].to_dict('records')

                return result

            except Exception as parse_error:
                logger.error(f"Error parsing Parquet file: {str(parse_error)}")
                raise ValueError(f"Failed to parse Parquet file: {str(parse_error)}")
            finally:
                if 'parquet_file' in locals():
                    parquet_file.close()
                force_garbage_collection()

        except Exception as e:
            logger.error(f"Error in S3 data retrieval process: {str(e)}")
            raise ValueError(f"Failed to retrieve data from S3: {str(e)}")

    except Exception as e:
        logger.error(f"Error in flow: {str(e)}")
        raise
    finally:
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
    """
    return flow(
        from_date=from_date,
        to_date=to_date,
        model=model,
        model_code=model_code,
        output=output,
        aggregation_timeframe=aggregation_timeframe
    )


