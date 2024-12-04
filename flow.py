from utils_local import *
import pytz
import os


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
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    :param output: str: inflow, outflow or both
    :param aggregation_timeframe: str: aggregation timeframe
    :param file_format: str: 'json' or 'parquet' (default: 'json')
    """
    
    # model types: station_level, postcode_level, suburb_level, district_level, city_level
    # model codes: station_id, postcode, suburb, district, city

    # Load data: To change in cloud environment
    main_folder = 'analytics/snapshots'
    single_parquet = main_folder + '/all_data.parquet'
    
    # Handle Parquet file reading
    if file_format == 'parquet':
        if os.path.exists(single_parquet):
            try:
                # print("Debug: Reading Parquet file...")
                raw_data = pd.read_parquet(single_parquet)
                # print(f"Debug: Raw data shape: {raw_data.shape}")
                
                # Convert timestamp and filter by date range
                # print("Debug: Converting and filtering timestamps...")
                raw_data['timestamp'] = pd.to_datetime(raw_data['timestamp'])
                date_mask = (raw_data['timestamp'] >= pd.to_datetime(from_date)) & \
                           (raw_data['timestamp'] <= pd.to_datetime(to_date))
                raw_data = raw_data[date_mask].copy()
                # print(f"Debug: Filtered data shape: {raw_data.shape}")
                
                if len(raw_data) == 0:
                    raise ValueError(f"No data found in Parquet file for date range {from_date} to {to_date}")
                
                # Process each row and extract station data
                # print("Debug: Processing station data...")
                all_stations = []
                for _, row in raw_data.iterrows():
                    try:
                        stations = row['data']['stations']
                        for station in stations:
                            # Create a new record with the required fields
                            station_record = {
                                'station_id': str(station.get('station_id', '')),
                                'num_bikes_available': station.get('num_bikes_available', 0),
                                'timestamp_file': row['timestamp']
                            }
                            all_stations.append(station_record)
                    except Exception as e:
                        # print(f"Warning: Error processing row: {str(e)}")
                        continue
                
                if not all_stations:
                    raise ValueError("No valid station data found after processing")
                
                # Create DataFrame with only the needed columns
                stations_data = pd.DataFrame(all_stations)
                stations_data = stations_data[['timestamp_file', 'station_id', 'num_bikes_available']]
                # print(f"Debug: Final DataFrame shape: {stations_data.shape}")
                
            except Exception as e:
                # print(f"Debug: Exception occurred: {str(e)}")
                raise ValueError(f"Error reading Parquet file: {str(e)}")
        else:
            raise ValueError(f"Parquet file not found at {single_parquet}")
    else:
        # Original JSON processing
        dates = list_folders(main_folder)
        files = list_all_files(main_folder, dates)
        files = [f for f in files if f.endswith('.json')]
        files = filter_input_by_timeframe(files, from_date, to_date)
        stations_data = json_to_dataframe(files)

    stations = get_stations(model, model_code)
    stations_data = stations_data[stations_data['station_id'].isin(stations)]

    stations_data_filtered = stations_data[['timestamp_file', 'station_id', 'num_bikes_available']].sort_values(by=['station_id','timestamp_file'])

    # Convert timestamp_file to datetime if it's not already
    if pd.api.types.is_numeric_dtype(stations_data_filtered['timestamp_file']):
        stations_data_filtered['timestamp_file'] = pd.to_datetime(stations_data_filtered['timestamp_file'].astype(float), unit='s')

    stations_data_filtered['diff'] = stations_data_filtered.groupby('station_id')['num_bikes_available'].diff().fillna(0)
    stations_data_filtered['in_bikes'] = stations_data_filtered['diff'].apply(lambda x: x if x > 0 else 0)
    stations_data_filtered['out_bikes'] = stations_data_filtered['diff'].apply(lambda x: -x if x < 0 else 0)
    stations_data_filtered.drop('diff', axis=1, inplace=True)

    flow_agg = stations_data_filtered.groupby('timestamp_file')[['in_bikes', 'out_bikes']].sum().reset_index()
    
    # Convert aggregation_timeframe string to number of periods
    if aggregation_timeframe.endswith('h'):
        window_size = int(aggregation_timeframe[:-1])
    elif aggregation_timeframe.endswith('m'):
        window_size = int(aggregation_timeframe[:-1]) / 60
    else:
        raise ValueError("aggregation_timeframe must be in format '1h' or '30m'")
        
    # Calculate number of periods based on data frequency (assuming 5-minute intervals)
    periods = int(window_size * 12)  # 12 five-minute periods per hour
    
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
    
    flow_agg.reset_index(inplace=True)
    flow_agg['time'] = flow_agg['timestamp_file'].dt.strftime('%H:%M')

    # Output in json format
    if output == 'inflow':
        return flow_agg[['time', 'in_bikes']].to_dict('records')
    elif output == 'outflow':
        return flow_agg[['time', 'out_bikes']].to_dict('records')
    else:
        return flow_agg[['time', 'in_bikes', 'out_bikes']].to_dict('records')

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
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    :param output: str: inflow, outflow or both
    :param aggregation_timeframe: str: aggregation timeframe
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


