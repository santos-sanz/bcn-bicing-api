from utils_local import *
import pytz


def flow(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str,
        output: str = 'both',    
        aggregation_timeframe: str = '1h'
):
    """
    This function returns the inflow, outflow or both of bikes for a given timeframe, model and model code.
    :param from_date: str: start date
    :param to_date: str: end date
    :param model: str: model type
    :param model_code: str: model code
    :param output: str: inflow, outflow or both
    :param aggregation_timeframe: str: aggregation timeframe
    """
    
    
    # model types: station_level, postcode_level, suburb_level, district_level, city_level
    # model codes: station_id, postcode, suburb, district, city

    # Load data: To change in cloud environment
    main_folder = 'analytics/snapshots'
    
    dates = list_folders(main_folder)
    files = list_all_files(main_folder, dates)
    files = filter_input_by_timeframe(files, from_date, to_date)


    stations_data = json_to_dataframe(files) 
    stations = get_stations(model, model_code)

    stations_data = stations_data[stations_data['station_id'].isin(stations)]

    stations_data_filtered = stations_data[['timestamp_file', 'station_id', 'num_bikes_available']].sort_values(by=['station_id','timestamp_file'])

    stations_data_filtered['diff'] = stations_data_filtered.groupby('station_id')['num_bikes_available'].diff().fillna(0)
    stations_data_filtered['in_bikes'] = stations_data_filtered['diff'].apply(lambda x: x if x > 0 else 0)
    stations_data_filtered['out_bikes'] = stations_data_filtered['diff'].apply(lambda x: -x if x < 0 else 0)
    stations_data_filtered.drop('diff', axis=1, inplace=True)

    flow_agg = stations_data_filtered.groupby('timestamp_file')[['in_bikes', 'out_bikes']].sum().reset_index()
    flow_agg['timestamp_file'] = pd.to_datetime(flow_agg['timestamp_file'], unit='s')
    
    flow_agg.set_index('timestamp_file', inplace=True)
    
    # Convert aggregation_timeframe string to number of periods
    if aggregation_timeframe.endswith('h'):
        window_size = int(aggregation_timeframe[:-1])
    elif aggregation_timeframe.endswith('m'):
        window_size = int(aggregation_timeframe[:-1]) / 60
    else:
        raise ValueError("aggregation_timeframe must be in format '1h' or '30m'")
        
    # Calculate number of periods based on data frequency (assuming 5-minute intervals)
    periods = int(window_size * 12)  # 12 five-minute periods per hour
    
    flow_agg['in_bikes'] = flow_agg['in_bikes'].rolling(window=periods).mean()
    flow_agg['out_bikes'] = flow_agg['out_bikes'].rolling(window=periods).mean()
    
    # Fill NaN values with 0
    flow_agg['in_bikes'] = flow_agg['in_bikes'].fillna(0)
    flow_agg['out_bikes'] = flow_agg['out_bikes'].fillna(0)
    
    # Round values to 2 decimal places to avoid floating point issues
    flow_agg['in_bikes'] = flow_agg['in_bikes'].round(2)
    flow_agg['out_bikes'] = flow_agg['out_bikes'].round(2)
    
    flow_agg.reset_index(inplace=True)
    flow_agg.rename(columns={'timestamp_file': 'timestamp'}, inplace=True)
    flow_agg['time'] = flow_agg['timestamp'].dt.strftime('%H:%M')

    # Output in json format
    if output == 'inflow':
        return flow_agg[['time', 'in_bikes']].to_dict('records')
    elif output == 'outflow':
        return flow_agg[['time', 'out_bikes']].to_dict('records')
    else:
        return flow_agg[['time', 'in_bikes', 'out_bikes']].to_dict('records')


