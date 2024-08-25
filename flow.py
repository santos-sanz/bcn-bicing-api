from utils_local import *


def flow(
        from_date: str,
        to_date: str,
        model: str,
        model_code: str,
        output: str = 'both',    
        aggregation_timeframe: str = '1h'
):
    
    # model types: station_level, postcode_level, suburb_level, district_level, city_level
    # model codes: station_id, postcode, suburb, district, city

    df = pd.DataFrame()

    # Load data: To change in cloud environment
    main_folder = 'analytics/snapshots'
    stations_master_file = main_folder + '/stations_master.csv'
    
    dates = list_folders(main_folder)
    files = list_all_files(main_folder, dates)
    files = filter_input_by_timeframe(files, from_date, to_date)


    stations_data = json_to_dataframe(files) 
    stations_master = pd.read_csv(stations_master_file)
    stations = get_stations(model, model_code, stations_master)
    
    stations_data = stations_data[stations_data['station_id'].isin(stations)]

    stations_data_filtered = stations_data[['timestamp_file', 'station_id', 'num_bikes_available']].sort_values(by=['station_id','timestamp_file'])

    stations_data_filtered['diff'] = stations_data_filtered.groupby('station_id')['num_bikes_available'].diff().fillna(0)
    stations_data_filtered['in_bikes'] = stations_data_filtered['diff'].apply(lambda x: x if x > 0 else 0)
    stations_data_filtered['out_bikes'] = stations_data_filtered['diff'].apply(lambda x: -x if x < 0 else 0)
    stations_data_filtered.drop('diff', axis=1, inplace=True)

    flow_agg = stations_data_filtered.groupby('timestamp_file')[['in_bikes', 'out_bikes']].sum().reset_index()
    flow_agg['timestamp_file'] = pd.to_datetime(flow_agg['timestamp_file'], unit='s')
    
    flow_agg.set_index('timestamp_file', inplace=True)
    flow_agg['in_bikes'] = flow_agg['in_bikes'].rolling(window=aggregation_timeframe).mean()
    flow_agg['out_bikes'] = flow_agg['out_bikes'].rolling(window=aggregation_timeframe).mean()
    flow_agg.reset_index(inplace=True)  

    flow_agg.rename(columns={'timestamp_file': 'timestamp'}, inplace=True)

    # Output in json format

    if output == 'inflow':
        return flow_agg[['timestamp', 'in_bikes']].to_json(orient='records')
    elif output == 'outflow':
        return flow_agg[['timestamp', 'out_bikes']].to_json(orient='records')
    else:
        return flow_agg[['timestamp', 'in_bikes', 'out_bikes']].to_json(orient='records')


