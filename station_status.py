from utils_local import *

def station_status(
        station_timestamp: str,
        model: str,
        model_code: str
):
    """
    This function returns the number of stations, the sum of bikes available and the sum of docks available for a given timestamp, model and model code.
    :param station_timestamp: str: timestamp of the station data
    :param model: str: model type
    :param model_code: str: model code
    """
    
    # model types: station_level, postcode_level, suburb_level, district_level, city_level
    # model codes: station_id, postcode, suburb, district, city

    # Load data: To change in cloud environment
    main_folder = 'analytics/snapshots'
    stations_master_file = main_folder + '/stations_master.csv'
    
    dates = list_folders(main_folder)
    files = list_all_files(main_folder, dates)
    files = filter_input_by_timestamp(files, station_timestamp)


    stations_data = json_to_dataframe(files) 
    stations_master = pd.read_csv(stations_master_file)

    stations = get_stations(model, model_code, stations_master)
    stations_data = stations_data[stations_data['station_id'].isin(stations)]
    stations_master['station_id'] = stations_master['station_id'].astype(int).astype(str)
    stations_master = stations_master[stations_master['station_id'].isin(stations)]

    stations_master = stations_master[['station_id', 'name', 'post_code', 'lat', 'lon']]
    stations_master = add_districts(stations_master)
    stations_master = add_suburbs(stations_master)

    stations_data = stations_data[['station_id', 'status', 'num_bikes_available', 'num_docks_available']]

    stations_data = pd.merge(stations_data, stations_master, on='station_id', how='inner')
    # Agregate the data number of stations, sum of bikes and docks
    
    unique_stations_count = int(stations_data['station_id'].nunique())
    num_bikes_available_sum = int(stations_data['num_bikes_available'].sum())
    num_docks_available_sum = int(stations_data['num_docks_available'].sum())

    # rename lon to lng
    stations_data.rename(columns={'lon': 'lng'}, inplace=True)
    
    results = {
    'stations': unique_stations_count,
    'num_bikes_available': num_bikes_available_sum,
    'num_docks_available': num_docks_available_sum,
    'lat': stations_master['lat'].mean(),
    'lng': stations_master['lon'].mean()
    }

    results_json = json.dumps(results, indent=0)


    return results_json, stations_data.to_json(orient='records')



    




