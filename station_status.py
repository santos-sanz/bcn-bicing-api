from utils_local import *

def station_status(
        station_timestamp: str,
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
    files = filter_input_by_timestamp(files, station_timestamp)


    stations_data = json_to_dataframe(files) 
    stations_master = pd.read_csv(stations_master_file)

    stations = get_stations(model, model_code, stations_master)
    stations_data = stations_data[stations_data['station_id'].isin(stations)]

    # TODO: Output: lat (station_master), lon (station_master), status (station_data), num_bikes_available (station_data)

