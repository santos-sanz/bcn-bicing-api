from utils_local import *

def station_status(
        station_timestamp: str,
        model: str,
        model_code: str
):
    
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

    stations_master = stations_master[['station_id', 'lat', 'lon']]
    stations_data = stations_data[['station_id', 'status', 'num_bikes_available', 'num_docks_available']]

    stations_data = pd.merge(stations_data, stations_master, on='station_id', how='inner')

    return stations_data.to_json(orient='records')



    




