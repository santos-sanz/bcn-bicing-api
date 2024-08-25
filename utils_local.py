import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def list_folders(folder):
    """
    List all subdirectories in the specified folder.
    :param folder: Path to the main folder.
    :return: List of folder names.
    """
    return [name for name in os.listdir(folder) if os.path.isdir(os.path.join(folder, name))]

def list_files(folder):
    """
    List all files in the specified directory.
    :param folder: Path to the directory.
    :return: List of file names.
    """
    return [name for name in os.listdir(folder) if os.path.isfile(os.path.join(folder, name))]

def list_all_files(main_folder, dates):
    """
    List all files within subdirectories specified by dates under the main folder.
    :param main_folder: Path to the main folder.
    :param dates: List of subdirectory names to search within.
    :return: List of file paths.
    """
    files = []
    for date in dates:
        folder = os.path.join(main_folder, date)
        for file in list_files(folder):
            files.append(os.path.join(folder, file))
    return files

from datetime import datetime
def filter_input_by_date(files:list, from_date:str, to_date:str):
    """
    Filter a list of files by the specified date range.
    :param files: List of file paths.
    :param from_date: Start date in the format 'YYYY-MM-DD'.
    :param to_date: End date in the format 'YYYY-MM-DD'.
    :return: List of file paths within the specified date range.
    """

    files_w_ts = {file:int(file.split('/')[-1].split('.')[0]) for file in files}
    from_date = int(datetime.timestamp(datetime.strptime(from_date, '%Y-%m-%d')))
    to_date = int(datetime.timestamp(datetime.strptime(to_date, '%Y-%m-%d')))
    files_filtered = [file for file in files if from_date <= files_w_ts[file] <= to_date]
    return files_filtered

def json_to_dataframe(json_files):
    """
    Convert a list of JSON files into a single pandas DataFrame.
    :param json_files: List of paths to JSON files.
    :return: pandas DataFrame containing all data from JSON files.
    """
    dataframes = []
    for json_file in json_files:
        with open(json_file) as f:
            data = json.load(f)
        df_data = pd.json_normalize(data['data']['stations'])
        df_data['file'] = json_file
        df_data['timestamp_file'] = os.path.basename(json_file).split('.')[0]
        dataframes.append(df_data)
    return pd.concat(dataframes)

def get_dis_surb(lat, lon, geojson):
    """
    Get the district that contains the specified coordinates.
    :param lat: Latitude.
    :param lon: Longitude.
    :param geojson: Path to the GeoJSON file containing the districts.
    :return: Name of the district containing the coordinates.
    """

    # Load the GeoJSON file
    gdf = gpd.read_file(geojson)

    # Create a Point object from the coordinates
    point = Point(lon, lat)

    # Check the district that contains the point
    for index, row in gdf.iterrows():
        if row['geometry'].contains(point):
            return {row['NOM'], row['CODI_UA']}

    return None

# Source: https://github.com/martgnz/bcn-geodata/blob/master/districtes/districtes.geojson

districts_geojson = 'analytics/snapshots/districtes.geojson'
def add_districts(stations_master, geojson = districts_geojson):
    """
    Add the district that contains each station to the stations_master DataFrame.
    :param stations_master: DataFrame containing the stations' information.
    :param geojson: Path to the GeoJSON file containing the districts.
    :return: DataFrame with the district and district_code columns added.
    """
    stations_master[['district', 'district_code']] = stations_master.apply(
        lambda row: pd.Series(get_dis_surb(row['lat'], row['lon'], geojson)), axis=1)
    return stations_master

suburb_geojson = 'analytics/snapshots/barris.geojson'
def add_suburbs(stations_master, geojson = suburb_geojson):
    """
    Add the suburb that contains each station to the stations_master DataFrame.
    :param stations_master: DataFrame containing the stations' information.
    :param geojson: Path to the GeoJSON file containing the suburbs.
    :return: DataFrame with the suburb and suburb_code columns added.
    """
    stations_master[['suburb', 'suburb_code']] = stations_master.apply(
        lambda row: pd.Series(get_dis_surb(row['lat'], row['lon'], geojson)), axis=1)
    return stations_master

def district_avg_position(df):
    """
    Calculate the average position of each district based on the stations' positions.
    :param df: DataFrame containing the district column and the latitude and longitude columns.
    :return: DataFrame with the average position of each district added.
    """

    df['avg_altitude'] = df.groupby('district')['altitude'].mean()
    df['avg_latitude'] = df.groupby('district')['latitude'].mean()
    df['avg_longitude'] = df.groupby('district')['longitude'].mean()
    return df

def district_avg_position_by_capacity(df):
    """
    Calculate the average position of each district based on the stations' positions and capacity.
    :param df: DataFrame containing the district column, the latitude and longitude columns, and the capacity column.
    :return: DataFrame with the average position of each district based on capacity added.
    """
    df['total_capacity'] = df.groupby('district')['capacity'].sum()
    df['latitude_capacity'] = df['latitude'] * df['capacity']
    df['longitude_capacity'] = df['longitude'] * df['capacity']
    df['avg_latitude_capacity'] = df.groupby('district')['latitude_capacity'].sum() / df.groupby('district')['capacity'].sum()
    df['avg_longitude_capacity'] = df.groupby('district')['longitude_capacity'].sum() / df.groupby('district')['capacity'].sum()
    df['altitude_capacity'] = df['altitude'] * df['capacity']
    df['avg_altitude_capacity'] = df.groupby('district')['altitude_capacity'].sum() / df.groupby('district')['capacity'].sum()
    df = df.drop(['latitude_capacity', 'longitude_capacity', 'altitude_capacity'], axis=1)
    return df

def district_stats():
    """
    Create a DataFrame with the population, motorized vehicles, and average score for each district.
    :return: DataFrame with the district statistics.
    """
    df = pd.DataFrame(district_population.items(), columns=['district', 'population'])
    df['motorized_vehicles'] = df['district'].map(motorized_vehicles)
    df['avg_score'] = df['district'].apply(lambda x: district_scores[x]['2023'])
    return df

# Population: February 2024: https://portaldades.ajuntament.barcelona.cat/es/estadísticas/yzlntdm2fs

district_population = {
    'Ciutat Vella': 111648,
    'Eixample': 275246,
    'Sants-Montjuïc': 192157,
    'Les Corts': 83732,
    'Sarrià-Sant Gervasi': 152365,
    'Gràcia': 126104,
    'Horta-Guinardó': 180566,
    'Nou Barris': 180183,
    'Sant Andreu': 155890,
    'Sant Martí': 249786,
}

# Bicing subscribers: https://portaldades.ajuntament.barcelona.cat/es/estadísticas/ahwkkg70ja

bicing_subscribers = {
    '2019': 113796,
    '2020': 126545,
    '2021': 130038,
    '2022': 136586,
    '2023': 147708,
}

# Viajes por tipo de bicicleta y año: https://portaldades.ajuntament.barcelona.cat/es/estadísticas/o5ncm8d1bw

travels_by_bike_type = {
    '2018': {
        'Electric': 0,
        'Mechanic': 12748000,
    },
    '2019': {
        'Electric': 0,
        'Mechanic': 11235396,
    },
    '2020': {
        'Electric': 0,
        'Mechanic': 8976523,
    },
    '2021': {
        'Electric': 5556856,
        'Mechanic': 9242470,
    },
    '2022': {
        'Electric': 9240085,
        'Mechanic': 7053712,
    },
    '2023': {
        'Electric': 12313579,
        'Mechanic': 5407897,
    },
    '2024': {
        'Electric': 3102725,
        'Mechanic': 960357,
    },
}

# Puntuacion media por distrito: https://portaldades.ajuntament.barcelona.cat/es/estadísticas/ks3r3iy9tj

district_scores = {

    'Ciutat Vella': {
        '2019': 7.5,
        '2020': 7.3,
        '2021': 7.0,
        '2022': 7.3,
        '2023': 6.7,
    },
    'Eixample': {
        '2019': 6.7,
        '2020': 6.8,
        '2021': 6.7,
        '2022': 6.5,
        '2023': 6.2,
    },
    'Sants-Montjuïc': {
        '2019': 6.5,
        '2020': 6.9,
        '2021': 6.7,
        '2022': 6.7,
        '2023': 6.2,
    },
    'Les Corts': {
        '2019': 6.4,
        '2020': 6.4,
        '2021': 6.3,
        '2022': 6.1,
        '2023': 6.0,
    },
    'Sarrià-Sant Gervasi': {
        '2019': 5.8,
        '2020': 6.2,
        '2021': 6.3,
        '2022': 6.2,
        '2023': 6.0,
    },
    'Gràcia': {
        '2019': 6.7,
        '2020': 6.5,
        '2021': 6.5,
        '2022': 6.6,
        '2023': 6.1,
    },
    'Horta-Guinardó': {
        '2019': 6.3,
        '2020': 6.5,
        '2021': 6.5,
        '2022': 6.3,
        '2023': 6.2,
    },
    'Nou Barris': {
        '2019': 6.7,
        '2020': 6.9,
        '2021': 6.6,
        '2022': 6.7,
        '2023': 6.3,
    },
    'Sant Andreu': {
        '2019': 6.8,
        '2020': 6.7,
        '2021': 6.6,
        '2022': 6.6,
        '2023': 6.5,
    },
    'Sant Martí': {
        '2019': 7.1,
        '2020': 6.9,
        '2021': 6.9,
        '2022': 6.7,
        '2023': 6.5,
    },
}

# Vwhiculos motorizados por distrito: a 31 de diciembre de 2023: https://portaldades.ajuntament.barcelona.cat/es/estadísticas/ki5kncyjyq

motorized_vehicles = {
    'Ciutat Vella': 17264,
    'Eixample': 75071,
    'Sants-Montjuïc': 52077,
    'Les Corts': 30881,
    'Sarrià-Sant Gervasi': 57605,
    'Gràcia': 32524,
    'Horta-Guinardó': 48601,
    'Nou Barris': 45229,
    'Sant Andreu': 43875,
    'Sant Martí': 68656,
}