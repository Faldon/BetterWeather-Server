import csv
import re
from sqlalchemy import exc
from math import sin, cos, sqrt, atan2, radians
from betterweather.models import WeatherStation

R = 6373.0


def import_stations_from_sql(path_to_file, db_session):
    """Import station data from sql

    :param str path_to_file: The sql file
    :param sqlachemy.orm.session.Session db_session: A SQLAlchemy database session
    :return True on success, False on failure
    :rtype: bool
    """
    db_session.begin(subtransactions=True)
    try:
        for query in open(path_to_file, 'r'):
            db_session.execute(query)
        db_session.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        print(err_dbapi)
        db_session.rollback()
        return False


def import_stations_from_csv(path_to_file, db_session):
    """Import station data from csv

    :param str path_to_file: The csv file
    :param sqlachemy.orm.session.Session db_session: A SQLAlchemy database session
    :return True on success, False on failure
    :rtype: bool
    """
    try:
        print('Importing station data into database.')
        with open(path_to_file, 'r') as station_list:
            csv_reader = csv.reader(station_list, delimiter=',', quotechar='"')
            try:
                db_session.begin(subtransactions=True)
                for row in csv_reader:
                    station_id = None
                    station_name = None
                    station_latitude = None
                    station_longitude = None

                    id_name_match = re.match(r"[0-9a-zA-Z]{4,5}\s", row[0])
                    if id_name_match:
                        s = id_name_match.string.split(' ')
                        station_id = s[0]
                        station_name = s[1]
                    lat_long_match = re.match(r"([-|‐]?\d{1,3}[,]\d{2})\s?([-|‐]?\d{1,3}[,]\d{2})\s?", row[1])
                    if lat_long_match:
                        latitude = lat_long_match.groups()[0]
                        station_latitude = latitude.replace('‐', '-').replace(',', '.')
                        longitude = lat_long_match.groups()[1]
                        station_longitude = longitude.replace('‐', '-').replace(',', '.')
                    station_amsl = row[2]
                    if station_id and station_name and station_latitude and station_longitude and station_amsl:
                        station = WeatherStation(
                            id=station_id,
                            name=station_name,
                            latitude=float(station_latitude),
                            longitude=float(station_longitude),
                            amsl=station_amsl
                        )
                        db_session.add(station)
                        print('.', end='')
                db_session.commit()
                print('\nData import completed successfully.')
            except exc.IntegrityError as err_integrity:
                db_session.rollback()
                print('\nAn error occured during operation: ' + err_integrity.__str__())
                print('Data import operation aborted.')
                return False

    except FileNotFoundError as err_file_not_found:
        print("Error on opening file " + path_to_file + ": " + err_file_not_found.__str__())
        return False
    except PermissionError as err_permission:
        print("Error on opening file " + path_to_file + ": " + err_permission.__str__())
        return False
    except IOError as err_io:
        print("Error on opening file " + path_to_file + ": " + err_io.__str__())
        return False
    return True


def get_station(db, station_id):
    """Get weather station information

    :param Session db: A SQLAlchemy database session
    :param str station_id: The station id
    :return: Weath station information
    :rtype: WeatherStation or None
    """
    return db.query(WeatherStation).get(station_id)


def get_nearest_station(db, latitude, longitude):
    src = dict(latitude=latitude, longitude=longitude)
    q = db.query(WeatherStation)
    distances = []
    for weather_station in q.all()[:]:
        dst = dict(latitude=weather_station.latitude, longitude=weather_station.longitude)
        distance = __get_distance(src, dst)
        distances.append(dict(station=weather_station, distance=distance))
    return sorted(distances, key=lambda k: k['distance'])[0]['station']


def __get_distance(src, dst):
    """Calculate the distance between two points

    :param Dict src: The source point
    :param Dict dst: The destination point
    :return: float The distance in kilometers
    """
    src_latitude = radians(float(src['latitude']))
    src_longitude = radians(float(src['longitude']))
    dst_latitude = radians(dst['latitude'])
    dst_longitude = radians(dst['longitude'])

    dlon = dst_longitude - src_longitude
    dlat = dst_latitude - src_latitude
    a = (sin(dlat / 2)) ** 2 + cos(src_latitude) * cos(dst_latitude) * (sin(dlon / 2)) ** 2
    c = 2 * atan2(sqrt(a), sqrt((1 - a)))
    distance = R * c
    return distance
