import csv, re
from sqlalchemy import exc
from betterweather.models import WeatherStation


def import_from_csv(path_to_file, db):
    """

    :type path_to_file: str
    :type db: sqlachemy.engine.Connection
    """
    try:
        with open(path_to_file,'r') as station_list:
            csv_reader = csv.reader(station_list, delimiter=',', quotechar='"')
            try:
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
                        db.add(station)
            except exc.IntegrityError:
                db.rollback()

    except FileNotFoundError:
        print("File not found")
        return False
    except IOError:
        print("IO Error")
        return False
    return True
