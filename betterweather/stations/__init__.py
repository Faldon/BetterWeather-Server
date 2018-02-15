import csv, re
from sqlalchemy import exc
from betterweather.models import WeatherStation


def import_from_csv(path_to_file, db):
    """

    :type path_to_file: str
    :type db: sqlachemy.orm.session.Session
    """
    try:
        print('Importing station data into database.')
        with open(path_to_file, 'r') as station_list:
            csv_reader = csv.reader(station_list, delimiter=',', quotechar='"')
            try:
                db.begin(subtransactions=True)
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
                        print('.', end='')
                db.commit()
                print('\nData import completed successfully.')
            except exc.IntegrityError as err_integrity:
                db.rollback()
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
