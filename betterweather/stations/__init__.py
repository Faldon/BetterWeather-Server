from math import sin, cos, sqrt, atan2, radians
from urllib import request, error
from betterweather import settings

R = 6373.0


def get_station(station_id):
    """Get weather station information

    :param str station_id: The station id
    :return: Weather station information or False on error
    :rtype: dict or bool
    """
    stations = __get_all_stations()
    if stations:
        return next(filter(lambda station: station.get('id').lower() == station_id.lower(), stations), {})
    return False


def get_nearest_station(latitude, longitude):
    """Get nearest weather station information to target poi

    :param float latitude: The latitude of target poi
    :param float longitude: The longitude of target poi
    :return: Weather station information or False on error
    :rtype: dict or bool
    """
    src = dict(latitude=latitude, longitude=longitude)
    stations = __get_all_stations()
    if stations:
        distances = []
        for station in stations:
            dst = dict(latitude=station.get('latitude'), longitude=station.get('longitude'))
            distance = __get_distance(src, dst)
            distances.append(dict(station=station, distance=distance))
        return sorted(distances, key=lambda k: k['distance'])[0].get('station')
    return False


def __get_all_stations():
    """Get all available weather stations

    :return: List of weather station information or False on error
    :rtype: list[dict] or bool
    """
    all_stations = list()
    try:
        file = request.urlretrieve(settings.STATIONS_URL)
        with open(file[0], 'rb') as station_list:
            for line in station_list.readlines():
                line = line.decode('latin-1')
                if len(line) >= 75 and line[12:12 + 5].strip() != 'id' and line[12:12 + 5] != '=====':
                    lat = float(line[44:44 + 6].split('.')[0]) + float(line[44:44 + 6].split('.')[1]) / 60
                    lon = float(line[51:51 + 7].split('.')[0]) + float(line[51:51 + 7].split('.')[1]) / 60
                    all_stations.append(
                        {
                            'id': line[12:12 + 5].strip(),
                            'ICAO': line[18:18 + 4] if line[18:18 + 4] != '----' else None,
                            'name': line[23:23 + 20].strip(),
                            'latitude': lat,
                            'longitude': lon,
                            'altitude': int(line[59:59 + 5]),
                            'type': line[72:72 + 4]
                        }
                    )
        return all_stations
    except error.HTTPError as err_http:
        print('HTTP Error while retrieving station data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('IO Error while retrieving station data: ' + err_io.__str__())
        return False
    except error.ContentTooShortError as err_content_to_short:
        print("Download of " + settings.STATIONS_URL + 'failed: ' + err_content_to_short.__str__())
        return False


def __get_distance(src, dst):
    """Calculate the distance between two points

    :param dict src: The source point
    :param dict dst: The destination point
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
