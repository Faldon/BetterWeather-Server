import os
import zipfile
from datetime import datetime
from urllib import request, error
from xml.etree import cElementTree as ElementTree

KML_NS = {
    'kml': "http://www.opengis.net/kml/2.2",
    'dwd': "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    'gx': "http://www.google.com/kml/ext/2.2",
    'xal': "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0",
    'atom': "http://www.w3.org/2005/Atom"
}


def get_forecast(source, definition, station_id, timestamp):
    """Get weather forecast

    Lookup the closest weather forecast of given station for the given time
    :param str source: The link to the directory containing the file
    :param str definition: The link to the MetElementDefinition xml file
    :param str station_id: The station id
    :param float timestamp: The time for the forecast as timestamp
    :return A weather forecast or False on error
    :rtype dict or bool
    """
    d = datetime.fromtimestamp(timestamp)
    try:
        remote_files = __get_remote_files(source, definition, station_id)
        if remote_files:
            zip_handle = zipfile.ZipFile(remote_files[0])
            file_name = zip_handle.filelist[0].filename
            zip_handle.extractall(path='/tmp/')

            kml_root = ElementTree.parse('/tmp/' + file_name)
            def_root = ElementTree.parse(remote_files[1])

            forecasts = __process_kml(kml_root, def_root)
            s = sorted(
                forecasts,
                key=lambda k: abs(datetime.combine(k['date']['value'], k['time']['value']).timestamp() - d.timestamp())
            )
            os.unlink(remote_files[0])
            os.unlink(remote_files[1])
            os.unlink('/tmp/' + file_name)
            return s[0]
        return False
    except IOError as err_io:
        print('IO Error while processing forecast data: ' + err_io.__str__())
        return False


def get_daily_trend(source, definition, station_id, date):
    d = datetime.strptime(date, '%Y-%m-%d')
    try:
        remote_files = __get_remote_files(source, definition, station_id)
        if remote_files:
            zip_handle = zipfile.ZipFile(remote_files[0])
            file_name = zip_handle.filelist[0].filename
            zip_handle.extractall(path='/tmp/')

            kml_root = ElementTree.parse('/tmp/' + file_name)
            def_root = ElementTree.parse(remote_files[1])

            forecasts = __process_kml(kml_root, def_root)
            f = filter(
                lambda k: k['date']['value'] == d.date(),
                forecasts

            )
            os.unlink(remote_files[0])
            os.unlink(remote_files[1])
            os.unlink('/tmp/' + file_name)
            return list(f)
        return False
    except IOError as err_io:
        print('IO Error while processing forecast data: ' + err_io.__str__())
        return False


def get_weekly_trend(source, definition, station_id):
    try:
        remote_files = __get_remote_files(source, definition, station_id)
        if remote_files:
            zip_handle = zipfile.ZipFile(remote_files[0])
            file_name = zip_handle.filelist[0].filename
            zip_handle.extractall(path='/tmp/')

            kml_root = ElementTree.parse('/tmp/' + file_name)
            def_root = ElementTree.parse(remote_files[1])

            forecasts = __process_kml(kml_root, def_root)

            os.unlink(remote_files[0])
            os.unlink(remote_files[1])
            os.unlink('/tmp/' + file_name)
            return forecasts
        return False
    except IOError as err_io:
        print('IO Error while processing forecast data: ' + err_io.__str__())
        return False


def get_present_weather(code):
    return __get_all_weathercodes().get(code, "")


def __get_remote_files(kml, definition, station_id):
    """Get files for the weather forecast from external source

        Download the kmz and the dwd element definiton xml from the dwd server
        :param str kml: The link to the kmz file of the requested station
        :param str definition: The link to the MetElementDefinition xml file
        :param str station_id: The station id
        :return The path to the downloaded files or False on error
        :rtype tuple or bool
        """
    url = os.path.join(kml, station_id, 'kml/MOSMIX_L_LATEST_' + station_id + '.kmz')
    try:
        mosmix_file = request.urlretrieve(url)
        definition_file = request.urlretrieve(definition)

        return mosmix_file[0], definition_file[0]
    except error.HTTPError as err_http:
        print('HTTP Error while retrieving forecast data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('IO Error while retrieving forecast data: ' + err_io.__str__())
        return False
    except error.ContentTooShortError as err_content_to_short:
        print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
        return False


def __get_all_weathercodes():
    return {
        0: """Cloud development not observed or observable""",
        1: """Clouds dissolving or becoming less developed""",
        2: """State of sky on the whole unchanged""",
        3: """Clouds generally forming or developing""",
        4: """Visibility reduced by smoke haze""",
        5: """Haze""",
        6: """Widespread dust in suspension in the air, not raised by wind at or near the station at the time of 
        observation.""",
        7: """Dust or sand raised by the wind at or near the station at the time of the observation, but no 
        well-developed dust whirl(s), and no sandstorm seen: or, in the case of ships, blowing spray at the station""",
        8: """Well developed dust whirl(s) or sand whirl(s) seen at or near the station during the preceding hour or at 
        the time of observation, but no duststorm or sandstorm""",
        9: """Duststorm or sandstorm within sight at the time of observation, or at the station during the preceding 
        hour""",
        10: """Mist""",
        11: """Patches of shallow fog or ice fog""",
        12: """More or less continuous""",
        13: """Lightning visible, no thunder heard""",
        14: """Precipitation within sight, not reaching the ground or surface of sea""",
        15: """Precipitation within sight, reaching ground or the surface of the sea, but distant, i.e. estimated to be 
        more than 5 km from the station""",
        16: """Precipitation within sight, reaching the ground or the surface of the sea, near to, but not at the 
        station""",
        17: """Thunderstorm, but no precipitation at the time of observation""",
        18: """Squalls at or within sight of the station during""",
        19: """Funnel cloud(s) or tuba during the preceding hour or at time of observation""",
        20: """Drizzle (not freezing) or snow grains""",
        21: """Rain (not freezing)""",
        22: """Snow""",
        23: """Rain and snow or ice pellets""",
        24: """Freezing drizzle or freezing rain""",
        25: """Shower(s) of rain""",
        26: """Shower(s) of snow, or of rain and snow""",
        27: """Shower(s) of hail, or of rain and hail""",
        28: """Fog or ice fog""",
        29: """Thunderstorm (with or without precipitation)""",
        30: """Slight duststorm ( has decreased during the preceding hour )""",
        31: """Moderate duststorm ( no appreciable change during the preceding hour)""",
        32: """Sandstorm ( has begun or increased during the preceding hour)""",
        33: """Severe Sandstorm ( has decreased during the preceding hour)""",
        34: """Duststorm ( no appreciable change during the preceding hour)""",
        35: """Sandstorm ( has begun or increased during the preceding hour)""",
        36: """Slight or moderate drifting snow ( generally low)""",
        37: """Heavy drifting snow ( below eye level)""",
        38: """Slight or moderate blowing snow ( generally high)""",
        39: """Heavy blowing snow ( above eye level)""",
        40: """Fog or ice fog at a distance at the time of observation, but not at the station during the preceding 
        hour, the fog or ice fog extending to a level above that of the observer""",
        41: """Fog or ice fog in patches""",
        42: """Fog or ice fog, sky visible ( has become thinner during the preceding hour)""",
        43: """Fog or ice fog, sky obscured ( has become thinner during the preceding hour)""",
        44: """Fog or ice fog, sky visible ( no appreciable change)""",
        45: """Fog or ice fog, sky obscured ( during the preceding hour)""",
        46: """Fog or ice fog, sky visible ( has begun or has become thicker during the preceding hour)""",
        47: """Fog or ice fog, sky obscured ( has begun or has become thicker during the preceding hour)""",
        48: """Fog or ice fog, sky visible""",
        49: """Fog or ice fog, sky obscured""",
        50: """Drizzle, not freezing, intermittent ( slight at time of observation)""",
        51: """Drizzle, not freezing, continuous ( slight at time of observation)""",
        52: """Drizzle, not freezing, intermittent ( moderate at time of observation)""",
        53: """Drizzle, not freezing, continuous ( moderate at time of observation)""",
        54: """Drizzle, not freezing, intermittent ( heavy (dense) at time of observation)""",
        55: """Drizzle, not freezing, continuous ( heavy (dense) at time of observation)""",
        56: """Drizzle, freezing, slight""",
        57: """Drizzle, freezing, moderate or heavy (dense)""",
        58: """Drizzle and rain, slight""",
        59: """Drizzle and rain, moderate or heavy""",
        60: """Rain, not freezing, intermittent ( slight at time of observation)""",
        61: """Rain, not freezing, continuous ( slight at time of observation)""",
        62: """Rain, not freezing, intermittent ( moderate at time of observation)""",
        63: """Rain, not freezing, continuous ( moderate at time of observation)""",
        64: """Rain, not freezing, intermittent ( heavy at time of observation)""",
        65: """Rain, not freezing, continuous ( heavy at time of observation)""",
        66: """Rain, freezing, slight""",
        67: """Rain, freezing, moderate or heavy""",
        68: """Rain or drizzle and snow, slight""",
        69: """Rain or drizzle and snow, moderate or heavy""",
        70: """Intermittent fall of snowflakes ( slight at time of observation)""",
        71: """Continuous fall of snowflakes ( slight at time of observation)""",
        72: """Intermittent fall of snowflakes ( moderate at time of observation)""",
        73: """Continuous fall of snowflakes ( moderate at time of observation)""",
        74: """Intermittent fall of snowflakes( heavy at time of observation)""",
        75: """Continuous fall of snowflakes ( heavy at time of observation)""",
        76: """Diamond dust (with or without fog)""",
        77: """Snow grains (with or without fog)""",
        78: """Isolated star-like snow crystals (with or without fog)""",
        79: """Ice pellets""",
        80: """Rain shower(s), slight""",
        81: """Rain shower(s), moderate or heavy""",
        82: """Rain shower(s), violent""",
        83: """Shower(s) of rain and snow mixed, slight""",
        84: """Shower(s) of rain and snow mixed, moderate or heavy""",
        85: """Snow shower(s), slight""",
        86: """Snow shower(s), moderate or heavy""",
        87: """Shower(s) of snow pellets or small hail ( slight )...""",
        88: """...with or without rain or rain and snow mixed ( moderate or heavy)""",
        89: """Shower(s) of hail, with or without rain or( slight )...""",
        90: """...rain and snow mixed, not associated with thunder ( moderate or heavy)""",
        91: """Slight rain at time of observation""",
        92: """Moderate or heavy rain at time of observation""",
        93: """Slight snow, or rain and snow mixed, or hail (2) at time of observation""",
        94: """Moderate or heavy snow, or rain and snow mixed, or hail (1) at time of observation""",
        95: """Thunderstorm, slight or moderate, without hail (2) but with rain and or snow at time of observation""",
        96: """Thunderstorm, slight or moderate, with hail (2) at time of observation""",
        97: """Thunderstorm, heavy, without hail (2) but with rain and or snow at time of observation""",
        98: """Thunderstorm combined with duststorm or sandstorm at time of observation""",
        99: """Thunderstorm, heavy, with hail (2) at time of observation"""
    }


def __process_kml(kml, definitions):
    """Process forecasts in kml format

    :param kml: The kml root
    :param definitions: The MetElementDefinition root
    :return: The forecasts for the specific placemark
    :rtype: list[dict]
    """
    forecast_dates = []
    for timestep in kml.findall('.//dwd:TimeStep', KML_NS):
        forecast_dates.append(datetime.strptime(timestep.text, '%Y-%m-%dT%H:%M:%S.000Z'))
    placemark = kml.find('.//kml:Placemark', KML_NS)
    undefined_sign = kml.find('.//dwd:DefaultUndefSign', KML_NS).text
    result = list()
    values = dict()

    for data in placemark.iterfind('.//dwd:Forecast', KML_NS):
        key = data.get('{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}elementName')
        values[key] = data.find('./dwd:value', KML_NS).text.split()
    for i in range(0, len(forecast_dates)):
        forecast = dict()
        forecast['date'] = {
            'value': forecast_dates[i].date(),
            'unit': None,
            'description': 'Date of forecast'
        }
        forecast['time'] = {
            'value': forecast_dates[i].time(),
            'unit': None,
            'description': 'Time of forecast'
        }
        for definition in definitions.iterfind('.//MetElement'):
            name = definition.find('ShortName').text
            unit = definition.find('UnitOfMeasurement').text
            description = definition.find('Description').text

            if unit[0] == undefined_sign:
                unit = None
            elif unit[0] == '%':
                unit = '%'
            elif unit[-1] == '°':
                unit = '°'

            forecast[name.lower()] = {
                'value': float(values[name][i]) if values.get(name, {i: undefined_sign})[i] != undefined_sign else None,
                'unit': unit,
                'description': description
            }
        result.append(forecast)
    return result
