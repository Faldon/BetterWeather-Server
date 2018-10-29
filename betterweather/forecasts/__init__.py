import os
import zipfile
from datetime import datetime
from urllib import request, error
from xml.etree import cElementTree as ElementTree
from betterweather import settings

KML_NS = {
    'kml': "http://www.opengis.net/kml/2.2",
    'dwd': "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    'gx': "http://www.google.com/kml/ext/2.2",
    'xal': "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0",
    'atom': "http://www.w3.org/2005/Atom"
}


def get_forecast(station_id, timestamp):
    """Get weather forecast

    Lookup the closest weather forecast of given station for the given time
    :param str station_id: The station id
    :param float timestamp: The time for the forecast as timestamp
    :return A weather forecast or False on error
    :rtype dict or bool
    """
    d = datetime.fromtimestamp(timestamp)
    try:
        remote_files = __get_remote_files(station_id)
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


def get_daily_trend(station_id, date):
    d = datetime.strptime(date, '%Y-%m-%d')
    try:
        remote_files = __get_remote_files(station_id)
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


def get_weekly_trend(station_id):
    try:
        remote_files = __get_remote_files(station_id)
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


def __get_remote_files(station_id):
    """Get files for the weather forecast from external source

        Download the kmz and the dwd element definiton xml from the dwd server
        :param str station_id: The station id
        :return The path to the downloaded files or False on error
        :rtype tuple or bool
        """
    url = os.path.join(settings.FORECASTS_URL, station_id, 'kml/MOSMIX_L_LATEST_' + station_id + '.kmz')
    try:
        mosmix_file = request.urlretrieve(url)
        definition_file = request.urlretrieve(settings.DEFINITION_URL)

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
        95: """slight or moderate thunderstorm with rain or snow""",
        57: """Drizzle, freezing, moderate or heavy (dence)""",
        56: """Drizzle, freezing, slight""",
        67: """Rain, freezing, moderate or heavy (dence)""",
        66: """Rain, freezing, slight""",
        86: """Snow shower(s), moderate or heavy""",
        85: """Snow shower(s), slight""",
        84: """Shower(s) of rain and snow mixed, moderate or heavy""",
        83: """Shower(s) of rain and snow mixed, slight""",
        82: """extremely heavy rain shower""",
        81: """moderate or heavy rain showers""",
        80: """slight rain shower""",
        75: """heavy snowfall, continuous""",
        73: """moderate snowfall, continuous""",
        71: """slight snowfall, continuous""",
        69: """moderate or heavy rain and snow""",
        68: """slight rain and snow""",
        55: """heavy drizzle, not freezing, continuous""",
        53: """moderate drizzle, not freezing, continuous""",
        51: """slight drizzle, not freezing, continuous""",
        65: """heavy rain, not freezing, continuous""",
        63: """moderate rain, not freezing, continuous""",
        61: """slight rain, not freezing, continuous""",
        49: """Ice Fog, sky not recognizable""",
        45: """Fog, sky not recognizable""",
        3: """Effective cloud cover at least 7 / 8""",
        2: """Effective cloud cover between 4.6 / 8 and 6 / 8""",
        1: """Effective cloud cover between 1 / 8 and 4.5 / 8""",
        0: """Effective cloud cover less than 1 / 8"""
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
