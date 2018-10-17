import os
import zipfile
from xml.etree import cElementTree as ElementTree
from urllib import request, error
from datetime import datetime, timedelta

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
    url = os.path.join(source, station_id, 'kml/MOSMIX_L_LATEST_' + station_id + '.kmz')
    try:
        mosmix_file = request.urlretrieve(url)
        zip_handle = zipfile.ZipFile(mosmix_file[0])
        file_name = zip_handle.filelist[0].filename
        zip_handle.extractall(path='/tmp/')
        kml_root = ElementTree.parse('/tmp/' + file_name)

        definition_file = request.urlretrieve(definition)
        def_root = ElementTree.parse(definition_file[0])

        forecasts = __process_kml(kml_root, def_root)
        s = sorted(
            forecasts,
            key=lambda k: abs(datetime.combine(k['date']['value'], k['time']['value']).timestamp() - d.timestamp())
        )
        return s[0]

    except error.HTTPError as err_http:
        print('HTTP Error while retrieving forecast data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('IO Error while retrieving forecast data: ' + err_io.__str__())
        return False
    except error.ContentTooShortError as err_content_to_short:
        print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
        return False


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
