import os
import re
import csv
import bz2
import time
import zipfile
import multiprocessing
from xml.etree import cElementTree as ElementTree
from urllib import request, error
from datetime import datetime, timedelta
from sqlalchemy.orm.session import Session
from sqlalchemy import exc
from sqlalchemy.orm import joinedload
from betterweather import betterweather
from betterweather.models import ForecastData, WeatherStation


KML_NS = {
    'kml': "http://www.opengis.net/kml/2.2",
    'dwd': "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    'gx': "http://www.google.com/kml/ext/2.2",
    'xal': "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0",
    'atom': "http://www.w3.org/2005/Atom"
}


def update_mosmix_poi(root_url, db_session, verbose):
    """Update weather forecast from mosmix poi csv

    :param str root_url: The link to the directory containing the files
    :param sqlachemy.orm.session.Session db_session: A SQLAlchemy database session
    :param bool verbose: Print verbose output
    :return: True on success, False on error
    :rtype: bool
    """
    try:
        if verbose:
            print('Retrieving forecast data from ' + root_url)
        root = request.urlopen(root_url)
        links = re.findall(r"(?:href=['\"])([:/.A-z?<_&\s=>0-9;-]+)", root.read().decode('utf-8'))
        db_session.autoflush = False
        station_ids = db_session.query(WeatherStation.id).all()
        chunk_size = int(len(links) / len(os.sched_getaffinity(0)))
        processes = []
        result_queue = multiprocessing.JoinableQueue()
        for links in ([links[i:i + chunk_size] for i in range(0, len(links), chunk_size)]):
            local_process = multiprocessing.Process(target=__process_links_poi,
                                                    args=(root_url, links, station_ids, verbose, result_queue))
            local_process.start()
            processes.append(local_process)
        for p in processes:
            if not p == multiprocessing.current_process():
                p.join()

        db_session.execute("START TRANSACTION;")
        while not result_queue.empty():
            db_session.execute(result_queue.get())
        db_session.execute("COMMIT;")
        db_session.flush()
        if verbose:
            print('Processing of ' + root_url + ' finished.')
        return True
    except exc.DBAPIError as err_dbapi:
        db_session.execute("ROLLBACK;")
        db_session.flush()
        print('\nDB Error: ' + err_dbapi.__str__())
        return False
    except error.HTTPError as err_http:
        print('Error while retrieving forecast data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('Error while retrieving forecast data: ' + err_io.__str__())
        return False


def update_mosmix_o_underline(root_url, db_session, verbose):
    """Update weather forecast from mosmix o_underline

    :param str root_url: The link to the directory containing the files
    :param sqlachemy.orm.session.Session db_session: A SQLAlchemy database session
    :param bool verbose: Print verbose output
    :return True on success, False on failure
    :rtype: bool
    """
    try:
        if verbose:
            print('Retrieving forecast data from ' + root_url)
        root = request.urlopen(root_url)
        links = re.findall(r"(?:href=['\"])([:/.A-z?<_&\s=>0-9;-]+)", root.read().decode('utf-8'))
        db_session.autoflush = False
        db_session.begin(subtransactions=True)
        processes = []
        result_queue = multiprocessing.JoinableQueue()
        for link in links:
            if re.search(r'latest', link):
                local_process = multiprocessing.Process(target=__process_link_ascii,
                                                        args=(root_url, link, verbose, result_queue))
                local_process.start()
                processes.append(local_process)
        for p in processes:
            if not p == multiprocessing.current_process():
                p.join()

        db_session.execute("START TRANSACTION;")
        while not result_queue.empty():
            db_session.execute(result_queue.get())
        db_session.execute("COMMIT;")
        db_session.flush()
        return True
    except exc.DBAPIError as err_dbapi:
        db_session.execute("ROLLBACK;")
        db_session.flush()
        print('\nDB Error: ' + err_dbapi.__str__())
        return False
    except error.HTTPError as err_http:
        print('HTTP Error while retrieving forecast data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('IO Error while retrieving forecast data: ' + err_io.__str__())
        return False


def update_mosmix_kml(root_url, verbose):
    """Update weather forecast from mosmix kml

    :param str root_url: The link to the directory containing the file
    :param bool verbose: Print verbose output
    :return True on success, False on failure
    :rtype: bool
    """
    db_session = betterweather.connect_db()
    db_session.autoflush = False
    url = root_url + 'MOSMIX_S_LATEST_240.kmz'
    if verbose:
        print('Retrieving forecast data from ' + url)
    try:
        file = request.urlretrieve(url)
        zip_handle = zipfile.ZipFile(file[0])
        file_name = zip_handle.filelist[0].filename
        zip_handle.extractall(path='/tmp/')
        forecast_dates = []
        kml_root = ElementTree.parse('/tmp/' + file_name)
        for timestep in kml_root.findall('.//dwd:TimeStep', KML_NS):
            forecast_dates.append(datetime.strptime(timestep.text, '%Y-%m-%dT%H:%M:%S.000Z'))
        placemarks = kml_root.findall('.//kml:Placemark', KML_NS)
        chunk_size = int(len(placemarks) / len(os.sched_getaffinity(0)))
        processes = []
        result_queue = multiprocessing.JoinableQueue()
        for partition in ([placemarks[i:i+chunk_size] for i in range(0, len(placemarks), chunk_size)]):
            local_process = multiprocessing.Process(target=__process_kml,
                                                    args=(forecast_dates, partition, verbose, result_queue))
            local_process.start()
            processes.append(local_process)
        for p in processes:
            if not p == multiprocessing.current_process():
                p.join()

        db_session.execute("START TRANSACTION;")
        while not result_queue.empty():
            db_session.execute(result_queue.get())
        db_session.execute("COMMIT;")
        db_session.flush()
        os.remove(file[0])
        os.remove('/tmp/' + file_name)
        if verbose:
            print('Processing of ' + file_name + ' finished.')
        return True
    except exc.DBAPIError as err_dbapi:
        db_session.execute("ROLLBACK;")
        db_session.flush()
        print('\nDB Error: ' + err_dbapi.__str__())
        return False
    except error.HTTPError as err_http:
        print('HTTP Error while retrieving forecast data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('IO Error while retrieving forecast data: ' + err_io.__str__())
        return False
    except error.ContentTooShortError as err_content_to_short:
        print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
        return False


def get_forecast(db, station_id, timestamp, full):
    """Get weather forecast

    Lookup the closest weather forecast of given station for the given time
    :param Session db: A SQLAlchemy database session
    :param str station_id: The station id
    :param float timestamp: The time for the forecast as timestamp
    :param bool full: Join related objects to result
    :return A weather forecast
    :rtype ForecastData or None
    """
    d = datetime.fromtimestamp(timestamp)
    if full:
        q = db.query(ForecastData).options(
            joinedload(ForecastData.station, innerjoin=True)
        ).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date()
        )
    else:
        q = db.query(ForecastData).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date()
        )
    forecasts = []
    for data in q.all():
        forecasts.append(dict(
            forecast=data,
            timediff=abs((d.time().hour * 60 + d.time().minute) - (data.time.hour * 60 + data.time.minute))
        ))
    sorted_data = sorted(forecasts, key=lambda k: k['timediff'])
    if sorted_data:
        return sorted_data[0]['forecast']
    return None


def __process_links_poi(root_url, links, stations, verbose, result_queue):
    """Download forecasts in csv format and process it for saving them to the database

    :param root_url: The root dir containing the o_underline files
    :param links: A list of download link
    :param stations: A list of all available station ids
    :param verbose: Print verbose output
    :param result_queue: The queue for objects to be saved to the database
    :return:
    """
    with betterweather.app.app_context():
        for link in links:
            station_id = link.__str__()[:5].replace("_", "")
            tup = (station_id, )
            if tup not in stations:
                if verbose:
                    print("Skipped station " + station_id)
                continue
            if verbose:
                print("Processing station " + station_id)

            url = root_url + link
            try:
                file = request.urlretrieve(url)
                time.sleep(1)
            except error.ContentTooShortError as err_content_to_short:
                if verbose:
                    print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
                continue
            except error.URLError as url_error:
                if verbose:
                    print("Download of " + url + 'failed: ' + url_error.__str__())
                continue
            with open(file[0], 'r') as forecast_for_station:
                csv_reader = csv.reader(forecast_for_station, delimiter=";")
                for row in csv_reader:
                    try:
                        dt_object = datetime.strptime(row[0], '%d.%m.%y')
                    except ValueError:
                        continue
                    for i in range(0, len(row)):
                        row[i] = row[i].replace(',', '.').replace(' ', '')
                    try:
                        forecast_date = dt_object.date()
                        forecast_time = datetime.strptime(row[1], '%H:%M').time()
                        dp = ForecastData()
                        dp.date = forecast_date
                        dp.time = forecast_time
                        dp.station_id = station_id
                        dp.id = station_id + dp.date.__str__() + dp.time.__str__()
                        dp.tt = float(row[2]) if row[2] != "---" else None
                        dp.td = float(row[3]) if row[3] != "---" else None
                        dp.tx = float(row[4]) if row[4] != "---" else None
                        dp.tn = float(row[5]) if row[5] != "---" else None
                        dp.tm = float(row[6]) if row[6] != "---" else None
                        dp.tg = float(row[7]) if row[7] != "---" else None
                        dp.dd = int(row[8]) if row[8] != "---" else None
                        dp.ff = float(row[9]) if row[9] != "---" else None
                        dp.fx = float(row[10]) if row[10] != "---" else None
                        dp.fx6 = int(row[11]) if row[11] != "---" else None
                        dp.fx9 = int(row[12]) if row[12] != "---" else None
                        dp.fx11 = int(row[13]) if row[13] != "---" else None
                        dp.rr1 = float(row[14]) if row[14] != "---" else None
                        dp.rr3 = float(row[15]) if row[15] != "---" else None
                        dp.rr6 = float(row[16]) if row[16] != "---" else None
                        dp.rr12 = float(row[17]) if row[17] != "---" else None
                        dp.rr24 = float(row[18]) if row[18] != "---" else None
                        dp.rrp6 = int(row[19]) if row[19] != "---" else None
                        dp.rrp12 = int(row[20]) if row[20] != "---" else None
                        dp.rrp24 = int(row[21]) if row[21] != "---" else None
                        dp.ev = float(row[22]) if row[22] != "---" else None
                        dp.ww = int(row[23]) if row[23] != "---" else None
                        dp.w = int(row[24]) if row[24] != "---" else None
                        dp.vv = float(row[25]) if row[25] != "---" else None
                        dp.n = int(row[26]) if row[26] != "---" else None
                        dp.nf = int(row[27]) if row[27] != "---" else None
                        dp.nl = int(row[28]) if row[28] != "---" else None
                        dp.nm = int(row[29]) if row[29] != "---" else None
                        dp.nh = int(row[30]) if row[30] != "---" else None
                        dp.pppp = float(row[31]) if row[31] != "---" else None
                        dp.ss1 = float(row[32]) if row[32] != "---" else None
                        dp.ss24 = float(row[33]) if row[33] != "---" else None
                        dp.gss1 = float(row[34]) if row[34] != "---" else None
                        result_queue.put(dp.to_upsert())
                        if verbose:
                            print('Added forecast for station ' + dp.station_id, end='')
                            print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
                    except ValueError:
                        continue
                if verbose:
                    print('\nProcessing data for station ' + station_id + ' finished.')
            os.remove(file[0])
            request.urlcleanup()
            result_queue.cancel_join_thread()
        return True


def __process_link_ascii(root_url, link, verbose, result_queue):
    """Download forecasts in o_underline format and process it for saving them to the database

    :param root_url: The root dir containing the o_underline files
    :param link: The link to the o_underline file
    :param verbose: Print verbose output
    :param result_queue: The queue for objects to be saved to the database
    """
    result = False
    with betterweather.app.app_context():
        match = re.search(r'(\d{3}_\d)', link)
        hours = match.group(0).split('_')
        if verbose:
            print("Processing file " + match.string)
        url = root_url + link
        try:
            file = request.urlretrieve(url)
            with bz2.open(file[0], 'rb') as forecast_for_station:
                data = forecast_for_station.read().decode('latin-1').splitlines()
                station_data = [e for e in data if re.search(r'\*', e)]
                weather_forecast_data = [e for e in data if not re.search(r'\*', e)]
                for row in station_data:
                    station_id = row[0:5].strip()
                    station_name = ''.join(row)[8:23].strip()
                    props = ''.join(row)[24:].split()
                    lon = props[0][:-2] + '.' + props[0][-2:]
                    lat = props[1][:-2] + '.' + props[1][-2:]
                    station_longitude = float(lon)
                    station_latitude = float(lat)
                    station_amsl = int(props[2])

                    weather_station = WeatherStation()
                    weather_station.id = station_id
                    weather_station.name = station_name
                    weather_station.latitude = station_latitude
                    weather_station.longitude = station_longitude
                    weather_station.amsl = station_amsl
                    result_queue.put(weather_station.to_upsert())
                    if verbose:
                        print('New station ' + station_id + ' added.')
                csv_reader = csv.reader(weather_forecast_data, delimiter=" ")
                creation_time = None
                for row in csv_reader:
                    try:
                        cf = 0
                        if row[cf] == 'VERSION':
                            continue
                        if row[cf] == 'MOS':
                            creation_time = datetime.strptime(row[1], '%y%m%d%H')
                            continue
                        if row[cf] == '':
                            cf = 1
                        station_id = row[cf]
                        if station_id == '99999' or row[cf + 1] == '*':
                            continue

                        dt_object = creation_time + timedelta(hours=int(row[cf + 1]))
                        forecast_date = dt_object.date()
                        forecast_time = dt_object.time()
                        dp = ForecastData()
                        dp.date = forecast_date
                        dp.time = forecast_time
                        dp.station_id = station_id
                        dp.id = station_id + dp.date.__str__() + dp.time.__str__()
                        dp.tt = float(row[cf + 3]) if not re.match(r'(-{3}|/{3}|$)', row[cf + 3]) else None
                        dp.td = float(row[cf + 4]) if not re.match(r'(-{3}|/{3}|$)', row[cf + 4]) else None
                        dp.tx = float(row[cf + 5]) if not re.match(r'(-{3}|/{3}|$)', row[cf + 5]) else None
                        dp.tn = float(row[cf + 6]) if not re.match(r'(-{3}|/{3}|$)', row[cf + 6]) else None
                        dp.dd = int(row[cf + 7]) if not re.match(r'(-{2}|/{2}|$)', row[cf + 7]) else None
                        dp.ff = float(row[cf + 8]) * 1.852 if not re.match(r'(-{2}|/{2}|$)', row[cf + 8]) else None
                        dp.fx = float(row[cf + 9]) * 1.852 if not re.match(r'(-{2}|/{2}|$)', row[cf + 9]) else None
                        dp.ww = int(row[cf + 12]) if not re.match(r'(-{2}|/{2}|$)', row[cf + 12]) else None
                        dp.w = int(row[cf + 13]) if not re.match(r'([-/])', row[cf + 13]) else None
                        dp.n = int(row[cf + 14]) if not re.match(r'([-/])', row[cf + 14]) else None
                        dp.nf = int(row[cf + 15]) if not re.match(r'([-/])', row[cf + 15]) else None
                        dp.nl = int(row[cf + 16]) if not re.match(r'([-/])', row[cf + 16]) else None
                        dp.nm = int(row[cf + 17]) if not re.match(r'([-/])', row[cf + 17]) else None
                        dp.nh = int(row[cf + 18]) if not re.match(r'([-/])', row[cf + 18]) else None
                        dp.pppp = float(row[cf + 19]) if not re.match(r'(-{4}|/{4}|$)', row[cf + 19]) else None
                        dp.t = float(row[cf + 20]) if not re.match(r'(-{3}|/{3}|$)', row[cf + 20]) else None

                        if hours[1] == 1:
                            dp.rr1 = float(row[cf + 10]) * 10 if not re.match(r'(-{2}|/{2}|$)', row[cf + 10]) else None
                            dp.qsw1 = float(row[cf + 21]) * 10 if not re.match(r'(-{3}|/{3}|$)', row[cf + 21]) else None
                            dp.gss1 = float(row[cf + 22]) * 10 if not re.match(r'(-{3}|/{3}|$)', row[cf + 22]) else None
                            dp.qlw1 = float(row[cf + 23]) * 10 if not re.match(r'(-{3}|/{3}|$)', row[cf + 23]) else None
                        if hours[1] == 3:
                            dp.rr3 = float(row[cf + 10]) * 10 if not re.match(r'(-{2}|/{2}|$)', row[cf + 10]) else None
                            dp.qsw3 = float(row[cf + 21]) * 10 if not re.match(r'(-{3}|/{3}|$)', row[cf + 21]) else None
                            dp.gss3 = float(row[cf + 22]) * 10 if not re.match(r'(-{3}|/{3}|$)', row[cf + 22]) else None
                            dp.qlw3 = float(row[cf + 23]) * 10 if not re.match(r'(-{3}|/{3}|$)', row[cf + 23]) else None

                        result_queue.put(dp.to_upsert())
                        if verbose:
                            print('Added forecast for station ' + station_id, end='')
                            print(' on ' + forecast_date.__str__() + ' ' + forecast_time.__str__())
                    except ValueError as err_value:
                        print(err_value)
                        continue
            if verbose:
                print('\nProcessing file ' + match.string + ' finished.')
            os.remove(file[0])
            result = True
        except error.ContentTooShortError as err_content_to_short:
            if verbose:
                print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
        finally:
            result_queue.cancel_join_thread()
    return result


def __process_kml(dates, placemarks, verbose, result_queue):
    """Process forecasts in kml format for saving them to the database

    :param dates: A list of forecast dates
    :param placemarks: A list of placemarks
    :param verbose: Print verbose output
    :param result_queue: The queue for objects to be saved to the database
    :return:
    """
    result = False
    with betterweather.app.app_context():
        try:
            for placemark in placemarks:
                station_id = placemark.find('./kml:name', KML_NS).text
                station_name = placemark.find('./kml:description', KML_NS).text
                coords = placemark.find('./kml:Point/kml:coordinates', KML_NS).text.split(',')
                station_longitude = float(coords[0])
                station_latitude = float(coords[1])
                station_amsl = int(float(coords[2]))

                weather_station = WeatherStation()
                weather_station.id = station_id
                weather_station.name = station_name
                weather_station.latitude = station_latitude
                weather_station.longitude = station_longitude
                weather_station.amsl = station_amsl
                result_queue.put(weather_station.to_upsert())
                if verbose:
                    print('New station ' + station_id + ' added.')
            for placemark in placemarks:
                station_id = placemark.find('./kml:name', KML_NS).text
                values = dict()
                for data in placemark.iterfind('.//dwd:Forecast', KML_NS):
                    key = data.get(
                        '{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}elementName')
                    values[key] = data.find('./dwd:value', KML_NS).text.split()
                for i in range(0, len(dates)):
                    dp = ForecastData()
                    dp.date = dates[i].date()
                    dp.time = dates[i].time()
                    dp.station_id = station_id
                    dp.id = station_id + dp.date.__str__() + dp.time.__str__()
                    dp.tt = float(values['TTT'][i]) * 1.852001 if values.get('TTT', {i: '-'})[i] != '-' else None
                    dp.tg = float(values['T5cm'][i]) * 1.852001 if values.get('T5cm', {i: '-'})[i] != '-' else None
                    dp.td = float(values['Td'][i]) * 1.852001 if values.get('Td', {i: '-'})[i] != '-' else None
                    dp.tx = float(values['TX'][i]) * 1.852001 if values.get('TX', {i: '-'})[i] != '-' else None
                    dp.tn = float(values['TN'][i]) * 1.852001 if values.get('TN', {i: '-'})[i] != '-' else None
                    dp.dd = int(float(values['DD'][i])) if values.get('DD', {i: '-'})[i] != '-' else None
                    dp.ff = float(values['FF'][i]) * (18 / 5) if values.get('FF', {i: '-'})[i] != '-' else None
                    dp.fx = float(values['FX1'][i]) * (18 / 5) if values.get('FX1', {i: '-'})[i] != '-' else None
                    dp.rr1 = float(values['RR1c'][i]) * (18 / 5) if values.get('RR1c', {i: '-'})[i] != '-' else None
                    dp.rr3 = float(values['RR3c'][i]) * (18 / 5) if values.get('RR3c', {i: '-'})[i] != '-' else None
                    dp.ww = int(float(values['ww'][i])) if values.get('ww', {i: '-'})[i] != '-' else None
                    dp.w = int(float(values['W1W2'][i])) if values.get('W1W2', {i: '-'})[i] != '-' else None
                    dp.n = int(float(values['N'][i])) if values.get('N', {i: '-'})[i] != '-' else None
                    dp.nf = int(float(values['Neff'][i])) if values.get('Neff', {i: '-'})[i] != '-' else None
                    dp.nl = int(float(values['Nl'][i])) if values.get('Nl', {i: '-'})[i] != '-' else None
                    dp.nm = int(float(values['Nm'][i])) if values.get('Nm', {i: '-'})[i] != '-' else None
                    dp.nh = int(float(values['Nh'][i])) if values.get('Nh', {i: '-'})[i] != '-' else None
                    dp.pppp = float(values['PPPP'][i]) / 100 if values.get('PPPP', {i: '-'})[i] != '-' else None
                    dp.qsw3 = float(values['RadS3'][i]) if values.get('RadS3', {i: '-'})[i] != '-' else None
                    dp.gss1 = float(values['Rad1h'][i]) if values.get('Rad1h', {i: '-'})[i] != '-' else None
                    dp.qlw3 = float(values['RadL3'][i]) if values.get('RadL3', {i: '-'})[i] != '-' else None
                    dp.vv = int(float(values['VV'][i])) if values.get('VV', {i: '-'})[i] != '-' else None
                    dp.ss1 = float(values['SunD1'][i]) if values.get('SunD1', {i: '-'})[i] != '-' else None
                    dp.fx6 = int(float(values['FXh25'][i])) if values.get('FXh25', {i: '-'})[i] != '-' else None
                    dp.fx9 = int(float(values['FXh40'][i])) if values.get('FXh40', {i: '-'})[i] != '-' else None
                    dp.fx11 = int(float(values['FXh55'][i])) if values.get('FXh55', {i: '-'})[i] != '-' else None
                    dp.rrp6 = int(float(values['R602'][i])) if values.get('R602', {i: '-'})[i] != '-' else None
                    dp.rrp12 = int(float(values['Rh00'][i])) if values.get('Rh00', {i: '-'})[i] != '-' else None
                    dp.rrp24 = int(float(values['Rd02'][i])) if values.get('Rd02', {i: '-'})[i] != '-' else None
                    result_queue.put(dp.to_upsert())
                    if verbose:
                        print('Added forecast for station ' + dp.station_id, end='')
                        print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
            result = True
        except Exception as err:
            print('\nError: ' + err.__str__())
        finally:
            result_queue.cancel_join_thread()
    return result
