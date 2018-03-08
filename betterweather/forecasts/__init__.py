import os
import re
import csv
import bz2
import zipfile
import multiprocessing
from xml.etree import ElementTree
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
    station_id = ''
    try:
        if verbose:
            print('Retrieving forecast data from ' + root_url)
        root = request.urlopen(root_url)
        links = re.findall(r"(?:href=['\"])([:/.A-z?<_&\s=>0-9;-]+)", root.read().decode('utf-8'))
        db_session.autoflush = False
        db_session.begin(subtransactions=True)
        for link in links:
            station_id = link.__str__()[:5].replace("_", "")
            if verbose:
                print("Processing station " + station_id)
            data = db_session.query(WeatherStation).get(station_id)
            if not data:
                continue
            url = root_url + link
            try:
                file = request.urlretrieve(url)
            except error.ContentTooShortError as err_content_to_short:
                if verbose:
                    print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
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
                        dp = db_session.query(ForecastData).filter(
                            ForecastData.station_id == station_id,
                            ForecastData.date == forecast_date,
                            ForecastData.time == forecast_time
                        ).first()
                        if not dp:
                            dp = ForecastData(
                                date=forecast_date,
                                time=forecast_time,
                                station_id=station_id
                            )
                            db_session.add(dp)
                        dp.tt = float(row[2]) if row[2] != "---" else dp.tt
                        dp.td = float(row[3]) if row[3] != "---" else dp.td
                        dp.tx = float(row[4]) if row[4] != "---" else dp.tx
                        dp.tn = float(row[5]) if row[5] != "---" else dp.tn
                        dp.tm = float(row[6]) if row[6] != "---" else dp.tm
                        dp.tg = float(row[7]) if row[7] != "---" else dp.tg
                        dp.dd = int(row[8]) if row[8] != "---" else dp.dd
                        dp.ff = float(row[9]) if row[9] != "---" else dp.ff
                        dp.fx = float(row[10]) if row[10] != "---" else dp.fx
                        dp.fx6 = int(row[11]) if row[11] != "---" else dp.fx6
                        dp.fx9 = int(row[12]) if row[12] != "---" else dp.fx9
                        dp.fx11 = int(row[13]) if row[13] != "---" else dp.fx11
                        dp.rr1 = float(row[14]) if row[14] != "---" else dp.rr1
                        dp.rr3 = float(row[15]) if row[15] != "---" else dp.rr3
                        dp.rr6 = float(row[16]) if row[16] != "---" else dp.rr6
                        dp.rr12 = float(row[17]) if row[17] != "---" else dp.rr12
                        dp.rr24 = float(row[18]) if row[18] != "---" else dp.rr24
                        dp.rrp6 = int(row[19]) if row[19] != "---" else dp.rrp6
                        dp.rrp12 = int(row[20]) if row[20] != "---" else dp.rrp12
                        dp.rrp24 = int(row[21]) if row[21] != "---" else dp.rrp24
                        dp.ev = float(row[22]) if row[22] != "---" else dp.ev
                        dp.ww = int(row[23]) if row[23] != "---" else dp.ww
                        dp.w = int(row[24]) if row[24] != "---" else dp.w
                        dp.vv = float(row[25]) if row[25] != "---" else dp.vv
                        dp.n = int(row[26]) if row[26] != "---" else dp.n
                        dp.nf = int(row[27]) if row[27] != "---" else dp.nf
                        dp.nl = int(row[28]) if row[28] != "---" else dp.nl
                        dp.nm = int(row[29]) if row[29] != "---" else dp.nm
                        dp.nh = int(row[30]) if row[30] != "---" else dp.nh
                        dp.pppp = float(row[31]) if row[31] != "---" else dp.pppp
                        dp.ss1 = float(row[32]) if row[32] != "---" else dp.ss1
                        dp.ss24 = float(row[33]) if row[33] != "---" else dp.ss24
                        dp.gss1 = float(row[34]) if row[34] != "---" else dp.gss1
                        if verbose:
                            print('Added forecast for station ' + dp.station_id, end='')
                            print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
                    except ValueError as err_value:
                        if verbose:
                            print(err_value)
                        continue
                if verbose:
                    print('\nProcessing data for station ' + station_id + ' finished.')
            os.remove(file[0])
        db_session.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        db_session.rollback()
        print('\nError while processing data for station ' + station_id + ': ' + err_dbapi.__str__())
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
        for link in links:
            if re.search(r'latest', link):
                match = re.search(r'(\d{3}_\d)', link)
                hours = match.group(0).split('_')
                if verbose:
                    print("Processing file " + match.string)
                url = root_url + link
                try:
                    file = request.urlretrieve(url)
                except error.ContentTooShortError as err_content_to_short:
                    if verbose:
                        print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
                    continue
                with bz2.open(file[0], 'rb') as forecast_for_station:
                    data = forecast_for_station.read().decode('latin-1').splitlines()
                    station_data = [e for e in data if re.search(r'\*', e)]
                    weather_forecast_data = [e for e in data if not re.search(r'\*', e)]
                    for row in station_data:
                        station_id = row[0:5].strip()
                        data = db_session.query(WeatherStation).get(station_id)
                        if not data:
                            station_name = ''.join(row)[8:23].strip()
                            props = ''.join(row)[24:].split()
                            lon = props[0][:-2] + '.' + props[0][-2:]
                            lat = props[1][:-2] + '.' + props[1][-2:]
                            station_longitude = float(lon)
                            station_latitude = float(lat)
                            station_amsl = int(props[2])
                            weather_station = WeatherStation(
                                id=station_id,
                                name=station_name,
                                latitude=station_latitude,
                                longitude=station_longitude,
                                amsl=station_amsl
                            )
                            db_session.add(weather_station)
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
                            if station_id != '99999' and row[cf + 1] != '*':
                                dt_object = creation_time + timedelta(hours=int(row[cf + 1]))
                                forecast_date = dt_object.date()
                                forecast_time = dt_object.time()
                                dp = db_session.query(ForecastData).filter(
                                    ForecastData.station_id == station_id,
                                    ForecastData.date == forecast_date,
                                    ForecastData.time == forecast_time
                                ).first()
                                if not dp:
                                    dp = ForecastData(
                                        date=forecast_date,
                                        time=forecast_time,
                                        station_id=station_id
                                    )
                                    db_session.add(dp)
                                dp.tt = float(row[cf + 3]) if not re.match(
                                    r'(-{3}|/{3}|$)', row[cf + 3]) else dp.tt
                                dp.td = float(row[cf + 4]) if not re.match(
                                    r'(-{3}|/{3}|$)', row[cf + 4]) else dp.td
                                dp.tx = float(row[cf + 5]) if not re.match(
                                    r'(-{3}|/{3}|$)', row[cf + 5]) else dp.tx
                                dp.tn = float(row[cf + 6]) if not re.match(
                                    r'(-{3}|/{3}|$)', row[cf + 6]) else dp.tn
                                dp.dd = int(row[cf + 7]) if not re.match(
                                    r'(-{2}|/{2}|$)', row[cf + 7]) else dp.dd
                                dp.ff = float(row[cf + 8]) * 1.852 if not re.match(
                                    r'(-{2}|/{2}|$)', row[cf + 8]) else dp.ff
                                dp.fx = float(row[cf + 9]) * 1.852 if not re.match(
                                    r'(-{2}|/{2}|$)', row[cf + 9]) else dp.fx
                                dp.ww = int(row[cf + 12]) if not re.match(
                                    r'(-{2}|/{2}|$)', row[cf + 12]) else dp.ww
                                dp.w = int(row[cf + 13]) if not re.match(
                                    r'([-/])', row[cf + 13]) else dp.w
                                dp.n = int(row[cf + 14]) if not re.match(
                                    r'([-/])', row[cf + 14]) else dp.n
                                dp.nf = int(row[cf + 15]) if not re.match(
                                    r'([-/])', row[cf + 15]) else dp.nf
                                dp.nl = int(row[cf + 16]) if not re.match(
                                    r'([-/])', row[cf + 16]) else dp.nl
                                dp.nm = int(row[cf + 17]) if not re.match(
                                    r'([-/])', row[cf + 17]) else dp.nm
                                dp.nh = int(row[cf + 18]) if not re.match(
                                    r'([-/])', row[cf + 18]) else dp.nh
                                dp.pppp = float(row[cf + 19]) if not re.match(
                                    r'(-{4}|/{4}|$)', row[cf + 19]) else dp.pppp
                                dp.t = float(row[cf + 20]) if not re.match(
                                    r'(-{3}|/{3}|$)', row[cf + 20]) else dp.t

                                if hours[1] == 1:
                                    dp.rr1 = float(row[cf + 10]) * 10 if not re.match(
                                        r'(-{2}|/{2}|$)', row[cf + 10]) else dp.rr1
                                    dp.qsw1 = float(row[cf + 21]) * 10 if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 21]) else dp.qsw1
                                    dp.gss1 = float(row[cf + 22]) * 10 if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 22]) else dp.gss1
                                    dp.qlw1 = float(row[cf + 23]) * 10 if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 23]) else dp.qlw1
                                if hours[1] == 3:
                                    dp.rr3 = float(row[cf + 10]) * 10 if not re.match(
                                        r'(-{2}|/{2}|$)', row[cf + 10]) else dp.rr3
                                    dp.qsw3 = float(row[cf + 21]) * 10 if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 21]) else dp.qsw3
                                    dp.gss3 = float(row[cf + 22]) * 10 if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 22]) else dp.gss3
                                    dp.qlw3 = float(row[cf + 23]) * 10 if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 23]) else dp.qlw3
                                if verbose:
                                    print('Added forecast for station ' + station_id, end='')
                                    print(' on ' + forecast_date.__str__() + ' ' + forecast_time.__str__())
                        except ValueError as err_value:
                            print(err_value)
                            continue
                if verbose:
                    print('\nProcessing file ' + match.string + ' finished.')
                os.remove(file[0])
        db_session.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        db_session.rollback()
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
    start = datetime.now()
    print(start)
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
        chunk_size = int(len(placemarks) / 4)
        processes = []
        for partition in ([placemarks[i:i+chunk_size] for i in range(0, len(placemarks), chunk_size)]):
            local_process = multiprocessing.Process(target=__process_kml, args=(forecast_dates, partition, verbose))
            local_process.start()
            processes.append(local_process)
        for p in processes:
            if not p == multiprocessing.current_process():
                p.join()

        os.remove(file[0])
        os.remove('/tmp/' + file_name)
        end = datetime.now()
        print(end)
        print(end-start)
        if verbose:
            print('Processing of ' + file_name + ' finished.')
        return True
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


def __process_kml(dates, placemarks, verbose):
    """Process forecasts for writing to the database

    :param dates: A list of forecast dates
    :param placemarks: A list of placemarks
    :param verbose: Print verbose output
    :return:
    """
    with betterweather.app.app_context():
        db_session = betterweather.connect_db()
        db_session.autoflush = False
        db_session.begin(subtransactions=True)
        try:
            for placemark in placemarks:
                station_id = placemark.find('./kml:name', KML_NS).text
                station_name = placemark.find('./kml:description', KML_NS).text
                coords = placemark.find('./kml:Point/kml:coordinates', KML_NS).text.split(',')
                station_longitude = float(coords[0])
                station_latitude = float(coords[1])
                station_amsl = int(float(coords[2]))
                data = db_session.query(WeatherStation).get(station_id)
                if not data:
                    weather_station = WeatherStation(
                        id=station_id,
                        name=station_name,
                        latitude=station_latitude,
                        longitude=station_longitude,
                        amsl=station_amsl
                    )
                    db_session.add(weather_station)
                    if verbose:
                        print('New station ' + station_id + ' added.')
                for data in placemark.iterfind('.//dwd:Forecast', KML_NS):
                    key = data.get(
                        '{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}elementName')
                    values = data.find('./dwd:value', KML_NS).text.split()
                    for i in range(0, len(dates)):
                        dp = db_session.query(ForecastData).filter(
                            ForecastData.station_id == station_id,
                            ForecastData.date == dates[i].date(),
                            ForecastData.time == dates[i].time()
                        ).first()
                        if not dp:
                            dp = ForecastData(
                                date=dates[i].date(),
                                time=dates[i].time(),
                                station_id=station_id
                            )
                            db_session.add(dp)
                        if key == 'TTT':
                            dp.tt = float(values[i]) * 1.852001 if values[i] != '-' else None
                        if key == 'T5cm':
                            dp.tg = float(values[i]) * 1.852001 if values[i] != '-' else None
                        if key == 'Td':
                            dp.td = float(values[i]) * 1.852001 if values[i] != '-' else None
                        if key == 'TX':
                            dp.tx = float(values[i]) * 1.852001 if values[i] != '-' else None
                        if key == 'TN':
                            dp.tn = float(values[i]) * 1.852001 if values[i] != '-' else None
                        if key == 'DD':
                            dp.dd = int(float(values[i])) if values[i] != '-' else None
                        if key == 'FF':
                            dp.ff = float(values[i]) * (18 / 5) if values[i] != '-' else None
                        if key == 'FX1':
                            dp.fx = float(values[i]) * (18 / 5) if values[i] != '-' else None
                        if key == 'RR1c':
                            dp.rr1 = float(values[i]) * (18 / 5) if values[i] != '-' else None
                        if key == 'RR3c':
                            dp.rr3 = float(values[i]) * (18 / 5) if values[i] != '-' else None
                        if key == 'WW':
                            dp.ww = int(float(values[i])) if values[i] != '-' else None
                        if key == 'W1W2':
                            dp.w = int(float(values[i])) if values[i] != '-' else None
                        if key == 'N':
                            dp.n = int(float(values[i])) if values[i] != '-' else None
                        if key == 'Neff':
                            dp.nf = int(float(values[i])) if values[i] != '-' else None
                        if key == 'Nl':
                            dp.nl = int(float(values[i])) if values[i] != '-' else None
                        if key == 'Nm':
                            dp.nm = int(float(values[i])) if values[i] != '-' else None
                        if key == 'Nh':
                            dp.nh = int(float(values[i])) if values[i] != '-' else None
                        if key == 'PPPP':
                            dp.pppp = float(values[i]) / 100 if values[i] != '-' else None
                        if key == 'RadS3':
                            dp.qsw3 = float(values[i]) if values[i] != '-' else None
                        if key == 'Rad1h':
                            dp.gss1 = float(values[i]) if values[i] != '-' else None
                        if key == 'RadL3':
                            dp.qlw3 = float(values[i]) if values[i] != '-' else None
                        if key == 'VV':
                            dp.vv = int(float(values[i])) if values[i] != '-' else None
                        if key == 'SunD1':
                            dp.ss1 = float(values[i]) if values[i] != '-' else None
                        if key == 'FXh25':
                            dp.fx6 = int(float(values[i])) if values[i] != '-' else None
                        if key == 'FXh40':
                            dp.fx9 = int(float(values[i])) if values[i] != '-' else None
                        if key == 'FXh55':
                            dp.fx11 = int(float(values[i])) if values[i] != '-' else None
                        if key == 'R602':
                            dp.rrp6 = int(float(values[i])) if values[i] != '-' else None
                        if key == 'Rh00':
                            dp.rrp12 = int(float(values[i])) if values[i] != '-' else None
                        if key == 'Rd02':
                            dp.rrp24 = int(float(values[i])) if values[i] != '-' else None
                        if verbose:
                            print('Added forecast for station ' + dp.station_id, end='')
                            print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
            db_session.commit()
            return True
        except exc.DBAPIError as err_dbapi:
            db_session.rollback()
            print('\nDB Error: ' + err_dbapi.__str__())
            return False
