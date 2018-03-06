import csv
import re
import os
import bz2
import zipfile
from xml.etree import ElementTree
from sqlalchemy import exc
from urllib import request, error
from datetime import datetime, timedelta
from math import sin, cos, sqrt, atan2, radians
from betterweather.models import WeatherStation, ForecastData, WeatherCode

R = 6373.0
KML_NS = {
    'kml': "http://www.opengis.net/kml/2.2",
    'dwd': "https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd",
    'gx': "http://www.google.com/kml/ext/2.2",
    'xal': "urn:oasis:names:tc:ciq:xsdschema:xAL:2.0",
    'atom': "http://www.w3.org/2005/Atom"
}


def import_stations_from_sql(path_to_file, db):
    db.begin(subtransactions=True)
    try:
        for query in open(path_to_file, 'r'):
            db.execute(query)
        db.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        print(err_dbapi)
        db.rollback()
        return False


def import_stations_from_csv(path_to_file, db):
    """
    Import station data from csv
    :param str path_to_file: The csv file
    :param sqlachemy.orm.session.Session db: A SQLAlchemy database session
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


def update_mosmix_poi(root_url, db, verbose):
    """
    Update weather forecast from mosmix poi csv
    :param str root_url: The link to the directory containing the files
    :param sqlachemy.orm.session.Session db: A SQLAlchemy database session
    :param bool verbose: Print verbose output
    :return:
    """
    station_id = ''
    try:
        if verbose:
            print('Retrieving forecast data from ' + root_url)
        root = request.urlopen(root_url)
        links = re.findall(r"(?:href=['\"])([:/.A-z?<_&\s=>0-9;-]+)", root.read().decode('utf-8'))
        db.begin(subtransactions=True)
        for link in links:
            station_id = link.__str__()[:5].replace("_", "")
            if verbose:
                print("Processing station " + station_id)
            data = db.query(WeatherStation).get(station_id)
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
                        forecast = db.query(ForecastData).filter(
                            ForecastData.station_id == station_id,
                            ForecastData.date == forecast_date,
                            ForecastData.time == forecast_time
                        ).first()
                        if not forecast:
                            dp = ForecastData(
                                date=forecast_date,
                                time=forecast_time,
                                tt=float(row[2]) if row[2] != "---" else None,
                                td=float(row[3]) if row[3] != "---" else None,
                                tx=float(row[4]) if row[4] != "---" else None,
                                tn=float(row[5]) if row[5] != "---" else None,
                                tm=float(row[6]) if row[6] != "---" else None,
                                tg=float(row[7]) if row[7] != "---" else None,
                                dd=int(row[8]) if row[8] != "---" else None,
                                ff=float(row[9]) if row[9] != "---" else None,
                                fx=float(row[10]) if row[10] != "---" else None,
                                fx6=int(row[11]) if row[11] != "---" else None,
                                fx9=int(row[12]) if row[12] != "---" else None,
                                fx11=int(row[13]) if row[13] != "---" else None,
                                rr1=float(row[14]) if row[14] != "---" else None,
                                rr3=float(row[15]) if row[15] != "---" else None,
                                rr6=float(row[16]) if row[16] != "---" else None,
                                rr12=float(row[17]) if row[17] != "---" else None,
                                rr24=float(row[18]) if row[18] != "---" else None,
                                rrp6=int(row[19]) if row[19] != "---" else None,
                                rrp12=int(row[20]) if row[20] != "---" else None,
                                rrp24=int(row[21]) if row[21] != "---" else None,
                                ev=float(row[22]) if row[22] != "---" else None,
                                ww=int(row[23]) if row[23] != "---" else None,
                                w=int(row[24]) if row[24] != "---" else None,
                                vv=float(row[25]) if row[25] != "---" else None,
                                n=int(row[26]) if row[26] != "---" else None,
                                nf=int(row[27]) if row[27] != "---" else None,
                                nl=int(row[28]) if row[28] != "---" else None,
                                nm=int(row[29]) if row[29] != "---" else None,
                                nh=int(row[30]) if row[30] != "---" else None,
                                pppp=float(row[31]) if row[31] != "---" else None,
                                ss1=float(row[32]) if row[32] != "---" else None,
                                ss24=float(row[33]) if row[33] != "---" else None,
                                gss1=float(row[34]) if row[34] != "---" else None,
                                station_id=station_id
                            )
                            db.add(dp)
                            if verbose:
                                print('Added forecast for station ' + dp.station_id, end='')
                                print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
                        else:
                            forecast.tt = float(row[2]) if row[2] != "---" else forecast.tt
                            forecast.td = float(row[3]) if row[3] != "---" else forecast.td
                            forecast.tx = float(row[4]) if row[4] != "---" else forecast.tx
                            forecast.tn = float(row[5]) if row[5] != "---" else forecast.tn
                            forecast.tm = float(row[6]) if row[6] != "---" else forecast.tm
                            forecast.tg = float(row[7]) if row[7] != "---" else forecast.tg
                            forecast.dd = int(row[8]) if row[8] != "---" else forecast.dd
                            forecast.ff = float(row[9]) if row[9] != "---" else forecast.ff
                            forecast.fx = float(row[10]) if row[10] != "---" else forecast.fx
                            forecast.fx6 = int(row[11]) if row[11] != "---" else forecast.fx6
                            forecast.fx9 = int(row[12]) if row[12] != "---" else forecast.fx9
                            forecast.fx11 = int(row[13]) if row[13] != "---" else forecast.fx11
                            forecast.rr1 = float(row[14]) if row[14] != "---" else forecast.rr1
                            forecast.rr3 = float(row[15]) if row[15] != "---" else forecast.rr3
                            forecast.rr6 = float(row[16]) if row[16] != "---" else forecast.rr6
                            forecast.rr12 = float(row[17]) if row[17] != "---" else forecast.rr12
                            forecast.rr24 = float(row[18]) if row[18] != "---" else forecast.rr24
                            forecast.rrp6 = int(row[19]) if row[19] != "---" else forecast.rrp6
                            forecast.rrp12 = int(row[20]) if row[20] != "---" else forecast.rrp12
                            forecast.rrp24 = int(row[21]) if row[21] != "---" else forecast.rrp24
                            forecast.ev = float(row[22]) if row[22] != "---" else forecast.ev
                            forecast.ww = int(row[23]) if row[23] != "---" else forecast.ww
                            forecast.w = int(row[24]) if row[24] != "---" else forecast.w
                            forecast.vv = float(row[25]) if row[25] != "---" else forecast.vv
                            forecast.n = int(row[26]) if row[26] != "---" else forecast.n
                            forecast.nf = int(row[27]) if row[27] != "---" else forecast.nf
                            forecast.nl = int(row[28]) if row[28] != "---" else forecast.nl
                            forecast.nm = int(row[29]) if row[29] != "---" else forecast.nm
                            forecast.nh = int(row[30]) if row[30] != "---" else forecast.nh
                            forecast.pppp = float(row[31]) if row[31] != "---" else forecast.pppp
                            forecast.ss1 = float(row[32]) if row[32] != "---" else forecast.ss1
                            forecast.ss24 = float(row[33]) if row[33] != "---" else forecast.ss24
                            forecast.gss1 = float(row[34]) if row[34] != "---" else forecast.gss1
                            if verbose:
                                print('Updated forecast for station ' + station_id, end='')
                                print(' on ' + forecast_date.__str__() + ' ' + forecast_time.__str__())
                    except ValueError as err_value:
                        if verbose:
                            print(err_value)
                        continue
                if verbose:
                        print('\nProcessing data for station ' + station_id + ' finished.')
            os.remove(file[0])
        db.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        db.rollback()
        print('\nError while processing data for station ' + station_id + ': ' + err_dbapi.__str__())
        return False
    except error.HTTPError as err_http:
        print('Error while retrieving forecast data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('Error while retrieving forecast data: ' + err_io.__str__())
        return False


def update_mosmix_o_underline(root_url, db, verbose):
    """
        Update weather forecast from mosmix o_underline
        :param str root_url: The link to the directory containing the files
        :param sqlachemy.orm.session.Session db: A SQLAlchemy database session
        :param bool verbose: Print verbose output
        :return:
        """
    try:
        if verbose:
            print('Retrieving forecast data from ' + root_url)
        root = request.urlopen(root_url)
        links = re.findall(r"(?:href=['\"])([:/.A-z?<_&\s=>0-9;-]+)", root.read().decode('utf-8'))
        db.begin(subtransactions=True)
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
                        data = db.query(WeatherStation).get(station_id)
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
                            db.add(weather_station)
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
                                forecast = db.query(ForecastData).filter(
                                    ForecastData.station_id == station_id,
                                    ForecastData.date == forecast_date,
                                    ForecastData.time == forecast_time
                                ).first()
                                if not forecast:
                                    dp = ForecastData(
                                        date=forecast_date,
                                        time=forecast_time,
                                        tt=float(row[cf + 3]) if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 3]) else None,
                                        td=float(row[cf + 4]) if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 4]) else None,
                                        tx=float(row[cf + 5]) if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 5]) else None,
                                        tn=float(row[cf + 6]) if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 6]) else None,
                                        dd=int(row[cf + 7]) if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 7]) else None,
                                        ff=float(row[cf + 8]) * 1.852 if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 8]) else None,
                                        fx=float(row[cf + 9]) * 1.852 if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 9]) else None,
                                        ww=int(row[cf + 12]) if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 12]) else None,
                                        w=int(row[cf + 13]) if not re.match(
                                            r'([-/])', row[cf + 13]) else None,
                                        n=int(row[cf + 14]) if not re.match(
                                            r'([-/])', row[cf + 14]) else None,
                                        nf=int(row[cf + 15]) if not re.match(
                                            r'([-/])', row[cf + 15]) else None,
                                        nl=int(row[cf + 16]) if not re.match(
                                            r'([-/])', row[cf + 16]) else None,
                                        nm=int(row[cf + 17]) if not re.match(
                                            r'([-/])', row[cf + 17]) else None,
                                        nh=int(row[cf + 18]) if not re.match(
                                            r'([-/])', row[cf + 18]) else None,
                                        pppp=float(row[cf + 19]) if not re.match(
                                            r'(-{4}|/{4}|$)', row[cf + 19]) else None,
                                        t=float(row[cf + 20]) if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 20]) else None,
                                        station_id=station_id
                                    )
                                    if hours[1] == 1:
                                        dp.rr1 = float(row[cf + 10]) * 10 if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 10]) else None
                                        dp.qsw1 = float(row[cf + 21]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 21]) else None
                                        dp.gss1 = float(row[cf + 22]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 22]) else None
                                        dp.qlw1 = float(row[cf + 23]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 23]) else None
                                    if hours[1] == 3:
                                        dp.rr3 = float(row[cf + 10]) * 10 if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 10]) else None
                                        dp.qsw3 = float(row[cf + 21]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 21]) else None
                                        dp.gss3 = float(row[cf + 22]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 22]) else None
                                        dp.qlw3 = float(row[cf + 23]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 23]) else None
                                    db.add(dp)
                                    if verbose:
                                        print('Added forecast for station ' + dp.station_id, end='')
                                        print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
                                else:
                                    forecast.tt = float(row[cf + 3]) if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 3]) else forecast.tt
                                    forecast.td = float(row[cf + 4]) if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 4]) else forecast.td
                                    forecast.tx = float(row[cf + 5]) if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 5]) else forecast.tx
                                    forecast.tn = float(row[cf + 6]) if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 6]) else forecast.tn
                                    forecast.dd = int(row[cf + 7]) if not re.match(
                                        r'(-{2}|/{2}|$)', row[cf + 7]) else forecast.dd
                                    forecast.ff = float(row[cf + 8]) * 1.852 if not re.match(
                                        r'(-{2}|/{2}|$)', row[cf + 8]) else forecast.ff
                                    forecast.fx = float(row[cf + 9]) * 1.852 if not re.match(
                                        r'(-{2}|/{2}|$)', row[cf + 9]) else forecast.fx
                                    forecast.ww = int(row[cf + 12]) if not re.match(
                                        r'(-{2}|/{2}|$)', row[cf + 12]) else forecast.ww
                                    forecast.w = int(row[cf + 13]) if not re.match(
                                        r'([-/])', row[cf + 13]) else forecast.w
                                    forecast.n = int(row[cf + 14]) if not re.match(
                                        r'([-/])', row[cf + 14]) else forecast.n
                                    forecast.nf = int(row[cf + 15]) if not re.match(
                                        r'([-/])', row[cf + 15]) else forecast.nf
                                    forecast.nl = int(row[cf + 16]) if not re.match(
                                        r'([-/])', row[cf + 16]) else forecast.nl
                                    forecast.nm = int(row[cf + 17]) if not re.match(
                                        r'([-/])', row[cf + 17]) else forecast.nm
                                    forecast.nh = int(row[cf + 18]) if not re.match(
                                        r'([-/])', row[cf + 18]) else forecast.nh
                                    forecast.pppp = float(row[cf + 19]) if not re.match(
                                        r'(-{4}|/{4}|$)', row[cf + 19]) else forecast.pppp
                                    forecast.t = float(row[cf + 20]) if not re.match(
                                        r'(-{3}|/{3}|$)', row[cf + 20]) else forecast.t
                                    
                                    if hours[1] == 1:
                                        forecast.rr1 = float(row[cf + 10]) * 10 if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 10]) else forecast.rr1
                                        forecast.qsw1 = float(row[cf + 21]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 21]) else forecast.qsw1
                                        forecast.gss1 = float(row[cf + 22]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 22]) else forecast.gss1
                                        forecast.qlw1 = float(row[cf + 23]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 23]) else forecast.qlw1
                                    if hours[1] == 3:
                                        forecast.rr3 = float(row[cf + 10]) * 10 if not re.match(
                                            r'(-{2}|/{2}|$)', row[cf + 10]) else forecast.rr3
                                        forecast.qsw3 = float(row[cf + 21]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 21]) else forecast.qsw3
                                        forecast.gss3 = float(row[cf + 22]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 22]) else forecast.gss3
                                        forecast.qlw3 = float(row[cf + 23]) * 10 if not re.match(
                                            r'(-{3}|/{3}|$)', row[cf + 23]) else forecast.qlw3
                                    if verbose:
                                        print('Updated forecast for station ' + station_id, end='')
                                        print(' on ' + forecast_date.__str__() + ' ' + forecast_time.__str__())
                        except ValueError as err_value:
                            print(err_value)
                            continue
                if verbose:
                    print('\nProcessing file ' + match.string + ' finished.')
                os.remove(file[0])
        db.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        db.rollback()
        print('\nDB Error: ' + err_dbapi.__str__())
        return False
    except error.HTTPError as err_http:
        print('HTTP Error while retrieving forecast data: ' + err_http.__str__())
        return False
    except IOError as err_io:
        print('IO Error while retrieving forecast data: ' + err_io.__str__())
        return False


def update_mosmix_kml(root_url, db, verbose):
    url = root_url + 'MOSMIX_S_LATEST_240.kmz'
    if verbose:
        print('Retrieving forecast data from ' + url)
    try:
        db.begin(subtransactions=True)
        file = request.urlretrieve(url)
        zip_handle = zipfile.ZipFile(file[0])
        file_name = zip_handle.filelist[0].filename
        zip_handle.extractall(path='/tmp/')
        forecast_dates = []
        forecast_data = dict()
        kml_root = ElementTree.parse('/tmp/' + file_name)
        for timestep in kml_root.findall('.//dwd:TimeStep', KML_NS):
            forecast_dates.append(datetime.strptime(timestep.text, '%Y-%m-%dT%H:%M:%S.000Z'))
        for placemark in kml_root.findall('.//kml:Placemark', KML_NS):
            print(placemark)
            station_id = placemark.find('./kml:name', KML_NS).text
            station_name = placemark.find('./kml:description', KML_NS).text
            coords = placemark.find('./kml:Point/kml:coordinates', KML_NS).text.split(',')
            station_longitude = coords[0]
            station_latitude = coords[1]
            station_amsl = coords[2]
            print(station_id, station_name, station_longitude, station_latitude, station_amsl)
            data = db.query(WeatherStation).get(station_id)
            if not data:
                weather_station = WeatherStation(
                    id=station_id,
                    name=station_name,
                    latitude=station_latitude,
                    longitude=station_longitude,
                    amsl=station_amsl
                )
                db.add(weather_station)
                if verbose:
                    print('New station ' + station_id + ' added.')
            for data in placemark.findall('.//dwd:Forecast', KML_NS):
                p = data.get('{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}elementName')
                forecast_data.update(p=data.find('./dwd:value', KML_NS).text.split())
            for key in forecast_data.keys():
                if key[0:1] == 'T':
                    values = forecast_data.get(key)
                    for i in range(0, len(values)):
                        forecast_data[key][i] = float(values[i]) * 1.852001 if values[i] != '-' else None
                if key == 'FF' or key == 'FX1':
                    values = forecast_data.get(key)
                    for i in range(0, len(values)):
                        forecast_data[key][i] = float(values[i]) * (18 / 5) if values[i] != '-' else None
                if key[0:2] == 'RR':
                    values = forecast_data.get(key)
                    for i in range(0, len(values)):
                        forecast_data[key][i] = float(values[i]) * (18 / 5) if values[i] != '-' else None
                if key == 'PPPP':
                    values = forecast_data.get(key)
                    for i in range(0, len(values)):
                        forecast_data[key][i] = float(values[i]) / 100 if values[i] != '-' else None
            for i in range(0, len(forecast_dates)):
                forecast = db.query(ForecastData).filter(
                    ForecastData.station_id == station_id,
                    ForecastData.date == forecast_dates[i].date(),
                    ForecastData.time == forecast_dates[i].time()
                ).first()
                if not forecast:
                    dp = ForecastData(
                        date=forecast_dates[i].date(),
                        time=forecast_dates[i].time(),
                        station_id=station_id
                    )
                    for key in forecast_data.keys():
                        if getattr(dp, key, None):
                            dp[key] = forecast_data[key][i] if forecast_data[key][i] != '-' else None
                    db.add(dp)
                    if verbose:
                        print('Added forecast for station ' + dp.station_id, end='')
                        print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
                else:
                    for key in forecast_data.keys():
                        if getattr(forecast, key, None):
                            forecast[key] = forecast_data[key][i] if forecast_data[key][i] != '-' else None
                    if verbose:
                        print('Updated forecast for station ' + station_id, end='')
                        print(' on ' + forecast_dates[i].date().__str__() + ' ' + forecast_dates[i].time().__str__())
        os.remove(file[0])
        os.remove('/tmp/' + file_name)
        if verbose:
            print('Processing of ' + file_name + ' finished.')
        db.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        db.rollback()
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


def get_station(db, station_id):
    """
    Get weather station information
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
    """
    Calculate the distance between two points
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
    a = (sin(dlat/2))**2 + cos(src_latitude) * cos(dst_latitude) * (sin(dlon/2))**2
    c = 2 * atan2(sqrt(a), sqrt((1-a)))
    distance = R * c
    return distance
