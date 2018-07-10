import os
import zipfile
import multiprocessing
from queue import Empty
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


def archive_forecast_data(verbose):
    """Move historical data away from active data

    :param bool verbose: Print verbose output
    :return True on success, False on failure
    :rtype: bool
    """
    today = datetime.today().date().isoformat()
    db_session = betterweather.connect_db()
    db_session.execute("START TRANSACTION;")
    try:
        db_session.execute("""INSERT INTO historical_data SELECT
        id,
        date,
        time,
        t,
        tt,
        td,
        tx,
        tn,
        tm,
        tg,
        dd,
        ff,
        fx,
        fx6,
        fx9,
        fx11,
        rr1,
        rr3,
        rr6,
        rr12,
        rr24,
        rrp6,
        rrp12,
        rrp24,
        ev,
        ww,
        w,
        vv,
        n,
        nf,
        nl,
        nm,
        nh,
        pppp,
        ss1,
        ss24,
        gss1,
        gss3,
        qsw1,
        qsw3,
        qlw1,
        qlw3,
        station_id
        FROM forecast_data WHERE date < '""" + today + "';")
        db_session.execute("DELETE FROM forecast_data WHERE date < '" + today + "';")
        db_session.execute("COMMIT;")
        db_session.flush()
        if verbose:
            print("All forecast data older than " + today + "have been moved to historical data.")
        return True
    except exc.DBAPIError as err_dbapi:
        db_session.execute("ROLLBACK;")
        db_session.flush()
        print('\nDB Error: ' + err_dbapi.__str__())
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
        forecasts = multiprocessing.JoinableQueue()
        stations = multiprocessing.JoinableQueue()
        for partition in ([placemarks[i:i+chunk_size] for i in range(0, len(placemarks), chunk_size)]):
            local_process = multiprocessing.Process(target=__process_kml,
                                                    args=(forecast_dates, partition, verbose, forecasts, stations))
            local_process.start()

        forecast_upserts = []
        station_upserts = []
        while len(multiprocessing.active_children()) > 0:
            if not stations.empty():
                try:
                    sql = stations.get(block=True, timeout=1)
                    station_upserts.append(sql)
                    stations.task_done()
                except Empty:
                    pass
            try:
                sql = forecasts.get(block=True, timeout=1)
                forecast_upserts.append(sql)
                forecasts.task_done()
            except Empty:
                pass
        for upsert in station_upserts:
            db_session.execute(upsert)
        for upsert in forecast_upserts:
            db_session.execute(upsert)
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
        greater = db.query(ForecastData).options(
            joinedload(ForecastData.station, innerjoin=True)
        ).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date(),
            ForecastData.time > d.time()
        ).order_by(
            ForecastData.time.asc(),
            ForecastData.issuetime.desc()
        ).limit(1).first()
        lesser = db.query(ForecastData).options(
            joinedload(ForecastData.station, innerjoin=True)
        ).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date(),
            ForecastData.time <= d.time()
        ).order_by(
            ForecastData.time.desc()
        ).limit(1).first()
    else:
        greater = db.query(ForecastData).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date(),
            ForecastData.time > d.time()
        ).order_by(
            ForecastData.time.asc(),
            ForecastData.issuetime.desc()
        ).limit(1).first()
        lesser = db.query(ForecastData).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date(),
            ForecastData.time <= d.time()
        ).order_by(
            ForecastData.time.desc()
        ).limit(1).first()

    if greater is None and lesser is not None:
        return lesser
    if greater is not None and lesser is None:
        return greater
    if greater is None and lesser is None:
        return None

    greater_timedelta = timedelta(
        hours=greater.time.hour,
        minutes=greater.time.minute,
        seconds=greater.time.second
    )
    lesser_timedelta = timedelta(
        hours=lesser.time.hour,
        minutes=lesser.time.minute,
        seconds=lesser.time.second
    )
    d_timedelta = timedelta(
        hours=d.time().hour,
        minutes=d.time().minute,
        seconds=d.time().second
    )
    if (greater_timedelta - d_timedelta).total_seconds() >= (lesser_timedelta - d_timedelta).total_seconds():
        return lesser
    else:
        return greater


def __process_kml(dates, placemarks, verbose, forecast_results, station_results):
    """Process forecasts in kml format for saving them to the database

    :param dates: A list of forecast dates
    :param placemarks: A list of placemarks
    :param verbose: Print verbose output
    :param forecast_results: The queue for forecasts to be saved to the database
    :param station_results: The queue for stations to be saved to the database
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
                station_results.put(weather_station.to_upsert())
                if verbose:
                    print('New station ' + station_id + ' added.')

                values = dict()
                for data in placemark.iterfind('.//dwd:Forecast', KML_NS):
                    key = data.get(
                        '{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}elementName')
                    values[key] = data.find('./dwd:value', KML_NS).text.split()
                for i in range(0, len(dates)):
                    forecast_id = ""
                    for char in station_id:
                        if char.isdigit():
                            forecast_id += char
                        else:
                            forecast_id += str(ord(char))
                    forecast_id += dates[i].date().__str__().replace("-", "")
                    forecast_id += dates[i].time().__str__().replace(":", "")
                    dp = ForecastData()
                    dp.id = int(forecast_id)
                    dp.date = dates[i].date()
                    dp.time = dates[i].time()
                    dp.station_id = station_id
                    dp.tt = float(values['TTT'][i]) - 273.15 if values.get('TTT', {i: '-'})[i] != '-' else None
                    dp.tg = float(values['T5cm'][i]) - 273.15 if values.get('T5cm', {i: '-'})[i] != '-' else None
                    dp.td = float(values['Td'][i]) - 273.15 if values.get('Td', {i: '-'})[i] != '-' else None
                    dp.tx = float(values['TX'][i]) - 273.15 if values.get('TX', {i: '-'})[i] != '-' else None
                    dp.tn = float(values['TN'][i]) - 273.15 if values.get('TN', {i: '-'})[i] != '-' else None
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
                    forecast_results.put(dp.to_upsert())
                    if verbose:
                        print('Added forecast for station ' + dp.station_id, end='')
                        print(' on ' + dp.date.__str__() + ' ' + dp.time.__str__())
            result = True
        except Exception as err:
            print('\nError: ' + err.__str__())
        finally:
            forecast_results.close()
            station_results.close()
            forecast_results.join_thread()
            station_results.join_thread()
    return result
