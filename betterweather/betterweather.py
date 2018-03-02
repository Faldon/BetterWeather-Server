import os
import socket
import click
from flask import Flask, g, jsonify, request
from datetime import datetime
from sqlalchemy.schema import CreateTable, MetaData
from sqlalchemy.orm import joinedload
from betterweather.db import schema, create_db_connection, get_db_engine
from betterweather import stations, models


app = Flask(__name__)
app.config.from_object('betterweather.settings')
app.config.from_envvar('BETTERWEATHER_SETTINGS', silent=True)


if __name__ == "__main__":
    app.run()


def connect_db():
    if not hasattr(g, 'db_engine'):
        g.db_engine = get_db_engine(app.config['DATABASE'])
    if not g.db_engine:
        print("No database engine created.")
        exit(1)
    session = create_db_connection(g.db_engine)
    return session


def get_db():
    return connect_db()


@app.teardown_appcontext
def close_db(err):
    if hasattr(g, 'db_engine') and g.db_engine:
        g.db_engine.dispose()


@app.cli.command('schema_create')
@click.option('--sql', is_flag=True, help='Dump the sql command to the console')
def schema_create_command(sql):
    """Create the database schema"""
    db = get_db()
    if sql:
        for table in models.Base.metadata.tables:
            print(CreateTable(models.Base.metadata.tables.get(table), bind=db.get_bind()))
    else:
        models.Base.metadata.create_all(db.get_bind())


@app.cli.command('schema_initialize_db')
@click.option('--sql', is_flag=True, help='Dump the sql command to the console')
@click.option('--force', is_flag=True, help='Force the operation on the connected database')
def schema_initialize_db_command(sql, force):
    """Initialize the database records"""
    db = get_db()
    with app.open_resource('db/db_init.sql', 'r') as sqlfile:
        if not schema.initialize_db(db, sqlfile, force, sql):
            print('There was an error initializing the database.')


@app.cli.command('schema_drop')
@click.option('--sql', is_flag=True, help='Dump the sql command to the console')
@click.option('--force', is_flag=True, help='Force the operation on the connected database')
@click.option('--full', is_flag=True, help='Delete the whole schema of the connected database')
def schema_drop_command(sql, force, full):
    """Drop the database schema"""
    db = get_db()
    if force:
        models.Base.metadata.drop_all(db.get_bind())
    if full:
        full_schema = MetaData.reflect(db.get_bind(), autoload_replace=False)
        full_schema.drop_all(db.get_bind())
    if sql:
        for table in models.Base.metadata.tables:
            print(CreateTable(models.Base.metadata.tables.get(table), bind=db.get_bind()))


@app.cli.command('schema_migrate')
@click.option('--sql', is_flag=True, help='Dump the sql command to the console')
@click.option('--force', is_flag=True, help='Force the operation on the connected database')
def schema_migrate_command(sql, force):
    """Migrate the database version"""
    success = schema.schema_update(get_db(), force, sql)
    if not success and force:
        print('An error occured during migration operation.')


@app.cli.command('weatherstation_import')
@click.argument("path_to_file")
@click.option('--file_format', help='The file format to use [default=csv]', type=click.Choice(['csv', 'sql']))
def weatherstation_import_command(path_to_file, file_format):
    """Import weather station data from file"""
    if not file_format or file_format == 'csv':
        stations.import_stations_from_csv(path_to_file, get_db())
    else:
        stations.import_stations_from_sql(path_to_file, get_db())


@app.cli.command('weatherstation_info')
@click.argument('station_id')
def weatherstation_info_command(station_id):
    """Get weather station info for given id"""
    db = get_db()
    station = db.query(models.WeatherStation).filter(models.WeatherStation.id == station_id).first()
    if not station:
        print(station)
        return
    print(station.to_json())


@app.cli.command('weatherstation_nearest')
@click.argument('latitude')
@click.argument('longitude')
def weatherstation_nearest_command(latitude, longitude):
    """Get nearest weather station for geolocation"""
    print(stations.get_nearest_station(get_db(), latitude, longitude).to_json())


@app.cli.command('forecastdata_retrieve')
@click.option('--file_format', help='The file format to use [default=csv]', type=click.Choice(['csv', 'kml', 'ascii']))
@click.option('--verbose', is_flag=True, help='Dump the sql command to the console')
def forecastdata_retrieve_command(file_format, verbose):
    """Update forecast data from online service"""
    if not file_format or file_format == 'csv':
        return stations.update_mosmix_poi(app.config['FORECASTS_URL_CSV'], get_db(), verbose)
    if file_format == 'ascii':
        return stations.update_mosmix_o_underline(app.config['FORECASTS_URL_ASCII'], get_db(), verbose)
    if file_format == 'kml':
        return stations.update_mosmix_o_underline(app.config['FORECASTS_URL_KML'], get_db(), verbose)


@app.cli.command('forecastdata_print')
@click.argument('station_id')
@click.option('--forecast_date', help='The time for the forecast formatted %Y-%m-%d %H:%M [default=now]')
def forecastdata_print_command(station_id, forecast_date):
    """Print forecast data for weather station"""
    try:
        t = datetime.strptime(forecast_date, '%Y-%m-%d %H:%M').timestamp()
    except ValueError:
        t = datetime.now().timestamp()
    except TypeError:
        t = datetime.now().timestamp()

    forecast = __get_forecast(station_id, t)
    if not forecast:
        print(forecast)
        return
    print(forecast.to_json())


@app.cli.command('config_cronjob')
def config_cronjob_command():
    """Prints the default command to update forecast data as cronjob"""
    print('export BETTERWEATHER_SETTINGS=' + app.root_path + '/production.py;', end='')
    print('cd ' + app.root_path + '/../;', end='')
    print('. venv/bin/activate;', end='')
    print('flask forecastdata_retrieve --file_format=ascii;', end='')
    print('flask forecastdata_retrieve --file_format=csv')


@app.cli.command('config_apache')
@click.option('--server_name', help='The name of your server [default to your hostname]')
def config_apache_command(server_name):
    """Prints the default apache2 config file"""
    if not server_name:
        server_name = socket.getfqdn()
    print("""# Virtual Host config for BetterWeather WSGI Server
# Required modules: mod_wsgi
<VirtualHost *:80>
    ServerName """, end='')
    print(server_name, end='')
    print("""
    WSGIDaemonProcess betterweather threads=15
    WSGIScriptAlias / """, end='')
    print(app.root_path + '/wsgi.py', end='')
    print("""
    <Directory """, end='')
    print(os.path.dirname(os.path.dirname(os.path.abspath(__file__))).__str__() + '>', end='')
    print("""
        WSGIProcessGroup betterweather
        WSGIApplicationGroup %{GLOBAL}
        
        <IfVersion < 2.4>
            Allow from all
            Order allow,deny
        </IfVersion>
        
        <IfVersion >= 2.4>
            Require all granted
        </IfVersion>
    </Directory>
</VirtualHost>""")


@app.route('/forecast/station/<station_id>/', defaults={'timestamp': datetime.now().timestamp()})
@app.route('/forecast/station/<station_id>/<int:timestamp>')
def get_forecast_by_station(station_id, timestamp):
    full = request.args.get('full', default=False)
    forecast = __get_forecast(station_id, timestamp, full)
    if not forecast:
        return jsonify(forecast)
    return jsonify(forecast.to_dict(full))


@app.route('/forecast/location/<float:latitude>/<float:longitude>/', defaults={'timestamp': datetime.now().timestamp()})
@app.route('/forecast/location/<float:latitude>/<float:longitude>/<int:timestamp>')
def get_forecast_by_location(latitude, longitude, timestamp):
    station = stations.get_nearest_station(get_db(), latitude, longitude)
    return get_forecast_by_station(station.id, timestamp)


@app.route('/station/location/<float:latitude>/<float:longitude>')
def get_station_by_location(latitude, longitude):
    return jsonify(stations.get_nearest_station(get_db(), latitude, longitude).to_dict())


@app.route('/station/<station_id>')
def get_station_by_id(station_id):
    db = get_db()
    station = db.query(models.WeatherStation).filter(models.WeatherStation.id == station_id).first()
    if not station:
        return jsonify(station)
    return jsonify(station.to_dict())


def __get_forecast(station_id, timestamp, full):
    """Get weather forecast

    Lookup the closest weather forecast of given station for the given time
    :param str station_id: The station id
    :param float timestamp: The time for the forecast as timestamp
    :param bool full: Join related objects to result
    :return A weather forecast
    :rtype betterweather.models.ForecastData or None
    """
    d = datetime.fromtimestamp(timestamp)
    db = get_db()
    if full:
        q = db.query(models.ForecastData).options(
            joinedload(models.ForecastData.station, innerjoin=True)
        ).filter(
            models.ForecastData.station_id == station_id,
            models.ForecastData.date == d.date()
        )
    else:
        q = db.query(models.ForecastData).filter(
            models.ForecastData.station_id == station_id,
            models.ForecastData.date == d.date()
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
