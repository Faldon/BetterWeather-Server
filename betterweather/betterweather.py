import click
import json
from flask import Flask, g, jsonify
from datetime import datetime
from sqlalchemy.schema import CreateTable
from betterweather.db import schema, create_db_connection, get_db_engine
from betterweather import stations, models


app = Flask(__name__)
app.config.from_object('betterweather.settings')

if __name__ == "__main__":
    app.run()


def connect_db():
    if not hasattr(g, 'db_engine'):
        g.db_engine = get_db_engine(app.config['DATABASE'])
    session = create_db_connection(g.db_engine)
    return session


def init_db():
    db = get_db()
    models.Base.metadata.create_all(db.get_bind())


def get_db():
    return connect_db()


@app.teardown_appcontext
def close_db(err):
    if hasattr(g, 'db_connection'):
        g.db_connection.close()


@app.cli.command('schema_create')
@click.option('--force', is_flag=True, help='Execute the query to the connected database')
@click.option('--verbose', is_flag=True, help='Dump the sql command to the console')
def schema_create_command(force, verbose):
    """Initializes the database"""
    db = get_db()
    success = schema.schema_create(db, force, verbose)
    if success and force:
        models.Base.metadata.create_all(db.get_bind())
    if verbose:
        for table in models.Base.metadata.tables:
            print(CreateTable(models.Base.metadata.tables.get(table), bind=db.get_bind()))
    if not success:
        print('An error occured. No changes were made to the database.')


@app.cli.command('schema_update')
@click.option('--force', is_flag=True, help='Execute the query to the connected database')
@click.option('--verbose', is_flag=True, help='Dump the sql command to the console')
def schema_update_command(force, verbose):
    """Migrates the database"""
    if not schema.schema_update(get_db(), force, verbose):
        print('An error occured during migration operation.')


@app.cli.command('import_weatherstations')
@click.argument("path_to_file")
@click.option('--file_format', help='The file format to use [default=csv]', type=click.Choice(['csv', 'sql']))
def import_staions(path_to_file, file_format):
    """Import weather station data from csv"""
    if not file_format or file_format == 'csv':
        stations.import_stations_from_csv(path_to_file, get_db())
    else:
        stations.import_stations_from_sql(path_to_file, get_db())


@app.cli.command('get_forecast_data')
@click.option('--file_format', help='The file format to use [default=csv]', type=click.Choice(['csv', 'kml', 'ascii']))
@click.option('--verbose', is_flag=True, help='Dump the sql command to the console')
def update_forecast(file_format, verbose):
    """Get weather forecast from online service"""
    if not file_format or file_format == 'csv':
        return stations.update_mosmix_poi(app.config['FORECASTS_URL_CSV'], get_db(), verbose)
    if file_format == 'ascii':
        return stations.update_mosmix_o_underline(app.config['FORECASTS_URL_ASCII'], get_db(), verbose)
    if file_format == 'kml':
        return stations.update_mosmix_o_underline(app.config['FORECASTS_URL_KML'], get_db(), verbose)


@app.cli.command('print_current_station_forecast')
@click.argument('station_id')
def print_current_station_forecast(station_id):
    t = datetime.now().timestamp()
    forecast = __get_forecast(station_id, t)
    print(forecast.id)


@app.route('/forecast/station/<station_id>/now')
def get_current_station_forecast(station_id):
    t = datetime.now().timestamp()
    return get_station_forecast(station_id, t)


@app.route('/forecast/location/<latitude>/<longitude>/now')
def get_current_location_forecast(latitude, longitude):
    t = datetime.now().timestamp()
    return get_location_forecast(latitude, longitude, t)


@app.route('/forecast/station/<station_id>/<timestamp>')
def get_station_forecast(station_id, timestamp):
    forecast = __get_forecast(station_id, timestamp)


@app.route('/forecast/location/<latitude>/<longitude>/timestamp')
def get_location_forecast(latitude, longitude, timestamp):
    station = stations.get_nearest_station(get_db(), latitude, longitude)
    return get_station_forecast(station.id, timestamp)


def __get_forecast(station_id, timestamp):
    d = datetime.fromtimestamp(timestamp)
    db = get_db()
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
    forecast = sorted(forecasts, key=lambda k: k['timediff'])[0]['forecast']
    return forecast
