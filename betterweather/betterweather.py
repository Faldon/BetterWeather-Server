import click
import json
from flask import Flask, g, jsonify
from datetime import datetime
from sqlalchemy.schema import CreateTable, MetaData
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
    """Create the database tables."""
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
    """Initialize the database records."""
    db = get_db()
    with app.open_resource('db/db_init.sql', 'r') as sqlfile:
        if not schema.initialize_db(db, sqlfile, force, sql):
            print('There was an error initializing the database.')


@app.cli.command('schema_drop')
@click.option('--sql', is_flag=True, help='Dump the sql command to the console')
@click.option('--force', is_flag=True, help='Force the operation on the connected database')
@click.option('--full', is_flag=True, help='Delete the whole schema of the connected database')
def schema_drop_command(sql, force, full):
    """Drop the database tables."""
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
    """Migrate the database version."""
    if force and not schema.schema_update(get_db(), force, sql):
        print('An error occured during migration operation.')


@app.cli.command('import_weatherstations')
@click.argument("path_to_file")
@click.option('--file_format', help='The file format to use [default=csv]', type=click.Choice(['csv', 'sql']))
def import_staions_command(path_to_file, file_format):
    """Import weather station data from file."""
    if not file_format or file_format == 'csv':
        stations.import_stations_from_csv(path_to_file, get_db())
    else:
        stations.import_stations_from_sql(path_to_file, get_db())


@app.cli.command('get_forecast_data')
@click.option('--file_format', help='The file format to use [default=csv]', type=click.Choice(['csv', 'kml', 'ascii']))
@click.option('--verbose', is_flag=True, help='Dump the sql command to the console')
def update_forecast_command(file_format, verbose):
    """Get weather forecast from online service."""
    if not file_format or file_format == 'csv':
        return stations.update_mosmix_poi(app.config['FORECASTS_URL_CSV'], get_db(), verbose)
    if file_format == 'ascii':
        return stations.update_mosmix_o_underline(app.config['FORECASTS_URL_ASCII'], get_db(), verbose)
    if file_format == 'kml':
        return stations.update_mosmix_o_underline(app.config['FORECASTS_URL_KML'], get_db(), verbose)


@app.cli.command('print_current_station_forecast')
@click.argument('station_id')
def print_current_station_forecast_command(station_id):
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
