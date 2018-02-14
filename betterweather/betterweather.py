from flask import Flask, g
import click, re
from urllib import request
from betterweather.db import schema, create_db_connection
from betterweather import stations, models

app = Flask(__name__)
app.config.from_object('betterweather.settings')

if __name__ == "__main__":
    app.run()


def connect_db():
    session = create_db_connection(app.config['DATABASE'])
    return session


def init_db():
    db = get_db()
    models.Base.metadata.create_all(db.get_bind())


def get_db():
    if not hasattr(g, 'db_session'):
        g.db_session = connect_db()
    return g.db_session


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'db_connection'):
        g.db_connection.close()


@app.cli.command('schema_create')
def schema_create_command():
    """Initializes the database"""
    init_db()


@app.cli.command('import_weatherstations')
@click.argument("path_to_file")
def import_staions(path_to_file):
    """Import weather station data from csv"""
    stations.import_from_csv(path_to_file, get_db())


@app.cli.command('get_forecast_data')
def update_forecast():
    try:
        root = request.urlopen(app.config['FORECASTS_URL'])
        links = re.findall(r"(?:href=['\"])([:/.A-z?<_&\s=>0-9;-]+)", root.read().decode('utf-8'))
        for link in links:
            file = request.urlretrieve(app.config['FORECASTS_URL']+link)
            print(file)

    except IOError as e:
        print(e)