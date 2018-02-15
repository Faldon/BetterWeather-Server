import click, re, csv
from flask import Flask, g
from datetime import date, time, datetime
from urllib import request, error
from sqlalchemy import exc, MetaData
from sqlalchemy.schema import CreateTable, Table
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
def close_db(err):
    if hasattr(g, 'db_connection'):
        g.db_connection.close()


@app.cli.command('schema_create')
@click.option('--force', is_flag=True, help='Execute the query to the connected database')
@click.option('--verbose', is_flag=True, help='Dump the sql command to the console')
def schema_create_command(force, verbose):
    """Initializes the database"""
    print('Creating database schema from model data skipping tables already present.')
    db = get_db()
    success = schema.schema_create(db, force, verbose)
    if success and force:
        models.Base.metadata.create_all(db.get_bind())
    if verbose:
        for table in models.Base.metadata.tables:
            print(CreateTable(models.Base.metadata.tables.get(table), bind=db.get_bind()))
    if not success:
        print('An error occured. No changes were made to he database.')


@app.cli.command('schema_update')
@click.option('--force', is_flag=True, help='Execute the query to the connected database')
@click.option('--verbose', is_flag=True, help='Dump the sql command to the console')
def schema_update_command(force, verbose):
    """Migrates the database"""
    print('Migrating the database to version ' + schema.DB_VERSION.__str__())
    if not schema.schema_update(get_db(), force, verbose):
        print('An error occured during migration operation.')


@app.cli.command('import_weatherstations')
@click.argument("path_to_file")
def import_staions(path_to_file):
    """Import weather station data from csv"""
    stations.import_from_csv(path_to_file, get_db())


@app.cli.command('get_forecast_data')
def update_forecast():
    """Get weather forecast from online service"""
    try:
        print('Retrieving forecast data from ' + app.config['FORECASTS_URL'])
        root = request.urlopen(app.config['FORECASTS_URL'])
        links = re.findall(r"(?:href=['\"])([:/.A-z?<_&\s=>0-9;-]+)", root.read().decode('utf-8'))
        total_items = links.__sizeof__()
        db = get_db()
        for link in links:
            station_id = link.__str__()[:5].replace("_", "")
            current_item = links.index(link)
            print("Processing station " + current_item.__str__() + ' of ' + total_items.__str__() + ': '+ station_id)
            url = app.config['FORECASTS_URL'] + link
            try:
                file = request.urlretrieve(url)
            except error.ContentTooShortError as err_content_to_short:
                print("Download of " + url + 'failed: ' + err_content_to_short.__str__())
                continue

            with open(file[0], 'r') as forecast_for_station:
                csv_reader = csv.reader(forecast_for_station, delimiter=";")
                try:
                    db.begin(subtransactions=True)
                    for row in csv_reader:
                        try:
                            dt_object = datetime.strptime(row[0], '%d.%m.%y')
                            forecast_data = models.ForecastData(
                                date=dt_object.date(),
                                time=datetime.strptime(row[1], '%H:%M'),
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
                                w=float(row[24]) if row[24] != "---" else None,
                                vv=float(row[25]) if row[25] != "---" else None,
                                n=float(row[26]) if row[26] != "---" else None,
                                nf=float(row[27]) if row[27] != "---" else None,
                                nl=float(row[28]) if row[28] != "---" else None,
                                nm=int(row[29]) if row[29] != "---" else None,
                                nh=int(row[30]) if row[30] != "---" else None,
                                pppp=int(row[31]) if row[31] != "---" else None,
                                ss1=float(row[32]) if row[32] != "---" else None,
                                ss24=float(row[33]) if row[33] != "---" else None,
                                gss1=int(row[34]) if row[34] != "---" else None,
                                station_id=station_id
                            )
                            db.add(forecast_data)
                            print('.', end='')
                        except ValueError:
                            continue
                    db.commit()
                    print('\nProcessing data for station ' + station_id + ' finished.')
                except exc.DBAPIError as err_dbapi:
                    db.rollback()
                    print('\nError while processing data for station ' + station_id + ': ' + err_dbapi.__str__())
        print("Forecast data successfully retrieved.")
    except error.HTTPError as err_http:
        print('Error while retrieving forecast data: ' + err_http.__str__())
    except IOError as err_io:
        print('Error while retrieving forecast data: ' + err_io.__str__())
