import os
import socket
import click
from flask import Flask, g, jsonify, request, render_template
from datetime import datetime
from betterweather import stations, codes, forecasts


app = Flask(__name__)
app.config.from_object('betterweather.settings')
app.config.from_envvar('BETTERWEATHER_SETTINGS', silent=True)


if __name__ == "__main__":
    app.run()


@app.cli.command('weatherstation_info')
@click.argument('station_id')
def weatherstation_info_command(station_id):
    """Get weather station info for given id"""
    station = stations.get_station(app.config['STATIONS_URL'], station_id)
    print(station if not station else station.to_json())


@app.cli.command('weatherstation_nearest')
@click.argument('latitude')
@click.argument('longitude')
def weatherstation_nearest_command(latitude, longitude):
    """Get nearest weather station for geolocation"""
    print(stations.get_nearest_station(latitude, longitude).to_json())


@app.cli.command('forecastdata_print')
@click.argument('station_id')
@click.option('--forecast_date', help='The time for the forecast formatted %Y-%m-%d %H:%M [default=now]')
@click.option('--full', is_flag=True, help='Also print station information')
def forecastdata_print_command(station_id, forecast_date, full):
    """Print forecast data for weather station"""
    try:
        t = datetime.strptime(forecast_date, '%Y-%m-%d %H:%M').timestamp()
    except ValueError:
        t = datetime.now().timestamp()
    except TypeError:
        t = datetime.now().timestamp()

    forecast = forecasts.get_forecast(None, station_id, t, full)
    print(forecast if not forecast else forecast.to_json(full))


@app.cli.command('weathercode_print')
@click.argument('key_number')
def weathercode_print_command(key_number):
    """Print weather code information"""
    weathercode = codes.get_weathercode(None, key_number)
    print(weathercode if not weathercode else weathercode.to_json())


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
    forecast = forecasts.get_forecast(None, station_id, timestamp, full)
    return jsonify(forecast) if not forecast else jsonify(forecast.to_dict(full))


@app.route('/forecast/location/<float:latitude>/<float:longitude>/', defaults={'timestamp': datetime.now().timestamp()})
@app.route('/forecast/location/<float:latitude>/<float:longitude>/<int:timestamp>')
def get_forecast_by_location(latitude, longitude, timestamp):
    station = stations.get_nearest_station(None, latitude, longitude)
    return get_forecast_by_station(station.id, timestamp)


@app.route('/station/location/<float:latitude>/<float:longitude>')
def get_station_by_location(latitude, longitude):
    return jsonify(stations.get_nearest_station(None, latitude, longitude).to_dict())


@app.route('/station/<station_id>')
def get_station_by_id(station_id):
    station = stations.get_station(station_id)
    return jsonify(station) if not station else jsonify(station.to_dict())


@app.route('/codes/weathercode/<int:key_number>')
def get_weathercode_by_id(key_number):
    weathercode = codes.get_weathercode(None, key_number)
    return jsonify(weathercode) if not weathercode else jsonify(weathercode.to_dict())


@app.route('/')
def show_index():
    return render_template('index.html')
