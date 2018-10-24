import os
import socket
import click
from flask import Flask, jsonify, render_template
from datetime import datetime
from betterweather import stations, forecasts


app = Flask(__name__)
app.config.from_object('betterweather.settings')
app.config.from_envvar('BETTERWEATHER_SETTINGS', silent=True)


if __name__ == "__main__":
    app.run()


@app.cli.command('weatherstation_info')
@click.argument('station_id')
def weatherstation_info_command(station_id):
    """Get weather station info for given id"""
    station = stations.get_station(station_id)
    print(station)


@app.cli.command('weatherstation_nearest')
@click.argument('latitude')
@click.argument('longitude')
def weatherstation_nearest_command(latitude, longitude):
    """Get nearest weather station for geolocation"""
    print(stations.get_nearest_station(latitude, longitude))


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

    forecast = forecasts.get_forecast(station_id, t)
    print(forecast)


@app.cli.command('weathercode_print')
@click.argument('key_number')
def weathercode_print_command(key_number):
    """Print weather code information"""
    print(forecasts.get_present_weather(int(key_number)))


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
    forecast = forecasts.get_forecast(station_id, timestamp)
    if forecast:
        forecast['date']['value'] = forecast['date']['value'].isoformat()
        forecast['time']['value'] = forecast['time']['value'].isoformat()
    return jsonify(forecast)


@app.route('/forecast/location/<float:latitude>/<float:longitude>/', defaults={'timestamp': datetime.now().timestamp()})
@app.route('/forecast/location/<float:latitude>/<float:longitude>/<int:timestamp>')
def get_forecast_by_location(latitude, longitude, timestamp):
    station = stations.get_nearest_station(latitude, longitude)
    return get_forecast_by_station(station.get('id'), timestamp) if station else jsonify(station)


@app.route('/station/location/<float:latitude>/<float:longitude>')
def get_station_by_location(latitude, longitude):
    return jsonify(stations.get_nearest_station(latitude, longitude))


@app.route('/station/<station_id>')
def get_station_by_id(station_id):
    return jsonify(stations.get_station(station_id))


@app.route('/codes/weathercode/<int:key_number>')
def get_weathercode_by_id(key_number):
    return jsonify(forecasts.get_present_weather(key_number))


@app.route('/')
def show_index():
    return render_template('index.html')


@app.route('/daily')
def show_daily_trend():
    return render_template('daily.html')


@app.route('/weekly')
def show_weekly_trend():
    return render_template('weekly.html')
