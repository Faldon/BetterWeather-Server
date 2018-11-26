# BetterWeather - Server
BetterWeather is a client/server application to get a weather forecast for a given loaction.
This repository covers the server part.

## Operating Mode
The forecast data is retrieved on request from the DWD (Deutscher Wetterdienst) as MOSMIX data. For more information, 
have a look at

- [here](https://www.dwd.de/EN/ourservices/opendata/opendata.html "DWD OpenData")
  for general informaton or 
- [here](https://www.dwd.de/EN/ourservices/met_application_mosmix/met_application_mosmix.html "DWD MOSMIX")
  for information about MOSMIX

The available MOSMIX weather stations are likewise retrieved on request from 
[here](https://www.dwd.de/EN/ourservices/met_application_mosmix/mosmix_stations.cfg?view=nasPublication "DWD MOSMIX station list").

## Technologie
The server backend is written in [Python 3](https://www.python.org/) and built upon the 
[Flask](http://flask.pocoo.org/) microframework.
A website frontend is deployed alongside the server application, which make use of [jQuery](https://jquery.com/),
[Bootstrap](http://getbootstrap.com/) and [Typeahead](https://twitter.github.io/typeahead.js/) (specifically the 
[address picker](https://github.com/komoot/typeahead-address-photon) adopted for [Photon](http://photon.komoot.de/))

A demo can be seen here: <https://weather.pulzer.it><br />
__Note__: Although not very much weather information are displayed here as web elements, on receiving the forecast a json 
object with all available forecast data is dumped in the developer console. 

## Deployment
1. Clone the git repo or download source distribution archive and extract it to a path of our choice, e.g.:
   
   ```bash
   $ tar -xvf /tmp/BetterWeather-1.0.tar.gz -C /var/www/
   ```
       
   The resulting directory reflects the program version you downloaded. If you'd like to have a version agnostic 
   top level directory, create is first and strip off the directory name when extracting:

   ```bash
   $ mkdir /var/www/betterweather
   $ tar -xvf /tmp/BetterWeather-1.0.tar.gz --strip-components=1 -C /var/www/betterweather/
   ```

   __CAVEAT:__ The path to your application must be accessible by your webserver user. Please consult the documentation 
   of your linux distribution and adjust the file system rights to your needs. Also have a look at 
   [Flask](http://flask.pocoo.org/docs/1.0/deploying/ "Flask deploy options") for different types of deploying a flask
   application, for example with nginx.

2. Set up your virtual environment in the path of your application:
   ```bash
   /var/www/betterweather$ python -m venv venv
   ```
   You need to export the flask application as environment variable, so it's recommended to add the export to the 
   virtual environment activation script:
   ```bash
   /var/www/betterweather$ echo "export FLASK_APP=betterweather.py" >> venv/bin/activate   
   ```
   
3. Source your virtual environment and install the application as python package:
    ```bash
    /var/www/betterweather$ . venv/bin/activate
    (venv) /var/www/betterweather$ pip install -e .
    ```

## Command-line usage
The application makes use of the flask command line intertpreter, so you can operate
the server from the shell.

If you run just the command ```flask``` or ```flask --help```
from within your virtual environment, all available commands and their usage will be
listed.

## Using the web frontend
For using Apache as the webserver, you need to install the apache mod_wsgi extension.
You can then print a suitable virtual host configuration from the command-line with
```flask config_apache``` when you sourced your virtual environment, e.g.:

```bash
   $ cd /var/www/betterweather
   $ . venv/bin/activate
   $ flask config_apache
```