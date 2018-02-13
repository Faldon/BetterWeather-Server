import sqlite3
from flask import Flask, g
import click
from betterweather.db import schema
from betterweather import stations

app = Flask(__name__)
app.config.from_object('betterweather.settings')

if __name__ == "__main__":
    app.run()


def connect_db():
    db = sqlite3.connect(app.config['DATABASE'])
    db.row_factory = sqlite3.Row
    return db


def init_db():
    db = get_db()
    db.cursor().executescript(schema.schema_create())
    db.commit()


def get_db():
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()


@app.cli.command('schema_create')
def schema_create_command():
    """Initializes the database"""
    init_db()


@app.cli.command('import_weatherstations')
@click.argument("path_to_file")
def import_staions(path_to_file):
    stations.import_from_csv(path_to_file, get_db())