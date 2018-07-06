from sqlalchemy.orm.session import Session
from sqlalchemy import exc
DB_VERSION = 5


def get_version():
    return DB_VERSION


def initialize_db(db, sqlfile, force=False, verbose=False):
    """

    :param sqlalchemy.orm.session.Session db:
    :param TextIO sqlfile:
    :param force:
    :param verbose:
    :return:
    """
    result = db.execute('SELECT version FROM db_information')
    row = result.fetchone()
    if row:
        print('Database already initialized')
        return False
    db.begin(subtransactions=True)
    try:
        if force:
            db.execute('INSERT INTO db_information VALUES (:name, :version);',
                       {'version': DB_VERSION, 'name': 'BetterWeather'})
        if verbose:
            print("INSERT INTO db_information VALUES ('BetterWeather', " + DB_VERSION.__str__() + ");")
        for code in sqlfile:
            if force:
                db.execute(code)
            if verbose:
                print(code)
        if force:
            db.commit()
        return True
    except exc.DBAPIError as err_dbapi:
        print(err_dbapi)
        return False


def schema_update(db, db_user, force=False, verbose=False):
    """
    Updates the database schema on version updates
    :param Session db: A SQLAlchemy database session
    :param string db_user: The database user
    :param bool force: Execute the query on the session
    :param bool verbose: Dump the sql query to the console
    :return: False, if an error occured, else True
    """
    queries = []
    db.begin(subtransactions=True)
    result = db.execute('SELECT version FROM db_information')
    row = result.fetchone()
    if not row:
        print('Database not initialized yet.')
        return False
    version = row['version']
    if version != DB_VERSION:
        for i in range(version, DB_VERSION):
            if i == 1:
                """ Migrating to DB Version 2"""
                queries.append({
                    'sql': """ALTER TABLE forecast_data ALTER COLUMN id DROP DEFAULT;""",
                    'params': None
                })
                queries.append({
                    'sql': """DROP SEQUENCE forecast_data_id_seq;""",
                    'params': None
                })
                queries.append({
                    'sql': """ALTER TABLE forecast_data ALTER COLUMN id SET DATA TYPE VARCHAR(23);""",
                    'params': None
                })
                queries.append({
                    'sql': """UPDATE forecast_data SET id = CONCAT(station_id, date, time);""",
                    'params': None
                })
            if i == 2:
                """ Migrating to DB Version 3"""
                queries.append({
                    'sql': """ALTER TABLE forecast_data DROP COLUMN id;""",
                    'params': None
                })
                queries.append({
                    'sql': """CREATE SEQUENCE forecast_data_id_seq;""",
                    'params': None
                })
                queries.append({
                    'sql': "ALTER TABLE forecast_data " +
                           "ADD COLUMN id INT PRIMARY KEY DEFAULT nextval('forecast_data_id_seq');",
                    'params': None
                })
                queries.append({
                    'sql': """GRANT USAGE, SELECT ON SEQUENCE forecast_data_id_seq TO """ + db_user + """;""",
                    'params': None
                })
                queries.append({
                    'sql': 'ALTER TABLE forecast_data DROP CONSTRAINT "uq_forecast_data.station_id_date_time";',
                    'params': None
                })
                queries.append({
                    'sql': """ALTER TABLE forecast_data ADD COLUMN issuetime TIMESTAMP;""",
                    'params': None
                })
                queries.append({
                    'sql': "UPDATE forecast_data SET issuetime = " +
                    "to_timestamp(concat(date, time, '00'), 'YYYY-MM-DDHH24:MI:SS');",
                    'params': None
                })
            if i == 3:
                queries.append({
                    'sql': """CREATE INDEX forecast_data_station_id_date ON forecast_data(station_id, date);""",
                    'params': None
                })
            if i == 4:
                queries.append({
                    'sql': """CREATE TABLE historical_data (
    id SERIAL NOT NULL,
    issuetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    date DATE NOT NULL,
    time TIME WITHOUT TIME ZONE NOT NULL,
    t FLOAT,
    tt FLOAT,
    td FLOAT,
    tx FLOAT,
    tn FLOAT,
    tm FLOAT,
    tg FLOAT,
    dd INTEGER,
    ff FLOAT,
    fx FLOAT,
    fx6 INTEGER,
    fx9 INTEGER,
    fx11 INTEGER,
    rr1 FLOAT,
    rr3 FLOAT,
    rr6 FLOAT,
    rr12 FLOAT,
    rr24 FLOAT,
    rrp6 INTEGER,
    rrp12 INTEGER,
    rrp24 INTEGER,
    ev FLOAT,
    ww INTEGER,
    w INTEGER,
    vv INTEGER,
    n INTEGER,
    nf INTEGER,
    nl INTEGER,
    nm INTEGER,
    nh INTEGER,
    pppp FLOAT,
    ss1 FLOAT,
    ss24 FLOAT,
    gss1 FLOAT,
    gss3 FLOAT,
    qsw1 FLOAT,
    qsw3 FLOAT,
    qlw1 FLOAT,
    qlw3 FLOAT,
    station_id VARCHAR(5),
    PRIMARY KEY(id)
);""",
                    'params': None

                })
        queries.append({
            'sql': """UPDATE db_information SET version=:version WHERE name=:name;""",
            'params': {'version': DB_VERSION, 'name': 'BetterWeather'}
        })
    if verbose:
        __print_raw_sql(queries)
    if force:
        try:
            for query in queries:
                db.execute(query['sql'], query['params'])
            db.commit()
            return True
        except exc.DBAPIError as err_dbapi:
            print(err_dbapi)
            db.rollback()
            return False
    db.rollback()
    return True


def __print_raw_sql(queries):
    """
    Print the raw sql queries
    :param Union queries: The sql queries
    :return:
    """
    for query in queries:
        sql = query['sql']
        params = query['params']
        if params:
            for key in params:
                sql = sql.replace(':' + key, __quote(params[key]))
        print(sql)


def __quote(value):
    if value is None:
        return 'NULL'
    if type(value) is str:
        return "'" + value.replace("'", "''") + "'"
    return value.__str__()

