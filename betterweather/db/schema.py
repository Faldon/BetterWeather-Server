from sqlalchemy.orm.session import Session
from sqlalchemy import exc
DB_VERSION = 1


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


def schema_update(db, force=False, verbose=False):
    """
    Updates the database schema on version updates
    :param Session db: A SQLAlchemy database session
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
    version = result.fetchone()['version']
    if version != DB_VERSION:
        queries.append({
            'sql': """UPDATE db_information 
    SET version=:version
    WHERE name=:name;""",
            'params': {'version': DB_VERSION, 'name': 'BetterWeather'}
        })
        for i in range(version, DB_VERSION):
            pass
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
        return "'" + value + "'"
    return value.__str__()

