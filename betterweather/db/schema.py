import re
from sqlalchemy.orm.session import Session
from sqlalchemy import exc
DB_VERSION = 2


def get_version():
    return DB_VERSION


def schema_create(db, force=False, verbose=False):
    """
    Creates the database information schema
    :param Session db: A SQLAlchemy database session
    :param bool force: Execute the query on the session
    :param bool verbose: Dump the sql query to the console
    :return: False, if an error occured, else True
    """
    queries = [
        {
            'sql': """CREATE TABLE db_information (
    version INT NOT NULL
);""",
            'params': None
        },
        {
            'sql': """INSERT INTO db_information VALUES (
    :version
);""",
            'params': {'version': DB_VERSION}
        },
    ]

    try:
        db.begin(subtransactions=True)
        if verbose:
            __print_raw_sql(queries)
        for query in queries:
            db.execute(query['sql'], query['params'])
        if force:
            db.commit()
        else:
            db.rollback()
        return True
    except exc.OperationalError as err_operational:
        if re.search("already exists", err_operational.__str__()):
            return True
        else:
            raise err_operational
    except exc.DBAPIError as err_dbapi:
        print(err_dbapi)
        db.rollback()
        return False


def schema_update(db, force=False, verbose=False):
    """
    Updates the database schema on version updates
    :param Session db: A SQLAlchemy database session
    :param bool force: Execute the query on the session
    :param bool verbose: Dump the sql query to the console
    :return: False, if an error occured, else True
    """
    db.begin(subtransactions=True)
    result = db.execute('SELECT version FROM db_information')
    version = result.fetchone()['version']
    queries = []
    try:
        for i in range(version, DB_VERSION):
            if i == 1:
                queries.append({
                    'sql': 'ALTER TABLE forecast_data ADD tn FLOAT;',
                    'params': None
                })
        queries.append({
            'sql': 'UPDATE db_information SET version=:version',
            'params': {'version': DB_VERSION}
        })
        if verbose:
            __print_raw_sql(queries)
        for query in queries:
            db.execute(query['sql'], query['params'])
        if force:
            db.commit()
        else:
            db.rollback()
        return True
    except exc.DBAPIError as err_dbapi:
        print(err_dbapi)
        db.rollback()
        return False


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
                sql = sql.replace(':' + key, params[key].__str__())
        print(sql)
