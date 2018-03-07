from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import exc


def get_db_engine(config):
    """
        :type config: dict
        :return sqlachemy.engine.Engine
        """
    uri = config.get('DIALECT') + "://"
    if config.get('USER') and config.get('PASS'):
        uri += config.get('USER') + ":" + config.get("PASS") + "@"
    if config.get('HOST'):
        uri += config.get('HOST')
    if config.get('PORT'):
        uri += ":" + config.get('PORT')

    uri += '/' + config.get('NAME')
    try:
        engine = create_engine(uri)
        return engine
    except ModuleNotFoundError as err_module_not_found:
        print(err_module_not_found)
        return None


def create_db_connection(engine):
    """
    :param sqlalchemy.engine.Engine engine: The sqlalchem engine to use
    :return sqlachemy.orm.session.Session
    """
    Session = scoped_session(sessionmaker(bind=engine, autocommit=True))
    session = Session()
    return session


def import_weathercodes_from_csv(path_to_file, db):
    pass


def import_weathercodes_from_sql(path_to_file, db):
    db.begin(subtransactions=True)
    try:
        for query in open(path_to_file, 'r'):
            db.execute(query)
        db.commit()
        return True
    except exc.DBAPIError:
        db.rollback()
        return False
