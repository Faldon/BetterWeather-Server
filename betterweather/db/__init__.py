from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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
    engine = create_engine(uri)
    return engine


def create_db_connection(engine):
    """
    :type engine: sqlalchemy.engine.Engine
    :return sqlachemy.orm.session.Session
    """
    Session = sessionmaker(bind=engine, autocommit=True)
    session = Session()
    return session
