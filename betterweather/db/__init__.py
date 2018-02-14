from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def create_db_connection(config):
    """

    :type config: dict
    :return sqlachemy.orm.session.Session
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
    Session = sessionmaker(bind=engine)
    session = Session()

    return session
