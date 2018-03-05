from betterweather.models import WeatherCode
from sqlalchemy.orm.session import Session


def get_weathercode(db, key_number):
    """
    Get weather code indormation
    :param Session db: A SQLAlchemy database session
    :param int key_number: The weather code key number
    :return: Weather code information
    :rtype: WeatherCode or None
    """
    return db.query(WeatherCode).get(key_number)