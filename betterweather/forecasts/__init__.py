from datetime import datetime
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import joinedload
from betterweather.models import ForecastData


def get_forecast(db, station_id, timestamp, full):
    """Get weather forecast

    Lookup the closest weather forecast of given station for the given time
    :param Session db: A SQLAlchemy database session
    :param str station_id: The station id
    :param float timestamp: The time for the forecast as timestamp
    :param bool full: Join related objects to result
    :return A weather forecast
    :rtype ForecastData or None
    """
    d = datetime.fromtimestamp(timestamp)
    if full:
        q = db.query(ForecastData).options(
            joinedload(ForecastData.station, innerjoin=True)
        ).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date()
        )
    else:
        q = db.query(ForecastData).filter(
            ForecastData.station_id == station_id,
            ForecastData.date == d.date()
        )
    forecasts = []
    for data in q.all():
        forecasts.append(dict(
            forecast=data,
            timediff=abs((d.time().hour * 60 + d.time().minute) - (data.time.hour * 60 + data.time.minute))
        ))
    sorted_data = sorted(forecasts, key=lambda k: k['timediff'])
    if sorted_data:
        return sorted_data[0]['forecast']
    return None
