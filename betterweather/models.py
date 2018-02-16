from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class WeatherStation(Base):
    __tablename__ = 'weather_stations'
    id = Column(String(5), primary_key=True)
    name = Column(String(255), nullable=False)
    latitude = Column(Numeric(4, 2), nullable=False)
    longitude = Column(Numeric(4, 2), nullable=False)
    amsl = Column(Integer)


class ForecastData(Base):
    __tablename__ = 'forecast_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, comment='Day of forecast')
    time = Column(Time, nullable=False, comment='Time of forecast')
    tt = Column(Float, comment='dry bulb temperature at 2m above ground in degrees C')
    td = Column(Float, comment='dew point temperature at 2m above ground in degrees C')
    tx = Column(Float, comment='maximum of temperature for previous day in degrees C')
    tn = Column(Float, comment='minimum of temperature for previous day in degrees C')
    tm = Column(Float, comment='daily mean of temperature previous day in degrees C')
    tg = Column(Float, comment='minimum temperature at 5cm above ground last 12h in degrees C')
    dd = Column(Integer, comment='mean wind direction during last 10 min at 10m above ground')
    ff = Column(Float, comment='mean wind speed during last 10 min at 10m above ground in km/h')
    fx = Column(Float, comment='maximum wind speed last hour in km/h')
    fx6 = Column(Integer, comment='chance of maximum wind speed last 12 hours more than 45km/h in percent')
    fx9 = Column(Integer, comment='chance of maximum wind speed last 12 hours more than 75km/h in percent')
    fx11 = Column(Integer, comment='chance of maximum wind speed last 12 hours more than 100km/h in percent')
    rr1 = Column(Float, comment='precipitation amount last hour in mm')
    rr3 = Column(Float, comment='precipitation amount last 3 hours in mm')
    rr6 = Column(Float, comment='precipitation amount last 6 hours in mm')
    rr12 = Column(Float, comment='precipitation amount last 12 hours in mm')
    rr24 = Column(Float, comment='precipitation amount last 24 hours in mm')
    rrp6 = Column(Integer, comment='chance of rain past last 6 hours in percent')
    rrp12 = Column(Integer, comment='chance of rain past last 12 hours in percent')
    rrp24 = Column(Integer, comment='chance of rain past last 24 hours in percent')
    ev = Column(Float, comment='potential evapotranspiration last day in kg/qm')
    ww = Column(Integer, comment='present weather as WW Code')
    w = Column(Integer, comment='past weather as WW Code')
    vv = Column(Integer, comment='horizontal visibility in m')
    n = Column(Integer, comment='cloud cover total in Achtel')
    nf = Column(Integer, comment='cloud cover total in Achtel')
    nl = Column(Integer, comment='effective cloud cover in Achtel')
    nm = Column(Integer, comment='cloud cover of medium level clouds in Achtel')
    nh = Column(Integer, comment='cloud cover of high level clouds in Achtel')
    pppp = Column(Float, comment='pressure reduced to mean sea level in hPa')
    ss1 = Column(Float, comment='total time of sunshine during last hour in Stunden')
    ss24 = Column(Float, comment='total time of sunshine during past day in Stunden')
    gss1 = Column(Float, comment='global radiation last hour in kJ/qm')
    station_id = Column(String(5), ForeignKey('weather_stations.id'))

    station = relationship("WeatherStation", back_populates="forecast_data")
    __table_args__ = (UniqueConstraint('station_id', 'date', 'time', name='uq_forecast_data.station_id_date_time'),)


WeatherStation.forecast_data = relationship("ForecastData", order_by=ForecastData.id, back_populates="station")
