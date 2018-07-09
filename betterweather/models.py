import json
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from betterweather.db.schema import __quote as quote

Base = declarative_base()


class Entity:
    __tablename__ = ''

    def to_dict(self):
        raise NotImplementedError

    def to_insert(self):
        query = "INSERT INTO " + self.__tablename__ + "(" + ",".join(self.to_dict().keys()) + ") VALUES("
        for value in self.to_dict().values():
            query += quote(value) + ","
        query = query[:-1] + ");"
        return query

    def to_update(self):
        query = "UPDATE " + self.__tablename__ + " SET "
        for key, value in self.to_dict().items():
            if key != "id":
                query += key + "=" + quote(value) + ","
        query = query[:-1] + " WHERE id = " + quote(self.to_dict()['id']) + ";"
        return query

    def to_upsert(self):
        query = "INSERT INTO " + self.__tablename__ + "(" + ",".join(self.to_dict().keys()) + ") SELECT "
        for value in self.to_dict().values():
            query += quote(value) + ","
        query = query[:-1] + " FROM (SELECT 0 as i) AS mutex "
        query += "LEFT JOIN " + self.__tablename__ + " ON id = " + quote(self.to_dict()['id']) + " "
        query += "WHERE i = 0 AND id IS NULL;\n"
        query += self.to_update()
        return query


class DBInformation(Base, Entity):
    __tablename__ = 'db_information'
    name = Column(String(13), primary_key=True)
    version = Column(Integer, nullable=False)

    def to_dict(self):
        return {
            'name': self.name,
            'version': self.version
        }


class WeatherCode(Base, Entity):
    __tablename__ = 'weather_codes'
    id = Column(Integer, primary_key=True)
    precipitation = Column(Boolean, nullable=False)
    key_group_outline = Column(String(255), nullable=False)
    weather_outline = Column(String(255), nullable=False)
    weather_detail_1 = Column(String(255), nullable=True, default=None)
    weather_detail_2 = Column(String(255), nullable=True, default=None)

    def to_json(self):
        return json.dumps({
            'id': self.id,
            'precipitation': self.precipitation,
            'key_group_outline': self.key_group_outline,
            'weather_outline': self.weather_outline,
            'weather_detail_1': self.weather_detail_1,
            'weather_detail_2': self.weather_detail_2
        })

    def to_dict(self):
        return {
            'id': self.id,
            'precipitation': self.precipitation,
            'key_group_outline': self.key_group_outline,
            'weather_outline': self.weather_outline,
            'weather_detail_1': self.weather_detail_1,
            'weather_detail_2': self.weather_detail_2
        }


class WeatherStation(Base, Entity):
    __tablename__ = 'weather_stations'
    id = Column(String(5), primary_key=True, autoincrement=False)
    name = Column(String(255), nullable=False)
    latitude = Column(Numeric(8, 6), nullable=False)
    longitude = Column(Numeric(9, 6), nullable=False)
    amsl = Column(Integer)

    def to_json(self):
        return json.dumps({
            'id': self.id,
            'name': self.name,
            'latitude': float(self.latitude.__str__()),
            'longitude': float(self.longitude.__str__()),
            'amsl': self.amsl
        })

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'latitude': float(self.latitude.__str__()),
            'longitude': float(self.longitude.__str__()),
            'amsl': self.amsl
        }


class ForecastData(Base, Entity):
    __tablename__ = 'forecast_data'
    id = Column(Integer, primary_key=True, autoincrement=False)
    date = Column(Date, nullable=False, comment='Day of forecast')
    time = Column(Time, nullable=False, comment='Time of forecast')
    t = Column(Float, comment='dry bulb temperature at ground in degrees C')
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
    nf = Column(Integer, comment='effective cloud cover in Achtel')
    nl = Column(Integer, comment='cloud cover of low level clouds in Achtel')
    nm = Column(Integer, comment='cloud cover of medium level clouds in Achtel')
    nh = Column(Integer, comment='cloud cover of high level clouds in Achtel')
    pppp = Column(Float, comment='pressure reduced to mean sea level in hPa')
    ss1 = Column(Float, comment='total time of sunshine during last hour in Stunden')
    ss24 = Column(Float, comment='total time of sunshine during past day in Stunden')
    gss1 = Column(Float, comment='global radiation last hour in kJ/qm')
    gss3 = Column(Float, comment='global radiation last 3 hours in kJ/qm')
    qsw1 = Column(Float, comment='short wave radiation last hour in kJ/qm')
    qsw3 = Column(Float, comment='short wave radiation last 3 hours in kJ/qm')
    qlw1 = Column(Float, comment='long wave radiation last hour in kJ/qm')
    qlw3 = Column(Float, comment='long wave radiation last 3 hours in kJ/qm')
    station_id = Column(String(5), ForeignKey('weather_stations.id'))

    station = relationship("WeatherStation", back_populates="forecast_data")

    Index('forecast_data_station_id_date', 'station_id', 'date')

    def to_json(self, full=False):
        """Return the object as JSON"""
        return json.dumps({
            'id': self.id,
            'station_id': self.station_id if not full else self.station.to_dict(),
            'date': self.date.__str__(),
            'time': self.time.__str__(),
            'issuetime': self.issuetime.__str__(),
            't': self.t,
            'tt': self.tt,
            'td': self.td,
            'tx': self.tx,
            'tn': self.tn,
            'tm': self.tm,
            'tg': self.tg,
            'dd': self.dd,
            'ff': self.ff,
            'fx': self.fx,
            'fx6': self.fx6,
            'fx9': self.fx9,
            'fx11': self.fx11,
            'rr1': self.rr1,
            'rr3': self.rr3,
            'rr6': self.rr6,
            'rr12': self.rr12,
            'rr24': self.rr24,
            'rrp6': self.rrp6,
            'rrp12': self.rrp12,
            'rrp24': self.rrp24,
            'ev': self.ev,
            'ww': self.ww,
            'w': self.w,
            'vv': self.vv,
            'n': self.n,
            'nf': self.nf,
            'nl': self.nl,
            'nm': self.nm,
            'nh': self.nh,
            'pppp': self.pppp,
            'ss1': self.ss1,
            'ss24': self.ss24,
            'gss1': self.gss1,
            'gss3': self.gss3,
            'qsw1': self.qsw1,
            'qsw3': self.qsw3,
            'qlw1': self.qlw1,
            'qlw3': self.qlw3
        })

    def to_dict(self, full=False):
        """Convert ForecastData to dict

        :param bool full: Include related objects
        :returns The object as dict
        :rtype dict
        """
        return {
            'id': self.id,
            'station_id': self.station_id if not full else self.station.to_dict(),
            'date': self.date.__str__(),
            'time': self.time.__str__(),
            'issuetime': self.issuetime.__str__(),
            't': self.t,
            'tt': self.tt,
            'td': self.td,
            'tx': self.tx,
            'tn': self.tn,
            'tm': self.tm,
            'tg': self.tg,
            'dd': self.dd,
            'ff': self.ff,
            'fx': self.fx,
            'fx6': self.fx6,
            'fx9': self.fx9,
            'fx11': self.fx11,
            'rr1': self.rr1,
            'rr3': self.rr3,
            'rr6': self.rr6,
            'rr12': self.rr12,
            'rr24': self.rr24,
            'rrp6': self.rrp6,
            'rrp12': self.rrp12,
            'rrp24': self.rrp24,
            'ev': self.ev,
            'ww': self.ww,
            'w': self.w,
            'vv': self.vv,
            'n': self.n,
            'nf': self.nf,
            'nl': self.nl,
            'nm': self.nm,
            'nh': self.nh,
            'pppp': self.pppp,
            'ss1': self.ss1,
            'ss24': self.ss24,
            'gss1': self.gss1,
            'gss3': self.gss3,
            'qsw1': self.qsw1,
            'qsw3': self.qsw3,
            'qlw1': self.qlw1,
            'qlw3': self.qlw3
        }


class HistoricalData(Base):
    __tablename__ = 'historical_data'
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, comment='Day of forecast')
    time = Column(Time, nullable=False, comment='Time of forecast')
    t = Column(Float, comment='dry bulb temperature at ground in degrees C')
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
    nf = Column(Integer, comment='effective cloud cover in Achtel')
    nl = Column(Integer, comment='cloud cover of low level clouds in Achtel')
    nm = Column(Integer, comment='cloud cover of medium level clouds in Achtel')
    nh = Column(Integer, comment='cloud cover of high level clouds in Achtel')
    pppp = Column(Float, comment='pressure reduced to mean sea level in hPa')
    ss1 = Column(Float, comment='total time of sunshine during last hour in Stunden')
    ss24 = Column(Float, comment='total time of sunshine during past day in Stunden')
    gss1 = Column(Float, comment='global radiation last hour in kJ/qm')
    gss3 = Column(Float, comment='global radiation last 3 hours in kJ/qm')
    qsw1 = Column(Float, comment='short wave radiation last hour in kJ/qm')
    qsw3 = Column(Float, comment='short wave radiation last 3 hours in kJ/qm')
    qlw1 = Column(Float, comment='long wave radiation last hour in kJ/qm')
    qlw3 = Column(Float, comment='long wave radiation last 3 hours in kJ/qm')
    station_id = Column(String(5), comment="The station id at forecast time.")

    Index('historical_data_station_id_date', 'station_id', 'date')


WeatherStation.forecast_data = relationship("ForecastData", order_by=ForecastData.id, back_populates="station")
