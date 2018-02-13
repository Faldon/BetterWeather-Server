DB_VERSION = 1


def get_version():
    return DB_VERSION


def schema_create():
    return """
    create table weather_stations (
        id VARCHAR(5) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        latitude NUMERIC(4, 2) NOT NULL,
        longitude NUMERIC(4, 2) NOT NULL,
        sealevel INTEGER
    );
    """
