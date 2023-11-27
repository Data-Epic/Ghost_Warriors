CREATE TABLE Weather(
    id SERIAL PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    wind_direction VARCHAR(100) NOT NULL,
    temperature_c INT NOT NULL,
    wind_kph FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    timestamp TIMESTAMP 
)