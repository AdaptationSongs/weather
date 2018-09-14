# weather
Tools to process, import, and analyze weather data gathered from the biomeridian monitoring stations.

## import-hobo-data.py
Parses temperature and humidity data downloaded from Onset HOBO loggers and imports it into the specified database.

### Requirements
* [SQLAlchemy](https://www.sqlalchemy.org/)

### Usage
`./import-hobo-data.py [database URI] [CSV file]`
* [database URI] - database into which to import the data. Table "hobo_data" will be created if it doesn't exist.
* [CSV file] - file downloaded from the HOBO logger using Onset's Bluetooth app

Example: `./import-hobo-data.py sqlite:///test.db 'SN 20299429 2018-08-05 13_42_24 +0545.csv'`

### Device compatibility
Known to work with:
* [HOBO MX2301](https://www.onsetcomp.com/products/data-loggers/mx2301)
