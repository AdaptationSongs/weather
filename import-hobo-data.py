#!/usr/bin/python
"""
Tool to parse temperature and humidity data from Onset HOBO loggers.
Output is stored in the specified database.

Arguments: [database URI] [CSV file]

Copyright (c) Damian Christey
License: GPL
"""

import sys
import csv
from time import time
from datetime import datetime
from sqlalchemy import Column, Integer, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class hobo_data(Base):
    # Set up table and columns
    __tablename__ = 'hobo_data'
    __table_args__ = {'sqlite_autoincrement': True}
    id = Column(Integer, primary_key=True, nullable=False)
    sn = Column(Integer)
    timestamp = Column(DateTime)
    temp = Column(Float)
    rh = Column(Float)
    dewpt = Column(Float)

# Create the database
engine = create_engine(sys.argv[1])
Base.metadata.create_all(engine)

# Create the session
session = sessionmaker()
session.configure(bind=engine)
s = session()

# Read the CSV file
with open(sys.argv[2]) as file:
    reader = csv.reader(file)
    readings = list(reader)

# Get serial number from header row
first_row = readings[0][0]
sn_start = first_row.find(':') + 1
sn_end = first_row.find('-') - 1
sn = first_row[sn_start:sn_end]

try:
    t = time()
    imported = 0
    existing = 0
    # Skip first 3 header rows
    nondata = 3
    for row in readings[3:]:
        # Check that this row has numbers in the right places
        try:
            timestamp = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
            temp = float(row[1])
            rh = float(row[2])
            dewpt = float(row[3])
            record = hobo_data(**{
                'sn' : sn,
                'timestamp' : timestamp,
                'temp' : temp,
                'rh' : rh,
                'dewpt' : dewpt,
            })
            # search for matching records
            match = s.query(hobo_data).filter(
                hobo_data.sn == sn,
                hobo_data.timestamp == timestamp
            )
            if (match.count() > 0):
                 # Skip over existing records
                existing += 1
            else:
                 # Add new record to the session
                s.add(record)
                imported += 1

        # Skip over non-numeric data
        except ValueError:
            nondata += 1

    s.commit() # Attempt to commit all the records

except Exception as e:
    print('Something bad happened: {0}'.format(e))
    s.rollback() # Rollback the changes on error

finally:
    s.close() # Close the connection
    print('Imported rows: {0}'.format(imported))
    print('Nondata rows skipped: {0}'.format(nondata))
    print('Existing records skipped: {0}'.format(existing))
    print('Time elapsed: {0} seconds'.format(time() - t))
