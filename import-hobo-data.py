#!/usr/bin/python
"""
Tool to parse temperature and humidity data from Onset HOBO loggers.
Output is stored in the specified database.

Arguments: [database URI] [CSV file]

Copyright (c) Damian Christey
License: GPL
"""

import sys
from hobo import HoboCSVReader
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

# Create the database
engine = create_engine(sys.argv[1])
Base.metadata.create_all(engine)

# Create the session
session = sessionmaker()
session.configure(bind=engine)
s = session()

# Read the CSV file
with HoboCSVReader(sys.argv[2]) as reader:
    try:
        t = time()
        imported = 0
        existing = 0
        for row in reader:
            record = hobo_data(**{
                'sn' : reader.sn,
                'timestamp' : row[reader._itimestamp],
                'temp' : row[reader._itemp],
                'rh' : row[reader._irh],
            })
            # search for matching records
            match = s.query(hobo_data).filter(
                hobo_data.sn == record.sn,
                hobo_data.timestamp == record.timestamp
            )
            if (match.count() > 0):
                 # Skip over existing records
                existing += 1
            else:
                 # Add new record to the session
                s.add(record)
                imported += 1

        s.commit() # Attempt to commit all the records

    except Exception as e:
        print('Something bad happened: {0}'.format(e))
        s.rollback() # Rollback the changes on error

    finally:
        s.close() # Close the connection
        print('Imported rows: {0}'.format(imported))
        print('Existing records skipped: {0}'.format(existing))
        print('Time elapsed: {0} seconds'.format(time() - t))
