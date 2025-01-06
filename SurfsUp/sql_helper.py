
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func

import pandas as pd

# Define the SQLHelper Class
# PURPOSE: Deal with all of the database logic

class SQLHelper():

    # Initialize PARAMETERS/VARIABLES

    #################################################
    # Database Setup
    #################################################

    def __init__(self):
        self.engine = create_engine("sqlite:///../Resources/hawaii.sqlite")
        self.Measurement = self.createMeasurement()
        self.Station = self.createStation()

    # Used for ORM
    def createMeasurement(self):
        # Reflect an existing database into a new model
        Base = automap_base()
        # reflect the tables
        Base.prepare(autoload_with=self.engine)
        # Save reference to the table
        Measurement = Base.classes.measurement
        return(Measurement)

    def queryMeasurementORMTest(self):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)
        """Return a list of measurement data including id, station, date, prcp, tobs"""
        # Query all passengers
        rows = session.query(self.Measurement.id, self.Measurement.station, self.Measurement.date, self.Measurement.prcp, self.Measurement.tobs).all()
        # Create the dataframe
        df = pd.DataFrame(rows, columns=['id', 'station', 'date', 'prcp', 'tobs'])
        # Close the Session
        session.close()
        return(df)        

    def queryMeasurementSQLTest(self):
        # Create our session (link) from Python to the DB
        conn = self.engine.connect() # Raw SQL/Pandas

        # Define Query
        query = text("""SELECT
                        id,
                        station,
                        date,
			            prcp,
			            tobs
                    FROM
                        measurement;""")
        df = pd.read_sql(query, con=conn)
        # Close the connection
        conn.close()
        return(df)

    def queryPrecipitation(self):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)
        precipitation = session.query(self.Measurement.prcp).filter(
        self.Measurement.date >= "2016-08-23", 
        self.Measurement.date <= "2017-08-23"
        ).all()
        # Create the dataframe
        df = pd.DataFrame(precipitation, columns=['prcp'])
        # Close the Session
        session.close()
        return(df)


    def createStation(self):
        # Reflect an existing database into a new model
        Base = automap_base()
        # reflect the tables
        Base.prepare(autoload_with=self.engine)
        # Save reference to the table
        Station = Base.classes.station
        return(Station)

    def queryStation(self):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)
        """Return a list of station data including id, station..."""
        # Query all passengers
        rows = session.query(self.Station.id, self.Station.station, self.Station.name, self.Station.latitude, self.Station.longitude, self.Station.elevation).all()
        # Create the dataframe
        df = pd.DataFrame(rows, columns=['id', 'station', 'name', 'latitude', 'longitude', 'elevation'])
        # Close the Session
        session.close()
        return(df)

    def queryTobs(self):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)
        temperature = session.query(self.Measurement.tobs).filter(
        self.Measurement.date >= "2016-08-23", 
        self.Measurement.date <= "2017-08-23",
        self.Measurement.station == "USC00516128"
        ).all()
        # Create the dataframe
        df = pd.DataFrame(temperature, columns=['tobs'])
        # Close the Session
        session.close()
        return(df)
# Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range.
# For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date.
# For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive.
    def queryStart(self,start):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)
        min_max_avg_start = session.query(func.max(self.Measurement.tobs), func.min(self.Measurement.tobs),func.avg(self.Measurement.tobs)).filter(
        self.Measurement.date >= start
        ).all()
        # Create the dataframe
        df = pd.DataFrame(min_max_avg_start, columns=['TMIN', 'TAVG', 'TMAX'])
        # Close the Session
        session.close()
        return(df)

    def queryStart_end(self, start, end):
        session = Session(self.engine)
        min_max_avg_tobs = session.query(
            func.min(self.Measurement.tobs),
            func.avg(self.Measurement.tobs),
            func.max(self.Measurement.tobs)
        ).filter(self.Measurement.date >= start, self.Measurement.date <= end).all()
        
        # Create the dataframe
        df = pd.DataFrame(min_max_avg_tobs, columns=['TMIN', 'TAVG', 'TMAX'])
        # Close the Session
        session.close()
        return(df)
