import pandas as pd
from flask import Flask, jsonify
from sql_helper import SQLHelper


#################################################
# Flask Setup
app = Flask(__name__)
sqlHelper = SQLHelper()

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/measurementTest<br/>"
        f"/api/v1.0/measurementSQLtest<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
        )

@app.route("/api/v1.0/measurementTest")
def measurementTest():
    # Execute queries
    df = sqlHelper.queryMeasurementORM()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data) 

@app.route("/api/v1.0/measurementSQLtest")
def measurementSQLtest():
    # Execute queries
    df = sqlHelper.queryMeasurementSQL()
    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data) 


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Execute queries
    df = sqlHelper.queryPrecipitation()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data) 

@app.route("/api/v1.0/stations")
def stations():
    # Execute queries
    df = sqlHelper.queryStation()
    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data) 

@app.route("/api/v1.0/tobs")
def tobs():
    # Execute queries
    df = sqlHelper.queryTobs()
    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data) 

@app.route("/api/v1.0/<start>")
def start_route(start):
    # Execute queries
    df = sqlHelper.queryStart(start)
    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data) 

@app.route("/api/v1.0/<start>/<end>")
def start_end_route(start,end):
    # Execute queries
    df = sqlHelper.queryStart_end(start,end)
    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data) 


if __name__ == '__main__':
    app.run(debug=True)
    