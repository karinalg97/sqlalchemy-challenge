# Import the dependencies.
from flask import Flask, jsonify

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import numpy as np
import pandas as pd
import datetime as dt


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
# Establish a home route displaying available routes in the API
# as well as formatting instructions for start and start-end routes
@app.route("/")
def home():
    return (
        f"Welcome to the Hawaii Climate API.<br/>"
        f"<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start/&ltstart&gt<br/>"
        f"/api/v1.0/start-end/&ltstart&gt/&ltend&gt<br/><br/>"
        f"Dates for &ltstart&gt and &ltend&gt should be formatted as mm-dd-yyyy or mmddyyyy."
    )

# Create the precipitation route displaying dates and rainfall levels
@app.route("/api/v1.0/precipitation")
def precipitation():

    # Obtain date one year prior to final oberserbed date
    date_query = dt.date(2017, 8, 23) - dt.timedelta(days=365)

    # Query for date and rainfall value filtered by desired date and ordered by date
    year_prec = session.query(measurement.date, measurement.prcp).filter(measurement.date >= date_query).order_by(measurement.date).all()

    session.close()

    # Use list comprehension to replace all null None values with 0.0
    prec_list = [(item[0], 0.0) if item[1] == None else item for item in year_prec]

    # Initialize and iterate through the data list to create an output list of dictionaries
    # Utilize indexing with x variable set as date to obtain multiple recordings for same dates
    prec_dict = {}
    item_list = []
    x = prec_list[0][0]

    for item in prec_list:

        if x == item[0]:

            item_list.append(item[1])

        else:

            prec_dict[x] = item_list
            item_list = []
            item_list.append(item[1])

        x = item[0]

    prec_dict[x] = item_list

    # Return a json of our precipitation, date data at given app route
    return jsonify(prec_dict)

# Create the stations route displaying all stations in the database
@app.route("/api/v1.0/stations")
def stations():

    # Query to obtain all stations in database ordered by most active to least
    station_list = session.query(measurement.station).\
        group_by(measurement.station).order_by(func.count(measurement.station).desc()).all()

    session.close()

    # Return a json of our stations at given app route
    return jsonify(list(np.ravel(station_list)))

# Create the tobs route displaying temperatures and dates for most recent recorded year of most active station
@app.route("/api/v1.0/tobs")
def tobs():

    # Obtain date one year prior to final oberserbed date
    temp_query_date = dt.date(2017, 8, 18) - dt.timedelta(days=365)

    # Query to obtain the most active station in the database
    active_station = session.query(measurement.station).\
        group_by(measurement.station).order_by(func.count(measurement.station).desc()).first()

    # Query to obtain dates and temperatures filtered by desired date and defined active station
    temp_data = session.query(measurement.date, measurement.tobs)\
        .filter(measurement.date >= temp_query_date).filter(measurement.station == active_station[0]).all()

    session.close()

    # Iterate through query and obtain key value pairs for date and temperature and append to empty list
    all_temps = []
    for date, tobs in temp_data:
        temp_dict = {}
        temp_dict["date"] = date
        temp_dict["temperature"] = tobs
        all_temps.append(temp_dict)

    # Return json of our dates and temperatures to the given app route
    return jsonify(all_temps)

# Create the start route that gives temperature statistics on given date range based on start date to end of database
@app.route("/api/v1.0/start/<start>")
def start_date(start):

    # Canonicalize date input to take dashed date format
    canon = start.replace("-", "")

    # Slice date input based on string location and convert to integer type
    mm = int(canon[slice(0, 2)])
    dd = int(canon[slice(2, 4)])
    yyyy = int(canon[slice(4, 8)])

    # Convert slices to dt.date format
    user_start_input = dt.date(yyyy, mm, dd)

    # Query and convert to list displaying all dates in database
    db_check = list(np.ravel(session.query(measurement.date).all()))

    # Reformat and check if user given date occurs in our database
    if user_start_input.strftime("%Y-%m-%d") in db_check:

        # If date present, continue to query min, max, avg temperature based on start to end date of database
        start_search = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
            filter(measurement.date >= user_start_input).all()
        
        session.close()

        # Convert query to key value pairs for visualization in API
        start_results = []
        for stat in start_search:
            start_dict = {}
            start_dict["min"] = stat[0]
            start_dict["max"] = stat[1]
            start_dict["avg"] = stat[2]
            start_results.append(start_dict)

        # Return json of temperature stats based on given start date to end of database to the given app route
        return jsonify(start_results)
    
    # If date is not present in the database, return an error statement
    else:
        
        session.close()

        return jsonify({"error": f"Date: {start} not found in database."}), 404

# Create the start-end route that gives temperature statistics on given date range based on start and end date supplied
@app.route("/api/v1.0/start-end/<start>/<end>")
def start_end_date(start, end):

    # Canonicalize date inputs to take dashed date format
    canon_start = start.replace("-", "")
    canon_end = end.replace("-", "")

    # Slice date inputs based on string location and convert to integer type
    mm_st = int(canon_start[slice(0, 2)])
    dd_st = int(canon_start[slice(2, 4)])
    yyyy_st = int(canon_start[slice(4, 8)])

    mm_ed = int(canon_end[slice(0, 2)])
    dd_ed = int(canon_end[slice(2, 4)])
    yyyy_ed = int(canon_end[slice(4, 8)])

    # Convert slices to dt.date format
    user_start_input = dt.date(yyyy_st, mm_st, dd_st)
    user_end_input = dt.date(yyyy_ed, mm_ed, dd_ed)

    # Query and convert to list displaying all dates in database
    db_check = list(np.ravel(session.query(measurement.date).all()))

    # Reformat and check if user given dates occur in our database and that start date occurs befoere end date
    if (user_start_input.strftime("%Y-%m-%d") and user_end_input.strftime("%Y-%m-%d") in db_check) and (user_end_input > user_start_input):

        # If conditions met, continue to query min, max, avg temperature based on start and end dates given
        dates_search = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)).\
            filter(measurement.date >= user_start_input, measurement.date <= user_end_input).all()
        
        session.close()

        # Convert query to key value pairs for visualization in API
        dates_results = []
        for stat in dates_search:
            dates_dict = {}
            dates_dict["min"] = stat[0]
            dates_dict["max"] = stat[1]
            dates_dict["avg"] = stat[2]
            dates_results.append(dates_dict)

        # Return json of temperature stats based on given start and end dates to the given app route
        return jsonify(dates_results)
    
    # If date is not present in the database, return a specific error statement defining reason for error
    elif user_start_input.strftime("%Y-%m-%d") and user_end_input.strftime("%Y-%m-%d") not in db_check:
        
        session.close()

        return jsonify({"error": f"Date: '{start}' or '{end}' not found in database."}), 404
    
    # If  start date is not before end date, return a specific error statement defining reason for error
    elif user_end_input < user_start_input:

        session.close()

        return jsonify({"error": f"start date '{start}' is later than end date '{end}', null values returned."}), 404

# Run debugger for app
if __name__ == "__main__":
    app.run(debug=True)