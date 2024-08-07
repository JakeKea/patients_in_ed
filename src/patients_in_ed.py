# Patients in ED Script
# Refer to the README for instructions
# Author: jake.kealey@nhs.net

#Imports
import json
import pandas as pd
import ncl_sqlsnippets as snips
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from os import getenv

#Subtract a specified amount of time from a date
def process_date_window(window, date_end):

    #If a number is given then assume it is in terms of days
    if isinstance(window, int):
        return date_end - timedelta(days=window-1)
    
    #If window is written:
    input_window = window.split(" ")

    #Sanitise input
    if len(input_window) != 2:
        raise Exception(f"The window type {window} is not formatted correctly.")
    
    if input_window[1].endswith('s'):
        input_window[1] = input_window[1][:-1]

    #Process window value to get date_start
    if input_window[1] == "day":
        return date_end - timedelta(days = int(input_window[0]) - 1)
    
    elif input_window[1] == "week":
        return date_end - timedelta(days = (int(input_window[0]) * 7) - 1)
    
    elif input_window[1] == "month":
        #Using months as the window will set the date to the first of that month
        return (date_end.replace(day=1)        
                - relativedelta(months = int(input_window[0])))
    
    elif input_window[1] == "year":
        return date_end - relativedelta(years = int(input_window[0]))
    
    elif input_window[1] == "date":
        return datetime.strftime(input_window[0], "%Y-%m-%d")
    
    else:
        raise Exception(
            f"The window type {window.split(' ')[1]} is not supported.")

#Determine cut_off dates
def derrive_cutoff_date(env):
    #Get starting point based on the DATE_WINDOW runtime value
    query_date_start = process_date_window(
        env["DATE_WINDOW"], datetime.now().date())
    
    ##Get the previous first day of the week so we only deal with complete weeks
    query_week_start = query_date_start - pd.to_timedelta(
        (query_date_start.weekday()) % 7, unit='D')
    
    return query_week_start

#Import runtime settings from the .env file
def import_settings():
    load_dotenv(override=True)

    env = {
        #Window to process
        "DATE_WINDOW": getenv("DATE_WINDOW"),

        #Relevant site codes
        "SITE_CODES": json.loads(getenv("SITE_CODES")),
        "SITE_NAMES": json.loads(getenv("SITE_NAMES")),
        "DEPARTMENT_TYPE_IDS": json.loads(getenv("DEPARTMENT_TYPE_IDS")),
        "DEPARTMENT_TYPE_DESCS": json.loads(getenv("DEPARTMENT_TYPE_DESCS")),

        #SQL Server Details
        "SQL_ADDRESS": getenv("SQL_ADDRESS"),
        "SQL_DATABASE": getenv("SQL_DATABASE"),
        "SQL_SCHEMA": getenv("SQL_SCHEMA"),
        "SQL_TABLE": getenv("SQL_TABLE"),

        "DEBUG_INGEST": {"True": True, "False": False}[getenv("DEBUG_INGEST")]
    }

    env["DATE_CUTOFF"] = derrive_cutoff_date(env)

    return env

#Download the source data from the ecds dataset
def ingest_source_data(env):
    
    #Prepare the ingestion query
    with open('./src/sql/data_ingestion.sql', 'r') as file:
        query_raw = file.read()

    #Add the cut off date to the ingestion query
    query_ingestion = (query_raw + "\n        AND [attendance.arrival.date] >=" 
                       + f"'{env["DATE_CUTOFF"]}'")
    
    #Debug setting to reduce ingestion processing to 10 rows
    if env["DEBUG_INGEST"]:
        query_ingestion = (query_ingestion[:6] 
                           + ' TOP(10) ' + query_ingestion[6:])

    #Run the query
    ##Add error handling?
    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
    return snips.execute_sfw(engine, query_ingestion)

#Split a single ECDS patient row into 1 row per hour spent on sitelean
def hours_in_site(pat):

    #Range for iteration
    start_date = datetime.strptime(
        pat["arrival_date"].split(" ")[0], "%Y-%m-%d")
    start_hour = int(pat['arrival_time'][0:2])
    end_date = datetime.strptime(
        pat["departure_date"].split(" ")[0], "%Y-%m-%d")
    end_hour = int(pat['departure_time'][0:2])

    site = pat['site_code']
    department_type_id = pat['department_type_id']

    #Inititialise for iteration
    current_date = start_date
    current_hour = start_hour

    #Array for new rows
    hours = []

    #Flag to mark the first hour spent as an 'arrival' so I can sum 'arrivals' during aggregation
    arrival_marked = False

    #Iterate each hour between the start and end date
    while ((current_date < end_date) 
           or ((current_date == end_date) and current_hour <= end_hour)):

        if arrival_marked:
            hours.append({
                "date_activity": current_date, 
                "hour": current_hour, 
                "site_code": site, 
                "department_type_id": department_type_id, 
                "count_patients":1, 
                "count_arrivals":0})
        else:
            #If they arrive at the start of the hour, include in snapshot
            if int(pat['arrival_time'][3:5]) == 0 and int(pat['arrival_time'][6:8]) == 0:
                hours.append({
                "date_activity": current_date, 
                "hour": current_hour, 
                "site_code": site, 
                "department_type_id": department_type_id, 
                "count_patients":1, 
                "count_arrivals":1})
            else:
                hours.append({
                    "date_activity": current_date, 
                    "hour": current_hour, 
                    "site_code": site, 
                    "department_type_id": department_type_id, 
                    "count_patients":0, 
                    "count_arrivals":1})
            arrival_marked = True

        #Code to update hour each iteration
        if current_hour == 23:
            current_hour = 0
            current_date += timedelta(days=1)
        else:
            current_hour += 1

    return hours  

#Build the hour table
def build_hour_table(df_ecds):

    # Iterate through the rows using iterrows()
    all_hours = []
    for index, row in df_ecds.iterrows():
        all_hours += hours_in_site(row)

    df_hours = pd.DataFrame(all_hours)

    return df_hours

def aggregate_hour_table(df_hours):
    df_hours_agg = df_hours.groupby(
        ["date_activity", "hour", "site_code", "department_type_id"]).agg("sum")
    return df_hours_agg.reset_index()

#Convert a date into a financial year value (Format: yy-zz)
def date_to_fy(date_target):
   
    year = date_target.year
    month = date_target.month

    if month <= 3:
        return str(year-2001) + "-" + str(year-2000)
    else:
        return str(year-2000) + "-" + str(year-1999)
    
#Convert a date into a financial month value
def date_to_fm(date_target):

    month = date_target.month

    if month <= 3:
        return month + 9
    else:
        return month - 3

#Format the data and produce the populated hour table
def process_ecds_data(df_ecds, env):

    #Build the hours table
    df_hours = build_hour_table(df_ecds)

    #Aggregate the data
    df_hours_agg = aggregate_hour_table(df_hours)

    #Add the year and month columns
    ##Use fin_year format of yy-zz
    df_hours_agg["fin_year"] = df_hours_agg["date_activity"].apply(date_to_fy)
    df_hours_agg["fin_month"] = df_hours_agg["date_activity"].apply(date_to_fm)

    month_map = {
        1: "Apr", 2: "May", 3: "Jun",
        4: "Jul", 5: "Aug", 6: "Sep",
        7: "Oct", 8: "Nov", 9: "Dec",
        10: "Jan", 11: "Feb", 12: "Mar"
    }
    df_hours_agg["month_name"] = df_hours_agg["fin_month"].map(month_map)

    #Add the weekstarting and weekending columns
    df_hours_agg['date_weekstarting'] = (
        df_hours_agg['date_activity'] 
        - pd.to_timedelta(
            (df_hours_agg['date_activity'].dt.dayofweek) % 7, unit='D')
    )
    df_hours_agg['date_weekending'] = (
        df_hours_agg['date_activity'] 
        - pd.to_timedelta(
            (df_hours_agg['date_activity'].dt.dayofweek) % 7 - 6, unit='D')
    )

    #Add shorthand names for the sites
    site_map = {
        "RAL26": "BH",
        "RAPNM": "NMUH",
        "RAL01": "RFH",
        "RRV03": "UCLH",
        "RKEQ4": "WH"
    }
    df_hours_agg["shorthand"] = df_hours_agg["site_code"].map(site_map)

    #Add the department desc
    dep_map = {
        "01": env["DEPARTMENT_TYPE_DESCS"][0],
        "02": env["DEPARTMENT_TYPE_DESCS"][1],
        "03": env["DEPARTMENT_TYPE_DESCS"][2]
    }
    df_hours_agg["department_type_desc"] = (
        df_hours_agg["department_type_id"].map(dep_map).fillna("Unknown"))

    return df_hours_agg

#Function to remove overlapping data and upload new data
def upload_output_data(df_out, env):
    #Connect to the database
    engine = snips.connect("PSFADHSSTP01.AD.ELC.NHS.UK,1460", "Data_Lab_NCL_Dev")

    #Check if the target table exists and create it if not
    if not snips.table_exists(engine, env["SQL_TABLE"], env["SQL_SCHEMA"]):
        #Build create table query
        full_table_path = f"[{env['SQL_DATABASE']}].[{env['SQL_SCHEMA']}].[{env['SQL_TABLE']}]"
        create_query_header = f"CREATE TABLE {full_table_path} (\n"

        with open("./SQL/create_template.sql", "r") as file:
            create_query_base = file.read()

        create_query = create_query_header + create_query_base.split("\n", 1)[1]

        session = snips.execute_query(engine, create_query)

    #Else code to delete overlapping data
    else:
        #Build delete data query
        earliest_week_start = df_out["date_weekstarting"].min()
        delete_query = f"DELETE FROM [{env['SQL_DATABASE']}].[{env['SQL_SCHEMA']}].[{env['SQL_TABLE']}] WHERE date_weekstarting >= '{earliest_week_start}';"

        #Delete old data
        session = snips.execute_query(engine, delete_query)

    #Upload result
    snips.upload_to_sql(df_out, engine, env["SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=150) 

#Import settings
env = import_settings()

#Ingest the source data
print("###### Importing the raw ECDS Data ####################################")
df_ecds = ingest_source_data(env)

#Process the data into the hours table
print("###### Processing the Ingested Data ###################################")
df_out = process_ecds_data(df_ecds, env)

#Upload the processed data
print("###### Uploading the Processed Data ###################################")
upload_output_data(df_out, env)