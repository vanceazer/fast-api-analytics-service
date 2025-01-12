from fastapi import FastAPI, HTTPException, Query, status, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from database import connect_to_database, vehicle_tracking_collection, initialize_database_and_tables, \
    RABBITMQ_CONNECTION_STRING
from typing import Optional, Literal
from datetime import datetime, timedelta
from models import *
from chart_properties import *
import random
from utils import *
from zoneinfo import ZoneInfo
import pika
import json
import subprocess
import requests

import logging
from otel_grpc import configure_otel_otlp
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
# from opentelemetry.metrics import get_meter

from utils import log_request, increment_request_counter, trace_operation
from dataclasses import asdict
import asyncio
from consumer import start_consuming, start_consuming_violation_scheduler
# import runpy
# from sqlalchemy import text
# from dateutil import tz
# from fastapi.responses import JSONResponse
# from sqlalchemy.sql import func
# import pytz
# import pyodbc

# Configure OpenTelemetry
configure_otel_otlp("eTraffika-data-service", endpoint="http://localhost:4317")


default_year = datetime.now().year
# Initialize FastAPI app
app = FastAPI()

# Instrument FastAPI with OpenTelemetry
FastAPIInstrumentor.instrument_app(app)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize OpenTelemetry Metrics
# meter = get_meter("eTraffika-data-service")
# request_counter = meter.create_counter(
#     name="http_request_count",
#     description="Counts the number of HTTP requests",
#     unit="1"
# )


# Allow all origins
origins = ["*"]

# Adding CORS middleware to allow cross-origin requests from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Global flag to ensure initialization occurs only once
initialized = False




def unflatten(data_list):
    print("Start Unflattening")
    for data in data_list:
        print(type(data))
        print("Device...")
        data["device"] = {}
        data["device"]["name"] = data.pop("device_name")
        data["device"]["type"] = data.pop("device_type")

        print("Location...")
        data["location"] = {}
        data["location"]["longitude"] = data.pop("longitude")
        data["location"]["latitude"] = data.pop("latitude")

        try:
            data["location"]["address"] = data.pop("address")
        except KeyError:
            data["location"]["address"] = "Not Available"

        print("Organisation...")
        data["organisation"] = {}
        data["organisation"]["id"] = data.pop("organisation_id")
        data["organisation"]["name"] = data.pop("organisation_name")

    return data_list




@app.on_event("startup")
async def startup_event():
    global initialized
    if not initialized:
        initialize_database_and_tables()
        # Connect to the database once at startup
        connection = connect_to_database()
        # Start the consumer subprocess
        subprocess.Popen(["python", "consumer.py"])
        initialized = True

# initializing the creation of database and tables
# @app.on_event("startup")
# async def startup_event():
# initialize_database_and_tables()
#
#
#
#
# # connect to the database
# print('Attemptiong to connect to db')


initialize_database_and_tables()


connection = connect_to_database()
#
# # command to start consumer file
# subprocess.Popen(["python", "consumer.py"])
# # with open('consumer.py') as f:
# #     code = f.read()
# #     exec(code)
# # runpy.run_path('consumer.py')

# @app.on_event("shutdown")
# async def shutdown():
#     await database.disconnect()
# asyncio.create_task(start_consuming())
# asyncio.create_task(start_consuming_violation_scheduler())



def send_to_queue(queue_name, message):
    connection_params = pika.URLParameters(RABBITMQ_CONNECTION_STRING)
    print('Receiving message queue')
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    connection.close()


# function to create a random date for data seeding
# def generate_random_utc_date():
#     # Get the current UTC datetime
#     current_utc_datetime = datetime.utcnow()
#
#     # Set the start date to January 2023c
#     start_date = datetime(2018, 1, 1)
#
#     # Calculate the number of days between the start date and today
#     days_difference = (current_utc_datetime - start_date).days
#
#     # Generate a random number of days within the range
#     random_days = random.randint(0, days_difference)
#
#     # Calculate the random date
#     random_date = start_date + timedelta(days=random_days)
#
#     return random_date




# function to pass in organisation id as headers
def get_OrganisationId(OrganisationId: int = Header(...)):
    return OrganisationId




# BEGINNING OF CRUD OPERATIONS

# post request to create raw location data
@app.post("/create-raw-location-data", tags=["Device"], status_code=status.HTTP_201_CREATED)
# @trace_operation("create_raw_location_data_operation")
async def create_raw_location_data(item: Instance):
    endpoint = "/create-raw-location-data"
    method = "POST"

    try:
        # Prepare data for processing
        production_date = datetime.now()
        item_dict = item.dict()
        item_dict['createdAt'] = production_date.isoformat()



        # Process the message using the utilities
        await process_message(item_dict)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message= "Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Instance log Data processed and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message= "Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))


# get request to fetch raw location data along with queried parameters
@app.get("/raw-location-data", tags=["Device"])
async def get_all_items(
    page: int = Query(1, gt=0),
    page_size: int = Query(25, gt=0),
    organisation_id: int = None,
    organisation_name: str = None,
    device_name: str = None,
    device_type: str = None,
    latitude: str = None,
    longitude: str = None,
    address: str = None,
    vehicle_id: str = None,
    vehicle_regnumber: str = None,
    make: str = None,
    model: str = None,
    color: str = None,
    istaxi: bool = None,
    isanonymous: bool = None,
    hasviolation: bool = None,
    state: str = None,
    created_at_start: Optional[str] = None,
    created_at_end: Optional[str] = None,
):
    conditions = []
    values = []

    if organisation_id is not None:
        conditions.append("organisation_id = ?")
        values.append(organisation_id)
    if organisation_name is not None:
        conditions.append("organisation_name = ?")
        values.append(organisation_name)
    if device_name is not None:
        conditions.append("device_name = ?")
        values.append(device_name)
    if device_type is not None:
        conditions.append("device_type = ?")
        values.append(device_type)
    if latitude is not None:
        conditions.append("latitude = ?")
        values.append(latitude)
    if longitude is not None:
        conditions.append("longitude = ?")
        values.append(longitude)
    if address is not None:
        conditions.append("address = ?")
        values.append(address)
    if vehicle_id is not None:
        conditions.append("vehicle_id = ?")
        values.append(vehicle_id)
    if vehicle_regnumber is not None:
        conditions.append("vehicle_regnumber = ?")
        values.append(vehicle_regnumber)
    if make is not None:
        conditions.append("make = ?")
        values.append(make)
    if model is not None:
        conditions.append("model = ?")
        values.append(model)
    if color is not None:
        conditions.append("color = ?")
        values.append(color)
    if istaxi is not None:
        conditions.append("istaxi = ?")
        values.append(istaxi)
    if isanonymous is not None:
        conditions.append("isanonymous = ?")
        values.append(isanonymous)
    if hasviolation is not None:
        conditions.append("hasviolation = ?")
        values.append(hasviolation)
    if state is not None:
        conditions.append("state = ?")
        values.append(state)
    if created_at_start is not None:
        datetime_format = "%Y-%m-%dT%H:%M"
        try:
            datetime_start = datetime.strptime(created_at_start, datetime_format)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format provided")
        conditions.append("createdat >= ?")
        values.append(datetime_start)
    if created_at_end is not None:
        datetime_format = "%Y-%m-%dT%H:%M"
        try:
            datetime_end = datetime.strptime(created_at_end, datetime_format)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format provided")
        conditions.append("createdat <= ?")
        values.append(datetime_end)

    conditions_str = " AND ".join(conditions)
    where_clause = f"WHERE {conditions_str}" if conditions else ""

    offset = (page - 1) * page_size

    query = f"""
            SELECT * FROM dbo.instancelograwdata
            {where_clause}
            ORDER BY createdat DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
    values.extend([offset, page_size])

    try:
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            result = {
                "image_url": row.image_url,
                "organisation_id": row.organisation_id,
                "organisation_name": row.organisation_name,
                "device_name": row.device_name,
                "device_type": row.device_type,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "address": row.address,
                "vehicle_id": row.vehicle_id,
                "vehicle_regnumber": row.vehicle_regnumber,
                "make": row.make,
                "model": row.model,
                "color": row.color,
                "istaxi": row.istaxi,
                "isanonymous": row.isanonymous,
                "hasviolation": row.hasviolation,
                "state": row.state,
                "createdat": row.createdat
            }
            results.append(result)

        total_count_query = f"""
                SELECT COUNT(*) FROM dbo.instancelograwdata
                {where_clause}
            """
        cursor.execute(total_count_query, values[:-2])
        total_count = cursor.fetchone()[0]

        return {
            "data": results,
            "page": page,
            "page_size": page_size,
            "total_count": total_count,
        }
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
#
#
#

# function to get device history data
@app.get("/device/device-history-data", tags=["Device"])
async def get_device_history_data(
    OrganisationId: int = Depends(get_OrganisationId),
    device_name: Optional[str] = None,
    vehicle_id: Optional[str] = None,
    vehicle_regnumber: str = None,
    make: Optional[str] = None,
    model: Optional[str] = None,
    color: Optional[str] = None,
    istaxi: Optional[bool] = None,
    state: Optional[str] = None,
    created_at_start: Optional[str] = None,
    created_at_end: Optional[str] = None,
    top_limit: Optional[int] = 20
):
    try:
        # Log request details
        log_request(
            endpoint="/device/device-history-data",
            method="GET",
            status="Processing",
            message="Received request for device history data",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "make": make,
                "model": model,
                "color": color,
                "istaxi": istaxi,
                "state": state,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit
            },
        )

        # Generate filter conditions
        conditions = []
        values = []

        # Apply OrganisationId filter if not 13
        if OrganisationId != 13:
            conditions.append("organisationid = ?")
            values.append(OrganisationId)

        if device_name is not None:
            conditions.append("devicename = ?")
            values.append(device_name)
        if vehicle_id is not None:
            conditions.append("vehicleId = ?")
            values.append(vehicle_id)
        if vehicle_regnumber is not None:
            conditions.append("regnumber = ?")
            values.append(vehicle_regnumber)
        if make is not None:
            conditions.append("make = ?")
            values.append(make)
        if model is not None:
            conditions.append("model = ?")
            values.append(model)
        if color is not None:
            conditions.append("color = ?")
            values.append(color)
        if istaxi is not None:
            conditions.append("istaxi = ?")
            values.append(istaxi)
        if state is not None:
            conditions.append("state = ?")
            values.append(state)

        if created_at_start is not None:
            datetime_format = "%Y-%m-%dT%H:%M"
            try:
                datetime_start = datetime.strptime(created_at_start, datetime_format)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format provided")
            conditions.append("createdat >= ?")
            values.append(datetime_start)
        if created_at_end is not None:
            datetime_format = "%Y-%m-%dT%H:%M"
            try:
                datetime_end = datetime.strptime(created_at_end, datetime_format)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format provided")
            conditions.append("createdat <= ?")
            values.append(datetime_end)

        conditions_str = " AND ".join(conditions)
        where_clause = f"WHERE {conditions_str}" if conditions else ""

        if top_limit > 0:
            top_clause = f"TOP {top_limit}"
        else:
            top_clause = ""  # No limit if top_limit is 0

        # Construct query to get device history data
        query = f"""
                SELECT {top_clause}devicename,
                       COUNT(*) AS vehicle_count
                FROM dbo.devicehistory
                {where_clause}
                GROUP BY devicename
                ORDER BY COUNT(*) DESC
            """

        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        total_vehicle_count = sum(row.vehicle_count for row in rows)

        # Format the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Captured by Device",
                "subCaption": "Vehicle Count by Device",
                "xAxisName": "Device",
                "yAxisName": "Number of Vehicles Captured",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollcolumn2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.devicename)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": row.vehicle_count
            })

        # Log success and increment metrics
        log_request(
            endpoint="/device/device-history-data",
            method="GET",
            status="200",
            message="Successfully processed device history data request",
        )
        increment_request_counter(
            endpoint="/device/device-history-data",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /device/device-history-data: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/device/device-history-data",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )


@app.get("/device/vehicle-count-drill-down", tags=["Device"])
async def get_vehicle_count(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        day: Optional[int] = None,
        month: Optional[str] = None,
        make: Optional[str] = None,
        model: Optional[str] = None,
        color: Optional[str] = None,
        istaxi: Optional[bool] = None,
        state: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day", "hour"]] = "year",
        chart_type: Optional[Literal["line", "column"]] = "line"
):
    try:
        # Log request details
        log_request(
            endpoint="/device/vehicle-count-drill-down",
            method="GET",
            status="Processing",
            message="Received request for vehicle count drill-down",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "year": year,
                "quarter": quarter,
                "day": day,
                "month": month,
                "make": make,
                "model": model,
                "color": color,
                "istaxi": istaxi,
                "state": state,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
                "chart_type": chart_type
            },
        )

        # Generate filter conditions
        conditions = []
        values = []
        column_mapping = {
            "OrganisationId": "organisationid"
        }

        # Apply OrganisationId filter if not 13
        organisation_column = column_mapping["OrganisationId"]
        if OrganisationId != 13:
            conditions.append(f"{organisation_column} = ?")
            values.append(OrganisationId)

        if device_name is not None:
            conditions.append("devicename = ?")
            values.append(device_name)
        if vehicle_id is not None:
            conditions.append("vehicleId = ?")
            values.append(vehicle_id)
        if vehicle_regnumber is not None:
            conditions.append("regnumber = ?")
            values.append(vehicle_regnumber)
        if year is not None:
            conditions.append("year = ?")
            values.append(year)
        if quarter is not None:
            conditions.append("quarter = ?")
            values.append(quarter)
        if month is not None:
            conditions.append("month = ?")
            values.append(month)
        if day is not None:
            conditions.append("day = ?")
            values.append(day)
        if make is not None:
            conditions.append("make = ?")
            values.append(make)
        if model is not None:
            conditions.append("model = ?")
            values.append(model)
        if color is not None:
            conditions.append("color = ?")
            values.append(color)
        if istaxi is not None:
            conditions.append("istaxi = ?")
            values.append(istaxi)
        if state is not None:
            conditions.append("state = ?")
            values.append(state)

        if created_at_start is not None:
            datetime_format = "%Y-%m-%dT%H:%M"
            try:
                datetime_start = datetime.strptime(created_at_start, datetime_format)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format provided")
            conditions.append("createdat >= ?")
            values.append(datetime_start)
        if created_at_end is not None:
            datetime_format = "%Y-%m-%dT%H:%M"
            try:
                datetime_end = datetime.strptime(created_at_end, datetime_format)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid datetime format provided")
            conditions.append("createdat <= ?")
            values.append(datetime_end)

        if drill_down == "hour" and created_at_start is None and created_at_end is None and year is None and month is None and day is None:
            today = datetime.today()
            conditions.append("month = ?")
            values.append(today.strftime("%B"))
            conditions.append("day = ?")
            values.append(today.day)

        if drill_down == "day" and created_at_start is None and created_at_end is None and year is None and month is None:
            today = datetime.today()
            conditions.append("year = ?")
            values.append(today.year)
            conditions.append("month = ?")
            values.append(today.strftime("%B"))

        if (created_at_start is None) and (year is None) and (drill_down != "year"):
            conditions.append("year = ?")
            values.append(default_year)

        conditions_str = " AND ".join(conditions)
        where_clause = f"WHERE {conditions_str}" if conditions else ""

        # Construct query based on drill-down level
        query = select_vehicle_count_query(drill_down, where_clause)

        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Prepare chart data based on requested chart type
        if chart_type == "line":
            fusioncharts_data = select_vehicle_count_fusion_chart_data_line(drill_down, rows)
        elif chart_type == "column":
            fusioncharts_data = select_vehicle_count_fusion_chart_data_column(drill_down, rows)
        else:
            raise ValueError("Invalid chart type. Please use 'line' or 'column'.")

        # Log success and increment metrics
        log_request(
            endpoint="/device/vehicle-count-drill-down",
            method="GET",
            status="200",
            message="Successfully processed vehicle count drill-down request",
        )
        increment_request_counter(
            endpoint="/device/vehicle-count-drill-down",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /device/vehicle-count-drill-down: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/device/vehicle-count-drill-down",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )



#
# function to get vehicle tracking data
from fastapi import HTTPException, Query, status, Depends
from typing import Optional
from datetime import datetime
import logging

@app.get("/device/vehicle-tracking-data", tags=["Device"])
async def get_vehicle_location(
    vehicle_regnumber: str = Query(..., description="Vehicle registration number"),
    organisation_id: Optional[int] = Query(None, description="Organisation ID in the location history"),
    start_date: Optional[datetime] = Query(None, description="Start date of the createdAt range"),
    end_date: Optional[datetime] = Query(None, description="End date of the createdAt range"),
    page: int = Query(1, ge=1, description="The page number"),
    page_size: int = Query(25, ge=1, le=100, description="The size of the page")  # Set default page size to 25
):
    skip = page_size * (page - 1)
    limit = page_size

    try:
        # Log request details
        log_request(
            endpoint="/device/vehicle-tracking-data",
            method="GET",
            status="Processing",
            message="Received request for vehicle tracking data",
            extra={
                "vehicle_regnumber": vehicle_regnumber,
                "organisation_id": organisation_id,
                "start_date": start_date,
                "end_date": end_date,
                "page": page,
                "page_size": page_size,
            },
        )

        # Construct query filters
        query = {"vehicle.regNumber": vehicle_regnumber}
        if organisation_id is not None:
            query["location_history.organisation_id"] = organisation_id

        date_query = {}
        if start_date:
            date_query["$gte"] = start_date
        if end_date:
            date_query["$lte"] = end_date

        if date_query:
            query["location_history.created_date"] = date_query

        pipeline = [
            {"$match": query},
            {"$unwind": "$location_history"},
            {"$match": query},
            {"$sort": {"location_history.created_date": -1}},  # Sort by date in descending order
            {
                "$facet": {
                    "data": [
                        {"$skip": skip},
                        {"$limit": limit},
                        {
                            "$project": {
                                "_id": 0,
                                "created_date": "$location_history.created_date",
                                "device_name": "$location_history.device_name",
                                "device_type": "$location_history.device_type",
                                "longitude": "$location_history.longitude",
                                "latitude": "$location_history.latitude",
                                "address": "$location_history.address",
                                "organisation_id": "$location_history.organisation_id",
                                "organisation_name": "$location_history.organisation_name"
                            }
                        }
                    ],
                    "totalCount": [
                        {"$count": "count"}
                    ]
                }
            }
        ]

        # Fetch data from MongoDB
        vehicle_location_data = await vehicle_tracking_collection.aggregate(pipeline).to_list(length=1)
        vehicle_location_data = vehicle_location_data[0] if vehicle_location_data else {}

        data = vehicle_location_data.get("data", [])
        if data:
            total = vehicle_location_data.get("totalCount", [{"count": 0}])[0].get("count", 0)
            last_page = (total + page_size - 1) // page_size

            data = unflatten(data)

            # Add rank property to each entry
            rank_start = skip + 1
            for i, entry in enumerate(data, start=rank_start):
                entry["rank"] = i

            # Structure response with pagination information
            response = {
                "data": data,
                "currentPage": page,
                "lastPage": last_page,
                "total": total,
                "pageSize": page_size
            }

            # Log success and increment metrics
            log_request(
                endpoint="/device/vehicle-tracking-data",
                method="GET",
                status="200",
                message="Successfully retrieved vehicle location data",
            )
            increment_request_counter(
                endpoint="/device/vehicle-tracking-data",
                method="GET",
                status="200",
            )

            return response

        # If no data is found
        else:
            data = []
            response = {
                "data": data,
                "currentPage": page,
                "lastPage": 0,
                "total": 0,
                "pageSize": page_size
            }

            # Log success with no data found
            log_request(
                endpoint="/device/vehicle-tracking-data",
                method="GET",
                status="200",
                message="No vehicle location data found",
            )
            increment_request_counter(
                endpoint="/device/vehicle-tracking-data",
                method="GET",
                status="200",
            )

            return response

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /device/vehicle-tracking-data: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/device/vehicle-tracking-data",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving vehicle location data: {str(e)}"
        )


@app.post("/violation-scheduler-data", tags=["Violation Scheduler"], status_code=201)
async def create_violation_scheduler_data(item: ViolationScheduler):
    endpoint = "/violation-scheduler-data"
    method = "POST"
    try:
        processed_item = preprocess_insert_violation_scheduler_data(item)
        # send_to_queue('violation-scheduler-queue', (values, "INSERT"))
        await process_violation_scheduler_message(processed_item)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message="Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Violation Scheduler Data processed and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message="Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/violation-scheduler/violation-status-by-count", tags=["Violation Scheduler"])
async def get_processed_vehicle(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        # drill_down: Optional[str] = "year"
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/violation-status-by-count",
            method="GET",
            status="Processing",
            message="Received request for violation status by count",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
            },
        )

        # Process request
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        query = f"""
                    SELECT isprocessed, COUNT(*) AS processed_count
                    FROM dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY isprocessed
                """
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Violation Status Count",
                "subCaption": "Violation Status",
                "use3DLighting": "0",
                "showPercentValues": "1",
                "decimals": "1",
                "showLegend": "1",
                "legendPosition": "bottom",
                "legendCaption": "Violation Status",
                "useDataPlotColorForLabels": "1",
                "type": "pie2d",
                "theme": "fusion",
            },
            "data": [],
        }

        for row in rows:
            is_processed = "Processed" if str(row.isprocessed).lower() == "true" else "Unprocessed"
            fusioncharts_data["data"].append({
                "label": is_processed,
                "value": row.processed_count,
            })

        # Increment metrics
        increment_request_counter(
            endpoint="/violation-scheduler/violation-status-by-count",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        logger.error("Error occurred: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/violation-status-by-count",
            method="GET",
            status="500",
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")



@app.get("/violation-scheduler/total-violations-count", tags=["Violation Scheduler"])
async def get_total_violations(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/total-violations-count",
            method="GET",
            status="Processing",
            message="Received request for total violations count",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
            },
        )

        # Process request
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        query = f"""
                SELECT COUNT(*) AS violations_count
                FROM dbo.violationschedulerdata
                {where_clause}
            """
        cursor = connection.cursor()
        cursor.execute(query, values)
        row = cursor.fetchone()

        # Format the data for FusionCharts
        fusioncharts_data = {
            "data": [{
                "label": "Total Violations Captured",
                "value": row.violations_count,
            }]
        }

        # Increment metrics
        increment_request_counter(
            endpoint="/violation-scheduler/total-violations-count",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        logger.error("Error occurred: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/total-violations-count",
            method="GET",
            status="500",
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")



@app.get("/violation-scheduler/average-violations-per-day-drill-down", tags=["Violation Scheduler"])
async def avg_violations_per_day_by_period(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/average-violations-per-day-drill-down",
            method="GET",
            status="Processing",
            message="Received request for average violations per day by drill-down period",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
            },
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
            drill_down=drill_down,
        )

        # Generate query
        query = select_average_violations_query(drill_down, where_clause)

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format data for FusionCharts
        fusioncharts_data = select_average_violations_fusion_chart_data(drill_down, rows)

        # Increment metrics
        increment_request_counter(
            endpoint="/violation-scheduler/average-violations-per-day-drill-down",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error details
        logger.error("Error occurred: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/average-violations-per-day-drill-down",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error"
        )





@app.get("/violation-scheduler/average-violations-per-day-overall", tags=["Violation Scheduler"])
async def avg_violations_per_day(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/average-violations-per-day-overall",
            method="GET",
            status="Processing",
            message="Received request for average violations per day (overall)",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
            },
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Construct query
        query = f"""
            SELECT 
                ROUND(
                    COUNT(*) / 
                    (DATEDIFF(DAY, MIN(CONVERT(DATETIME, createdat, 126)), 
                    MAX(CONVERT(DATETIME, createdat, 126))) + 1.0), 2
                ) AS avg_violations
            FROM dbo.violationschedulerdata
            {where_clause};
        """

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        row = cursor.fetchone()

        if not row:
            # Return 0 when no data is available
            avg_violations = 0.0
        else:
            avg_violations = row.avg_violations

        # Format data for FusionCharts
        fusioncharts_data = {
            "data": [{
                "label": "Average Violations Per Day",
                "value": avg_violations
            }]
        }

        # Increment metrics
        increment_request_counter(
            endpoint="/violation-scheduler/average-violations-per-day-overall",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error details
        logger.error("Error occurred in /average-violations-per-day-overall: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/average-violations-per-day-overall",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )




@app.get("/violation-scheduler/violation-count-and-status-drill-down", tags=["Violation Scheduler"])
async def get_violation_count_and_status(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
        chart_type: Optional[Literal["combined", "stacked"]] = "combined"
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/violation-count-and-status-drill-down",
            method="GET",
            status="Processing",
            message="Received request for violation count and status drill-down",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
                "chart_type": chart_type,
            },
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
            drill_down=drill_down,
        )

        # Construct query
        query = select_processed_violations_query(drill_down, where_clause)
        logger.debug(f"Generated query: {query}")
        logger.debug(f"Values: {values}")

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Determine chart type
        if chart_type == "combined":
            fusioncharts_data = select_processed_violations_fusion_chart_data_combined(drill_down, rows)
        elif chart_type == "stacked":
            fusioncharts_data = select_processed_violations_fusion_chart_data_stacked(drill_down, rows)
        else:
            raise ValueError("Invalid chart type. Please use 'combined' or 'stacked'.")

        # Log success and increment metrics
        log_request(
            endpoint="/violation-scheduler/violation-count-and-status-drill-down",
            method="GET",
            status="200",
            message="Successfully processed request for violation count and status drill-down",
        )
        increment_request_counter(
            endpoint="/violation-scheduler/violation-count-and-status-drill-down",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except ValueError as ve:
        logger.error("Validation error: %s", str(ve))
        increment_request_counter(
            endpoint="/violation-scheduler/violation-count-and-status-drill-down",
            method="GET",
            status="400",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve),
        )
    except Exception as e:
        logger.error("Error occurred in /violation-count-and-status-drill-down: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/violation-count-and-status-drill-down",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )








@app.get("/violation-scheduler/violation-count-by-device", tags=["Violation Scheduler"])
async def get_violations_by_device(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log the incoming request
        log_request(
            endpoint="/violation-scheduler/violation-count-by-device",
            method="GET",
            status="Processing",
            message="Received request to fetch violations count by device",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit
            }
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Determine the TOP clause
        top_clause = f"TOP {top_limit}" if top_limit > 0 else ""  # No limit if top_limit is 0

        # Construct the query
        query = f"""
            SELECT {top_clause} device_name, COUNT(*) AS violations_count
            FROM dbo.violationschedulerdata
            {where_clause}
            GROUP BY device_name
            ORDER BY violations_count DESC
        """
        logger.debug(f"Generated query: {query}")
        logger.debug(f"Values: {values}")

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Summarize total violations
        total_violations_count = sum(row.violations_count for row in rows)

        # Prepare FusionCharts-compatible data
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by Device",
                "xAxisName": "Device",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollHeight": "10",
                "numVisiblePlot": "12",
                "type": "scrollColumn2D",
                "showHoverEffect": "1"
            },
            "categories": [
                {
                    "category": [{"label": str(row.device_name)} for row in rows]
                }
            ],
            "dataset": [
                {
                    "data": [{"value": str(row.violations_count)} for row in rows]
                }
            ]
        }

        # Log the successful response
        log_request(
            endpoint="/violation-scheduler/violation-count-by-device",
            method="GET",
            status="200",
            message="Successfully fetched violations count by device"
        )
        increment_request_counter(
            endpoint="/violation-scheduler/violation-count-by-device",
            method="GET",
            status="200"
        )

        return fusioncharts_data

    except Exception as e:
        # Log and handle the exception
        logger.error("Error in /violation-count-by-device: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/violation-count-by-device",
            method="GET",
            status="500"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request."
        )




@app.get("/violation-scheduler/violation-type-monitored-by-device", tags=["Violation Scheduler"])
async def get_different_violations_by_device(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log the incoming request
        log_request(
            endpoint="/violation-scheduler/violation-type-monitored-by-device",
            method="GET",
            status="Processing",
            message="Received request to fetch different violations by device",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit
            }
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Determine the TOP clause
        top_clause = f"TOP {top_limit}" if top_limit > 0 else ""  # No limit if top_limit is 0

        # Construct the query
        query = f"""
            SELECT {top_clause} offensename, COUNT(*) AS offencecount
            FROM dbo.violationschedulerdata
            {where_clause}
            GROUP BY offensename
            ORDER BY offencecount DESC
        """
        logger.debug(f"Generated query: {query}")
        logger.debug(f"Values: {values}")

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Summarize total offences
        total_offences_count = sum(row.offencecount for row in rows)

        # Prepare FusionCharts-compatible data
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by Type",
                "xAxisName": "Violation Type",
                "yAxisName": "Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollHeight": "10",
                "numVisiblePlot": "12",
                "type": "scrollColumn2D",
                "showHoverEffect": "1"
            },
            "categories": [
                {
                    "category": [{"label": str(row.offensename)} for row in rows]
                }
            ],
            "dataset": [
                {
                    "data": [{"value": str(row.offencecount)} for row in rows]
                }
            ]
        }

        # Log the successful response
        log_request(
            endpoint="/violation-scheduler/violation-type-monitored-by-device",
            method="GET",
            status="200",
            message="Successfully fetched violation types monitored by device"
        )
        increment_request_counter(
            endpoint="/violation-scheduler/violation-type-monitored-by-device",
            method="GET",
            status="200"
        )

        return fusioncharts_data

    except Exception as e:
        # Log and handle the exception
        logger.error("Error in /violation-type-monitored-by-device: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/violation-type-monitored-by-device",
            method="GET",
            status="500"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request."
        )





@app.get("/violation-scheduler/violation-count-by-mode-of-processing", tags=["Violation Scheduler"])
async def get_violations_by_mode_of_processing(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log the incoming request
        log_request(
            endpoint="/violation-scheduler/violation-count-by-mode-of-processing",
            method="GET",
            status="Processing",
            message="Received request to fetch violations count by mode of processing",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit
            }
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Determine the TOP clause
        top_clause = f"TOP {top_limit}" if top_limit > 0 else ""  # No limit if top_limit is 0

        # Construct the query
        query = f"""
            SELECT {top_clause} handletype, COUNT(*) AS violations_count
            FROM dbo.violationschedulerdata
            {where_clause}
            GROUP BY handletype
            ORDER BY handletype
        """
        logger.debug(f"Generated query: {query}")
        logger.debug(f"Values: {values}")

        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Prepare FusionCharts-compatible data
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by Mode of Processing",
                "subCaption": "Violations Count",
                "use3DLighting": "0",
                "showPercentValues": "1",
                "decimals": "1",
                "showLegend": "1",
                "legendPosition": "bottom",
                "legendCaption": "Handle Type",
                "useDataPlotColorForLabels": "1",
                "type": "pie2d",
                "theme": "fusion",
            },
            "data": [{"label": str(row.handletype), "value": str(row.violations_count)} for row in rows]
        }

        # Log the successful response
        log_request(
            endpoint="/violation-scheduler/violation-count-by-mode-of-processing",
            method="GET",
            status="200",
            message="Successfully fetched violations count by mode of processing"
        )
        increment_request_counter(
            endpoint="/violation-scheduler/violation-count-by-mode-of-processing",
            method="GET",
            status="200"
        )

        return fusioncharts_data

    except Exception as e:
        # Log and handle the exception
        logger.error("Error in /violation-count-by-mode-of-processing: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/violation-count-by-mode-of-processing",
            method="GET",
            status="500"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request."
        )


@app.get("/violation-scheduler/approval-mode-statistics", tags=["Violation Scheduler"])
async def get_approval_status(
        OrganisationId: int = Depends(get_OrganisationId),
        user_id: Optional[str] = None,
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
        chart_type: Optional[Literal["pie", "stacked"]] = "pie"
):
    try:
        # Log the incoming request
        log_request(
            endpoint="/violation-scheduler/approval-mode-statistics",
            method="GET",
            status="Processing",
            message="Received request to fetch approval mode statistics",
            extra={
                "OrganisationId": OrganisationId,
                "user_id": user_id,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
                "chart_type": chart_type
            }
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            user_id=user_id,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
            drill_down=drill_down
        )

        # Determine chart type and fetch data accordingly
        if chart_type == 'pie':
            query = f"""
                SELECT approvalmode, COUNT(*) AS count
                FROM dbo.violationschedulerdata
                {where_clause}
                GROUP BY approvalmode
            """
            logger.debug(f"Generated query for pie chart: {query}")
            logger.debug(f"Values: {values}")

            cursor = connection.cursor()
            cursor.execute(query, values)
            rows = cursor.fetchall()

            # Format data for FusionCharts
            fusioncharts_data = {
                "chart": {
                    "caption": "Approval Status Count",
                    "subCaption": "Approval Status",
                    "use3DLighting": "0",
                    "showPercentValues": "1",
                    "decimals": "1",
                    "showLegend": "1",
                    "legendPosition": "bottom",
                    "legendCaption": "Approval Status",
                    "useDataPlotColorForLabels": "1",
                    "type": "pie2d",
                    "theme": "fusion",
                },
                "data": []
            }

            # Populate data for the pie chart
            for row in rows:
                approvalmode = str(row.approvalmode)
                approvalmode = {
                    "1": "Pending",
                    "2": "Approved",
                    "3": "Rejected"
                }.get(approvalmode.lower(), "Unknown")

                fusioncharts_data["data"].append({
                    "label": approvalmode,
                    "value": str(row.count)
                })

            # Log successful response
            log_request(
                endpoint="/violation-scheduler/approval-mode-statistics",
                method="GET",
                status="200",
                message="Successfully fetched approval mode statistics (pie chart)"
            )
            increment_request_counter(
                endpoint="/violation-scheduler/approval-mode-statistics",
                method="GET",
                status="200"
            )

            return fusioncharts_data

        elif chart_type == 'stacked':
            logger.debug("Fetching data for stacked chart")
            query = select_approval_mode_query(drill_down, where_clause)
            logger.debug(f"Generated query for stacked chart: {query}")

            cursor = connection.cursor()
            cursor.execute(query, values)
            rows = cursor.fetchall()
            logger.debug(f"Fetched rows for stacked chart: {rows}")

            fusioncharts_data = select_approval_mode_fusion_chart_data_stacked(drill_down, rows)

            # Log successful response
            log_request(
                endpoint="/violation-scheduler/approval-mode-statistics",
                method="GET",
                status="200",
                message="Successfully fetched approval mode statistics (stacked chart)"
            )
            increment_request_counter(
                endpoint="/violation-scheduler/approval-mode-statistics",
                method="GET",
                status="200"
            )

            return fusioncharts_data

        else:
            raise ValueError("Invalid chart type. Please use 'pie' or 'stacked'.")

    except Exception as e:
        # Log and handle error
        logger.error("Error in /violation-scheduler/approval-mode-statistics: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/approval-mode-statistics",
            method="GET",
            status="500"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request."
        )







@app.get("/violation-scheduler/repeated-violation-overview", tags=["Violation Scheduler"])
async def get_violations_by_mode_of_processing(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/repeated-violation-overview",
            method="GET",
            status="Processing",
            message="Received request for repeated violation overview",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit,
            },
        )

        # Generate the WHERE clause based on the filters provided
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        if top_limit > 0:
            top_clause = f"TOP {top_limit}"
        else:
            top_clause = ""  # No limit if top_limit is 0

        # SQL query to count repeated violations by vehicle number
        query = f"""
            SELECT 
                {top_clause} offensename,
                COUNT(*) AS violations_count
            FROM 
                dbo.violationschedulerdata
            {where_clause}
            GROUP BY 
                offensename
            HAVING 
                COUNT(*) > 1
            ORDER BY 
                violations_count DESC
        """

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Repeated Violations",
                "xAxisName": "Violations",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollColumn2D",
                "showHoverEffect": "1"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.offensename)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.violations_count)
            })

        # Log success and increment metrics
        log_request(
            endpoint="/violation-scheduler/repeated-violation-overview",
            method="GET",
            status="200",
            message="Successfully processed request for repeated violation overview",
        )
        increment_request_counter(
            endpoint="/violation-scheduler/repeated-violation-overview",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        logger.error("Error occurred in /violation-scheduler/repeated-violation-overview: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/repeated-violation-overview",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )


@app.get("/violation-scheduler/repeated-violation-by-vehicle-number", tags=["Violation Scheduler"])
async def get_violations_by_mode_of_processing(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/repeated-violation-by-vehicle-number",
            method="GET",
            status="Processing",
            message="Received request for repeated violations by vehicle number",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit,
            },
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Apply top limit
        if top_limit > 0:
            top_clause = f"TOP {top_limit}"
        else:
            top_clause = ""  # No limit if top_limit is 0

        # Construct query
        query = f"""
            SELECT 
                {top_clause} vehicle_regnumber,
                offensename,
                COUNT(*) AS violations_count
            FROM 
                dbo.violationschedulerdata
            {where_clause}
            GROUP BY 
                vehicle_regnumber, offensename
            HAVING 
                COUNT(*) > 1
            ORDER BY 
                violations_count DESC
        """
        logger.debug(f"Generated query: {query}")
        logger.debug(f"Values: {values}")

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Repeated Violations by Vehicle Number",
                "xAxisName": "Violations",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollColumn2D",
                "showHoverEffect": "1"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.vehicle_regnumber)
            })
            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.violations_count),
                "toolText": f"Offense: {str(row.offensename)}, Violations: {str(row.violations_count)}"
            })

        # Log success and increment metrics
        log_request(
            endpoint="/violation-scheduler/repeated-violation-by-vehicle-number",
            method="GET",
            status="200",
            message="Successfully processed request for repeated violations by vehicle number",
        )
        increment_request_counter(
            endpoint="/violation-scheduler/repeated-violation-by-vehicle-number",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        logger.error("Error occurred in /repeated-violation-by-vehicle-number: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/repeated-violation-by-vehicle-number",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )




@app.get("/violation-scheduler/captured-violations-trend-drill-down", tags=["Violation Scheduler"])
async def total_violations_trend_by_period(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day", "hour"]] = "year"
):
    try:
        # Log request details
        log_request(
            endpoint="/violation-scheduler/captured-violations-trend-drill-down",
            method="GET",
            status="Processing",
            message="Received request for captured violations trend by period",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
            },
        )

        # Generate filter conditions
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
            drill_down=drill_down,
        )

        # Debugging output to verify
        logger.debug(f"Generated where_clause: {where_clause}")
        logger.debug(f"Values: {values}")

        # Construct query based on drill_down
        query = select_total_violations_query(drill_down, where_clause)

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts
        fusioncharts_data = select_total_violations_fusion_chart_data(drill_down, rows)

        # Log success and increment metrics
        log_request(
            endpoint="/violation-scheduler/captured-violations-trend-drill-down",
            method="GET",
            status="200",
            message="Successfully processed request for captured violations trend by period",
        )
        increment_request_counter(
            endpoint="/violation-scheduler/captured-violations-trend-drill-down",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        logger.error("Error occurred in /captured-violations-trend-drill-down: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/captured-violations-trend-drill-down",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )






@app.post("/update-violation-status", tags=["Violation Scheduler"], status_code=status.HTTP_201_CREATED)
async def update_violation_status(item: ViolationSchedulerUpdate):
    endpoint = "/update-violation-status"
    method = "POST"
    try:
        # updated_date = datetime.now()
        #
        # values = item.dict()
        # values['reviewDate'] = updated_date.isoformat()
        # values['isprocessed'] = "TRUE"
        # processed_item = (values, "UPDATE")
        processed_item = preprocess_insert_violation_scheduler_data(item)

        await process_violation_scheduler_message(processed_item)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message="Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Violation Data updated and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message="Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))

# @app.post("/add-rejection-reason", tags=["Violation Scheduler"], status_code=status.HTTP_201_CREATED)
# async def add_rejection_reason(new_reason: str):
#     try:
#         await process_rejected_reasons(new_reason)
#         return {"message": "Data pushed and processed successfully"}
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

# @app.get("/get_rejected_reasons")
# async def get_all_rejection_reasons():
#     try:
#         cursor = connection.cursor()
#         query = "SELECT reasonname from dbo.rejectReasons"
#         cursor.execute(query)
#         rows = cursor.fetchall()
#         all_reasons = (reason.reasonname for reason in rows)
#         return all_reasons
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/violation-scheduler/violation-by-rejected-reason", tags=["Violation Scheduler"])
async def get_manual_violation_by_rejected_reason(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[int] = 3,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None
):
    try:
        # Log the incoming request for monitoring
        log_request(
            endpoint="/violation-scheduler/violation-by-rejected-reason",
            method="GET",
            status="Processing",
            message="Received request for violation by rejected reason",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
            },
        )

        # Generate the WHERE clause based on filters
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end
        )

        # Debugging output to verify
        logger.debug(f"Generated where_clause: {where_clause}")
        logger.debug(f"Values: {values}")

        # SQL query to count violations by rejection reason
        query = f"""
            WITH CleanedReasons AS (
                SELECT
                    TRIM(REPLACE(REPLACE(REPLACE(REPLACE(value, '["', ''), '"]', ''), '""', '"'), '[]', '')) AS cleaned_reason
                FROM
                    dbo.violationschedulerdata
                CROSS APPLY
                    STRING_SPLIT(REPLACE(REPLACE(REPLACE(REPLACE(rejection_reason, '["', ''), '"]', ''), '"', ''), '[]', ''), ',')
                {where_clause}
            )
            SELECT
                cleaned_reason AS rejection_reason,
                COUNT(*) AS count
            FROM
                CleanedReasons
            GROUP BY
                cleaned_reason
            ORDER BY
                count DESC;
        """
        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Prepare the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Violation by Rejected Reason",
                "xAxisName": "Rejection Reason",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollColumn2D",
                "showHoverEffect": "1"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.rejection_reason)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.count)
            })

        # Log success and increment metrics
        log_request(
            endpoint="/violation-scheduler/violation-by-rejected-reason",
            method="GET",
            status="200",
            message="Successfully processed request for violation by rejected reason",
        )
        increment_request_counter(
            endpoint="/violation-scheduler/violation-by-rejected-reason",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment metrics
        logger.error("Error occurred in /violation-by-rejected-reason: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/violation-by-rejected-reason",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )



@app.get("/violation-scheduler/operators-processed-and-rejected-events", tags=["Violation Scheduler"])
async def get_operators_processed_and_rejected_events(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[int] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log the incoming request for monitoring
        log_request(
            endpoint="/violation-scheduler/operators-processed-and-rejected-events",
            method="GET",
            status="Processing",
            message="Received request for operators processed and rejected events",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "approvalmode": approvalmode,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit,
            },
        )

        # Generate the WHERE clause based on filters
        where_clause, values = violation_scheduler_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            approvalmode=approvalmode,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end
        )

        # Apply the top limit if greater than 0, else no limit
        top_clause = f"TOP {top_limit}" if top_limit > 0 else ""

        # Debugging output to verify
        logger.debug(f"Generated where_clause: {where_clause}")
        logger.debug(f"Values: {values}")

        # SQL query to count violations by operator with processed and rejected events
        query = f"""
            SELECT
                {top_clause} username,
                COUNT(CASE WHEN approvalmode = 1 THEN 1 END) AS pending,
                COUNT(CASE WHEN approvalmode = 2 THEN 1 END) AS approved,
                COUNT(CASE WHEN approvalmode = 3 THEN 1 END) AS rejected
            FROM
                dbo.violationschedulerdata
            {where_clause}
            GROUP BY
                username
            ORDER BY
                approved DESC;
        """
        # Execute the query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Prepare the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Operator Event Analytics",
                "xAxisName": "Operator Name",
                "yAxisName": "Count",
                "theme": "fusion",
                "subCaption": "Operator's processed and Rejected Event",
                "scrollheight": "10",
                "flatScrollBars": "1",
                "scrollShowButtons": "0",
                "scrollColor": "#cccccc",
                "showHoverEffect": "1",
                "labelDisplay": "wrap",
                "labelPadding": "20",
                "type": "scrollstackedcolumn2d"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Pending",
                    "data": []
                },
                {
                    "seriesName": "Approved",
                    "data": []
                },
                {
                    "seriesName": "Rejected",
                    "data": []
                }
            ]
        }

        # Populate the chart data
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.username)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.pending)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.approved)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.rejected)
            })

        # Log success and increment metrics
        log_request(
            endpoint="/violation-scheduler/operators-processed-and-rejected-events",
            method="GET",
            status="200",
            message="Successfully processed request for operators processed and rejected events",
        )
        increment_request_counter(
            endpoint="/violation-scheduler/operators-processed-and-rejected-events",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment metrics
        logger.error("Error occurred in /operators-processed-and-rejected-events: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/violation-scheduler/operators-processed-and-rejected-events",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )



@app.post("/create-installation-health-data", tags=["Installation Health"], status_code=status.HTTP_201_CREATED)
async def create_installation_health(item: InstallationHealthData):
    endpoint = "/create-installation-health-data"
    method = "POST"

    try:

        processed_item = preprocess_installation_health_data(item)

        await process_installation_health_data(processed_item)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message="Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Installation health Data processed and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message="Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))









@app.get("/installation/installation-by-status", tags=["Installation Health"])
async def get_installation_by_status(
        OrganisationId: int = Depends(get_OrganisationId),
        installation_id: Optional[str] = None,
        installation_name: Optional[str] = None,
        isonline: Optional[bool] = None,
):
    try:
        # Log request details
        log_request(
            endpoint="/installation/installation-by-status",
            method="GET",
            status="Processing",
            message="Received request for installation status by count",
            extra={
                "OrganisationId": OrganisationId,
                "installation_id": installation_id,
                "installation_name": installation_name,
                "isonline": isonline,
            },
        )

        # Generate filter conditions
        where_clause, values = installation_filter_conditions_2(
            OrganisationId=OrganisationId,
            installation_id=installation_id,
            installation_name=installation_name,
            isonline=isonline
        )

        # Debugging output to verify the generated query
        logger.debug(f"Generated where_clause: {where_clause}")
        logger.debug(f"Values: {values}")

        query = f"""
            SELECT isonline, COUNT(*) AS isonline_count
            FROM dbo.installationstatus
            {where_clause}
            GROUP BY isonline
        """

        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Installation Status by Count",
                "subCaption": "Installation Status",
                "use3DLighting": "0",
                "showPercentValues": "1",
                "decimals": "1",
                "showLegend": "1",
                "legendPosition": "bottom",
                "legendCaption": "Installation Status",
                "useDataPlotColorForLabels": "1",
                "type": "pie2d",
                "theme": "fusion",
            },
            "data": []
        }

        for row in rows:
            is_online = str(row.isonline)

            if is_online.lower() == "true":
                is_online = "Online"
            elif is_online.lower() == "false":
                is_online = "Offline"

            fusioncharts_data["data"].append({
                "label": is_online,
                "value": row.isonline_count,
            })

        # Log success and increment metrics
        log_request(
            endpoint="/installation/installation-by-status",
            method="GET",
            status="200",
            message="Successfully processed request for installation status by count",
        )
        increment_request_counter(
            endpoint="/installation/installation-by-status",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment failure counter
        logger.error("Error occurred in /installation/installation-by-status: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/installation/installation-by-status",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )



# @app.get("/installation/installation-list", tags=["Installation Health"])
# async def get_installation_list(
#         OrganisationId: int = Depends(get_OrganisationId),
#         installation_id: Optional[str] = None,
#         installation_name: Optional[str] = None,
#         isonline: Optional[bool] = None,
#         page: int = Query(1, gt=0),
#         page_size: int = Query(25, gt=0),
# ):
#     try:
#         # Generate the WHERE clause based on the filters provided
#         where_clause, values = installation_filter_conditions_2(
#             OrganisationId=OrganisationId,
#             installation_id=installation_id,
#             installation_name=installation_name,
#             isonline=isonline
#         )
#
#
#         offset = (page - 1) * page_size
#         values.extend([offset, page_size])
#         print(f"Generated where_clause: {where_clause}")
#         print(f"Values: {values}")
#
#         query = f"""
#                     SELECT *
#                     FROM dbo.installationstatus
#                     {where_clause}
#                     ORDER BY id
#                     OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
#                 """
#
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         rows = cursor.fetchall()
#
#         total_count_query = f"""
#                         SELECT COUNT(*) FROM dbo.installationstatus
#                         {where_clause}
#                     """
#         cursor.execute(total_count_query, values[:-2])
#         total_count = cursor.fetchone()[0]
#
#
#         results = []
#
#         for row in rows:
#             results.append({
#                 "id": row.id,
#                 "installation":{
#                     "id": row.installation_id,
#                     "name": str(row.installation_name)
#                 },
#                 "location": {
#                     "latitude": str(row.latitude),
#                     "longitude": str(row.longitude)
#                 },
#                 "isonline": row.isonline,
#                 "organisation": {
#                     "id": row.organisation_id,
#                     "name": str(row.organisation_name)
#                 }
#             })
#
#         return {
#             "data": results,
#             "page": page,
#             "page_size": page_size,
#             "total_count": total_count,
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))



@app.get("/installation/installation-heartbeat", tags=["Installation Health"])
async def get_installation_heartbeat(
        OrganisationId: int = Depends(get_OrganisationId),
        installation_name: Optional[str] = None,
        isonline: Optional[bool] = None,
        limit: Optional[int] = 30,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
        chart_type: Optional[Literal["combined", "stacked"]] = "combined"
):
    try:
        # Log request details
        log_request(
            endpoint="/installation/installation-heartbeat",
            method="GET",
            status="Processing",
            message="Received request for installation heartbeat",
            extra={
                "OrganisationId": OrganisationId,
                "installation_name": installation_name,
                "isonline": isonline,
                "limit": limit,
                "drill_down": drill_down,
                "chart_type": chart_type,
            },
        )

        # If installation_name is not None, clean it up and split into list
        if installation_name is not None:
            installation_name = [item.strip() for item in installation_name.split(',') if item.strip()]

        cursor = connection.cursor()

        # If no installation names are provided, fetch the top 3 unique installation names
        if installation_name is None or len(installation_name) == 0:
            smaller_where = f"WHERE organisation_id = {OrganisationId}" if OrganisationId != 13 else ""
            cursor.execute(f"""
                SELECT DISTINCT TOP 3 installation_name
                FROM installationhealthhistory
                {smaller_where}
            """)

            installation_name = [row[0] for row in cursor.fetchall()]

        # If no installations are found, return an empty response
        if len(installation_name) == 0:
            return {
                "chart": {
                    "caption": "Installation Heartbeat",
                    "subCaption": f"Online Status for the past {limit} pings",
                    "xAxisName": "Timestamp",
                    "yAxisName": "Online Status",
                    "theme": "fusion",
                    "lineThickness": "3",
                    "flatScrollBars": "1",
                    "scrollheight": "10",
                    "numVisiblePlot": "12",
                    "type": "scrollline2d",
                    "showHoverEffect": "1"
                },
                "categories": [{"category": []}],
                "dataset": [{"data": []}]
            }

        # Generate the WHERE clause based on the filters provided
        conditions, values = installation_filter_conditions(
            OrganisationId=OrganisationId,
            isonline=isonline
        )

        if limit > 0:
            top_clause = f"TOP {limit}"
        else:
            top_clause = ""  # No limit if limit is 0

        # Initialize FusionCharts data
        fusioncharts_data = {
            "chart": {
                "caption": f"Installation Heartbeat for {installation_name}",
                "subCaption": f"Online Status for the past {limit} pings",
                "xAxisName": "Timestamp",
                "yAxisName": "Online Status",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },
            "categories": [{"category": []}],
            "dataset": []
        }

        for name in installation_name:
            conditions.append(f"installation_name = ?")
            values.append(name)

            conditions_str = " AND ".join(conditions)
            where_clause = f"WHERE {conditions_str}" if conditions else ""

            query = f"""
                SELECT *
                FROM (
                    SELECT {top_clause} installation_name, isonline, CONVERT(VARCHAR(16), createdat, 120) as createdat_
                    FROM installationhealthhistory
                    {where_clause}
                    ORDER BY createdat_ DESC
                ) AS most_recent_heartbeat
                ORDER BY createdat_ ASC;
            """

            cursor.execute(query, values)
            rows = cursor.fetchall()

            if len(rows) == 0:
                # If no rows are found for the installation, add empty data for it
                fusioncharts_data["dataset"].append(
                    {"seriesName": f"{name}", "data": []}
                )
            else:
                # Add timestamps for the category (x-axis labels)
                if len(fusioncharts_data["categories"][0]["category"]) == 0:
                    fusioncharts_data["categories"][0]["category"] = [{"label": row.createdat_} for row in rows]

                # Add data values (online status)
                for row in rows:
                    fusioncharts_data["dataset"][-1]["data"].append({
                        "value": str(1) if row.isonline else str(0)
                    })

            # Clean up the conditions for the next iteration
            conditions.pop()
            values.pop()

        # Log success and increment metrics
        log_request(
            endpoint="/installation/installation-heartbeat",
            method="GET",
            status="200",
            message="Successfully processed request for installation heartbeat",
        )
        increment_request_counter(
            endpoint="/installation/installation-heartbeat",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        logger.error("Error occurred in /installation/installation-heartbeat: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/installation/installation-heartbeat",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )



@app.post("/create-service-health-data", tags=["Service Health"], status_code=status.HTTP_201_CREATED)
async def create_service_health(item: ServiceHealthData):
    endpoint = "/create-service-health-data"
    method = "POST"

    try:

        processed_item = preprocess_service_health_data(item)

        await process_service_health_data(processed_item)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message="Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Service health Data processed and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message="Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/service/service-by-status", tags=["Service Health"])
async def get_service_by_status(
        OrganisationId: int = Depends(get_OrganisationId),
        service_name: Optional[str] = None,
        isonline: Optional[bool] = None,
):
    try:
        # Log request details
        log_request(
            endpoint="/service/service-by-status",
            method="GET",
            status="Processing",
            message="Received request for service status by count",
            extra={
                "OrganisationId": OrganisationId,
                "service_name": service_name,
                "isonline": isonline,
            },
        )

        # Generate filter conditions
        where_clause, values = service_filter_conditions_2(
            OrganisationId=OrganisationId,
            service_name=service_name,
            isonline=isonline
        )

        # Debugging output to verify the generated query
        logger.debug(f"Generated where_clause: {where_clause}")
        logger.debug(f"Values: {values}")

        query = f"""
            SELECT isonline, COUNT(*) AS isonline_count
            FROM dbo.servicestatus
            {where_clause}
            GROUP BY isonline
        """

        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Service Status by Count",
                "subCaption": "Service Status",
                "use3DLighting": "0",
                "showPercentValues": "1",
                "decimals": "1",
                "showLegend": "1",
                "legendPosition": "bottom",
                "legendCaption": "Service Status",
                "useDataPlotColorForLabels": "1",
                "type": "pie2d",
                "theme": "fusion",
            },
            "data": []
        }

        for row in rows:
            is_online = str(row.isonline)

            if is_online.lower() == "true":
                is_online = "Online"
            elif is_online.lower() == "false":
                is_online = "Offline"

            fusioncharts_data["data"].append({
                "label": is_online,
                "value": row.isonline_count,
            })

        # Log success and increment metrics
        log_request(
            endpoint="/service/service-by-status",
            method="GET",
            status="200",
            message="Successfully processed request for service status by count",
        )
        increment_request_counter(
            endpoint="/service/service-by-status",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment failure counter
        logger.error("Error occurred in /service/service-by-status: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/service/service-by-status",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )

#
# @app.get("/service/service-list", tags=["Service Health"])
# async def get_service_list(
#         OrganisationId: int = Depends(get_OrganisationId),
#         service_name: Optional[str] = None,
#         isonline: Optional[bool] = None,
#         page: int = Query(1, gt=0),
#         page_size: int = Query(25, gt=0),
# ):
#     try:
#         # Generate the WHERE clause based on the filters provided
#         where_clause, values = service_filter_conditions_2(
#             OrganisationId=OrganisationId,
#             service_name=service_name,
#             isonline=isonline
#         )
#
#
#         offset = (page - 1) * page_size
#         values.extend([offset, page_size])
#         print(f"Generated where_clause: {where_clause}")
#         print(f"Values: {values}")
#
#         query = f"""
#                     SELECT *
#                     FROM dbo.servicestatus
#                     {where_clause}
#                     ORDER BY id
#                     OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
#                 """
#
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         rows = cursor.fetchall()
#
#         total_count_query = f"""
#                         SELECT COUNT(*) FROM dbo.servicestatus
#                         {where_clause}
#                     """
#         cursor.execute(total_count_query, values[:-2])
#         total_count = cursor.fetchone()[0]
#
#
#         results = []
#
#         for row in rows:
#             results.append({
#                 "id": row.id,
#                 "service": {
#                     "name": str(row.service_name)
#                 },
#                 "location": {
#                     "latitude": str(row.latitude),
#                     "longitude": str(row.longitude)
#                 },
#                 "isonline": row.isonline,
#                 "organisation": {
#                     "id": row.organisation_id,
#                     "name": str(row.organisation_name)
#                 }
#             })
#
#         return {
#             "data": results,
#             "page": page,
#             "page_size": page_size,
#             "total_count": total_count,
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
#


@app.get("/service/service-heartbeat", tags=["Service Health"])
async def get_service_heartbeat(
        OrganisationId: int = Depends(get_OrganisationId),
        service_name: Optional[str] = None,
        isonline: Optional[bool] = None,
        limit: Optional[int] = 30
):
    try:
        # Log the start of the request
        log_request(
            endpoint="/service/service-heartbeat",
            method="GET",
            status="Processing",
            message="Received request for service heartbeat",
            extra={
                "OrganisationId": OrganisationId,
                "service_name": service_name,
                "isonline": isonline,
                "limit": limit,
            },
        )

        if service_name is not None:
            service_name = [item.strip() for item in service_name.split(',') if item.strip()]

        cursor = connection.cursor()

        # If no service names are provided, fetch the top 3 unique service names
        if service_name is None or len(service_name) == 0:
            smaller_where = f"WHERE organisation_id = {OrganisationId}" if OrganisationId != 13 else ""
            cursor.execute(f"""
                SELECT DISTINCT TOP 3 service_name
                FROM servicehealthhistory
                {smaller_where}
            """)

            service_name = [row[0] for row in cursor.fetchall()]

        # If no services are found, return an empty response
        if len(service_name) == 0:
            return {
                "chart": {
                    "caption": "Service Heartbeat",
                    "subCaption": f"Online Status for the past {limit} pings",
                    "xAxisName": "Timestamp",
                    "yAxisName": "Online Status",
                    "theme": "fusion",
                    "lineThickness": "3",
                    "flatScrollBars": "1",
                    "scrollheight": "10",
                    "numVisiblePlot": "12",
                    "type": "scrollline2d",
                    "showHoverEffect": "1"
                },
                "categories": [{"category": []}],
                "dataset": [{"data": []}]
            }

        # Generate the WHERE clause based on the filters provided
        conditions, values = service_filter_conditions(
            OrganisationId=OrganisationId,
            isonline=isonline
        )

        if limit > 0:
            top_clause = f"TOP {limit}"
        else:
            top_clause = ""  # No limit if top_limit is 0

        # Initialize FusionCharts data
        fusioncharts_data = {
            "chart": {
                "caption": f"Service Heartbeat for {service_name}",
                "subCaption": f"Online Status for the past {limit} pings",
                "xAxisName": "Timestamp",
                "yAxisName": "Online Status",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },
            "categories": [{"category": []}],
            "dataset": []
        }

        for name in service_name:
            conditions.append(f"service_name = ?")
            values.append(name)

            conditions_str = " AND ".join(conditions)
            where_clause = f"WHERE {conditions_str}" if conditions else ""

            query = f"""
                SELECT *
                FROM (
                    SELECT {top_clause} service_name, isonline, CONVERT(VARCHAR(16), createdat, 120) as createdat_
                    FROM servicehealthhistory
                    {where_clause}
                    ORDER BY createdat_ DESC
                ) AS most_recent_heartbeat
                ORDER BY createdat_ ASC;
            """

            cursor.execute(query, values)
            rows = cursor.fetchall()

            if len(rows) == 0:
                # If no rows are found for the service, add empty data for it
                fusioncharts_data["dataset"].append(
                    {"seriesName": f"{name}", "data": []}
                )
            else:
                # Add an empty dataset entry for this service
                fusioncharts_data["dataset"].append(
                    {"seriesName": f"{name}", "data": []}
                )

                # Add timestamps for the category (x-axis labels)
                if len(fusioncharts_data["categories"][0]["category"]) == 0:
                    fusioncharts_data["categories"][0]["category"] = [{"label": row.createdat_} for row in rows]

                # Add data values (online status)
                for row in rows:
                    fusioncharts_data["dataset"][-1]["data"].append({
                        "value": str(1) if row.isonline else str(0)
                    })

            # Clean up the conditions for the next iteration
            conditions.pop()
            values.pop()

        # Log success and increment metrics
        log_request(
            endpoint="/service/service-heartbeat",
            method="GET",
            status="200",
            message="Successfully processed request for service heartbeat",
        )
        increment_request_counter(
            endpoint="/service/service-heartbeat",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment failure counter
        logger.error("Error occurred in /service/service-heartbeat: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/service/service-heartbeat",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )


# @app.get("/service/service-heartbeat", tags=["Service Health"])
# async def get_service_heartbeat(
#         OrganisationId: int = Depends(get_OrganisationId),
#         service_name: Optional[str] = None,
#         isonline: Optional[bool] = None,
#         limit: Optional[int] = 30
# 
# ):
#     try:
# 
#         # Generate the WHERE clause based on the filters provided
#         where_clause, values = service_filter_conditions(
#             OrganisationId=OrganisationId,
#             service_name=service_name,
#             isonline=isonline
#         )
# 
#         if limit > 0:
#             top_clause = f"TOP {limit}"
#         else:
#             top_clause = ""  # No limit if top_limit is 0
# 
# 
#         print(f"Generated where_clause: {where_clause}")
#         print(f"Values: {values}")
# 
# 
#         # Query to get the service's heartbeat data
#         query = f"""
#                     SELECT *
#                     FROM(
#                         SELECT {top_clause} service_name, isonline, createdat
#                         FROM servicehealthhistory
#                         {where_clause}
#                         ORDER BY createdat DESC
#                     ) as most_recent_heartbeat
#                     ORDER BY createdat ASC;                 
#                 """
# 
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         rows = cursor.fetchall()
# 
#         # Prepare data for FusionCharts
#         fusioncharts_data = {
#             "chart": {
#                 "caption": f"service Heartbeat for {service_name}",
#                 "subCaption": "Online Status Over Time",
#                 "xAxisName": "Timestamp",
#                 "yAxisName": "Online Status",
#                 "theme": "fusion",
#                 "lineThickness": "3",
#                 "flatScrollBars": "1",
#                 "scrollheight": "10",
#                 "numVisiblePlot": "12",
#                 "type": "scrollline2d",
#                 "showHoverEffect": "1"
#             },
#             "categories": [{
#                 "category": []
#             }],
#             "dataset": [{
#                 "seriesName": "Online Status",
#                 "data": []
#             }]
#         }
# 
#         # Populate the data
#         for row in rows:
#             fusioncharts_data["categories"][0]["category"].append({
#                 "label": row.createdat.strftime("%Y-%m-%d %H:%M:%S")
#             })
#             fusioncharts_data["dataset"][0]["data"].append({
#                 "value": "1" if row.isonline else "0"
#             })
# 
#         return fusioncharts_data
# 
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
# 





@app.get("/watchlist/vehicle-count-by-status", tags=["Watchlist"])
async def get_watchlist_status(
        OrganisationId: int = Depends(get_OrganisationId)
):
    try:
        # Log request details
        log_request(
            endpoint="/watchlist/vehicle-count-by-status",
            method="GET",
            status="Processing",
            message="Received request for watchlist vehicle count by status",
            extra={"OrganisationId": OrganisationId}
        )

        # Step 1: Call the external API to get the data
        url = f"http://core/api/v1/WatchList/get-data?OrganisationId={OrganisationId}"
        response = requests.get(url)

        # Handle any errors from the external API
        if response.status_code != 200:
            logger.error(f"Failed API call to external service. Status code: {response.status_code}")
            increment_request_counter(
                endpoint="/watchlist/vehicle-count-by-status",
                method="GET",
                status="500"
            )
            return {
                "chart": {
                    "caption": "Watchlist Status Count",
                    "subCaption": "Watchlist Status Summary",
                    "use3DLighting": "0",
                    "showPercentValues": "1",
                    "decimals": "1",
                    "showLegend": "1",
                    "legendPosition": "bottom",
                    "legendCaption": "Watchlist Status",
                    "useDataPlotColorForLabels": "1",
                    "type": "pie2d",
                    "theme": "fusion",
                },
                "data": []  # Empty data array if API call fails
            }

        external_data = response.json()

        # Check if the API response is successful
        if not external_data.get("isSuccessful", False):
            logger.error("External API response is not successful.")
            increment_request_counter(
                endpoint="/watchlist/vehicle-count-by-status",
                method="GET",
                status="500"
            )
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                 detail="Failed to retrieve valid data from external service")

        # Step 2: Extract activeCount and deletedCount
        active_count = external_data["data"].get("activeCount", 0)
        deleted_count = external_data["data"].get("deletedCount", 0)

        # Format the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Watchlist Status Count",
                "subCaption": "Watchlist Status Summary",
                "use3DLighting": "0",
                "showPercentValues": "1",
                "decimals": "1",
                "showLegend": "1",
                "legendPosition": "bottom",
                "legendCaption": "Watchlist Status",
                "useDataPlotColorForLabels": "1",
                "type": "pie2d",
                "theme": "fusion",
            },
            "data": [
                {"label": "Active", "value": active_count},
                {"label": "Closed", "value": deleted_count}
            ]
        }

        # Log success and increment metrics
        log_request(
            endpoint="/watchlist/vehicle-count-by-status",
            method="GET",
            status="200",
            message="Successfully processed request for watchlist vehicle count by status"
        )
        increment_request_counter(
            endpoint="/watchlist/vehicle-count-by-status",
            method="GET",
            status="200"
        )

        return fusioncharts_data

    except Exception as e:
        logger.error(f"Error occurred in /watchlist/vehicle-count-by-status: {str(e)}", exc_info=True)
        increment_request_counter(
            endpoint="/watchlist/vehicle-count-by-status",
            method="GET",
            status="500"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request."
        )





@app.post("/create-watchlist-hits", tags=["Watchlist"], status_code=201)
async def create_watchlist_hits(item: CreateWatchlistHits):
    endpoint = "/create-watchlist-hits"
    method = "POST"

    try:
        processed_item = preprocess_watchlist_hits(item)
        # send_to_queue('violation-scheduler-queue', (values, "INSERT"))
        await process_watchlist_hits(processed_item)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message="Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Watchlist hits Data processed and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message="Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/watchlist/watchlist-hits-drilldown", tags=["Watchlist"])
async def get_watchlist_drilldown(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
        chart_type: Optional[Literal["column", "line"]] = "column"
):
    try:
        # Log request details
        log_request(
            endpoint="/watchlist/watchlist-hits-drilldown",
            method="GET",
            status="Processing",
            message="Received request for watchlist hits drilldown",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
                "chart_type": chart_type,
            },
        )

        # Generate filter conditions
        where_clause, values = watchlist_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
            drill_down=drill_down,
        )
        logger.debug(f"Generated where_clause: {where_clause}")
        logger.debug(f"Values: {values}")

        # Construct query
        query = select_total_watchlist_hits(drill_down, where_clause)

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Determine chart type
        if chart_type == "column":
            fusioncharts_data = select_watchlist_hits_fusion_chart("scrollColumn2D", drill_down, rows)
        elif chart_type == "line":
            fusioncharts_data = select_watchlist_hits_fusion_chart("scrollline2d", drill_down, rows)
        else:
            raise ValueError("Invalid chart type. Please use 'column' or 'line'.")

        # Log success and increment metrics
        log_request(
            endpoint="/watchlist/watchlist-hits-drilldown",
            method="GET",
            status="200",
            message="Successfully processed request for watchlist hits drilldown",
        )
        increment_request_counter(
            endpoint="/watchlist/watchlist-hits-drilldown",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except ValueError as ve:
        logger.error("Validation error: %s", str(ve))
        increment_request_counter(
            endpoint="/watchlist/watchlist-hits-drilldown",
            method="GET",
            status="400",
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve),
        )
    except Exception as e:
        logger.error("Error occurred in /watchlist/watchlist-hits-drilldown: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/watchlist/watchlist-hits-drilldown",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )






@app.get("/watchlist/watchlist-hits-by-device", tags=["Watchlist"])
async def get_hits_by_device(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log request details
        log_request(
            endpoint="/watchlist/watchlist-hits-by-device",
            method="GET",
            status="Processing",
            message="Received request for watchlist hits by device",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit,
            },
        )

        # Generate filter conditions
        where_clause, values = watchlist_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Add top_limit condition to query
        top_clause = f"TOP {top_limit}" if top_limit > 0 else ""

        # Construct query
        query = f"""
                    SELECT {top_clause} device_name, count(*) as total_hits
                    FROM WATCHLISTHITS
                    {where_clause}
                    GROUP BY device_name
                    ORDER BY total_hits
                """

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Prepare FusionCharts data
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Hits by Device",
                "xAxisName": "Device",
                "yAxisName": "Watchlist Hits",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollColumn2D",
                "showHoverEffect": "1"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.device_name)
            })
            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_hits)
            })

        # Log success and increment metrics
        log_request(
            endpoint="/watchlist/watchlist-hits-by-device",
            method="GET",
            status="200",
            message="Successfully processed request for watchlist hits by device",
        )
        increment_request_counter(
            endpoint="/watchlist/watchlist-hits-by-device",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /watchlist/watchlist-hits-by-device: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/watchlist/watchlist-hits-by-device",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )


@app.get("/watchlist/watchlist-hits-by-vehicle-number", tags=["Watchlist"])
async def get_hits_by_vehicle_number(
        OrganisationId: int = Depends(get_OrganisationId),
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        top_limit: Optional[int] = 20
):
    try:
        # Log request details
        log_request(
            endpoint="/watchlist/watchlist-hits-by-vehicle-number",
            method="GET",
            status="Processing",
            message="Received request for watchlist hits by vehicle number",
            extra={
                "OrganisationId": OrganisationId,
                "device_name": device_name,
                "device_type": device_type,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "top_limit": top_limit,
            },
        )

        # Generate filter conditions
        where_clause, values = watchlist_filter_conditions(
            OrganisationId=OrganisationId,
            device_name=device_name,
            device_type=device_type,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Add top_limit condition to query
        top_clause = f"TOP {top_limit}" if top_limit > 0 else ""

        # Construct query
        query = f"""
                    SELECT {top_clause} vehicle_regnumber, count(*) as total_hits
                    FROM WATCHLISTHITS
                    {where_clause}
                    GROUP BY vehicle_regnumber
                    ORDER BY total_hits
                """

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Prepare FusionCharts data
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Hits by Vehicle Number",
                "xAxisName": "Vehicle Number",
                "yAxisName": "Watchlist Hits",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollColumn2D",
                "showHoverEffect": "1"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.vehicle_regnumber)
            })
            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_hits)
            })

        # Log success and increment metrics
        log_request(
            endpoint="/watchlist/watchlist-hits-by-vehicle-number",
            method="GET",
            status="200",
            message="Successfully processed request for watchlist hits by vehicle number",
        )
        increment_request_counter(
            endpoint="/watchlist/watchlist-hits-by-vehicle-number",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /watchlist/watchlist-hits-by-vehicle-number: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/watchlist/watchlist-hits-by-vehicle-number",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )



@app.post("/offence_payment_data", tags=["Vehicle Offence & Payment"], status_code=201)
async def create_offence_payment_data(item: OffencePaymentInstance):
    endpoint = "/offence_payment_data"
    method = "POST"


    try:

        processed_item = preprocess_insert_offence_payment_data(item)
        await process_offence_payment_message(processed_item)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message="Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Offence payment Data processed and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message="Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/update-offence-status", tags=["Vehicle Offence & Payment"], status_code=status.HTTP_201_CREATED)
async def update_offence_status(item: OffencePaymentUpdate):
    endpoint = "/update-offence-status"
    method = "POST"


    try:

        processed_item = preprocess_offence_payment_update_data(item)

        await process_offence_payment_update_message(processed_item)

        # Log success and increment counter
        log_request(endpoint, method, "200",
                    message="Data processed successfully",
                    extra=item.dict())  # Log the same data on success
        increment_request_counter(endpoint, method, "200")
        return {"message": "Offence status updated and stored successfully."}

    except Exception as e:
        # Log failure and increment error counter
        log_request(endpoint, method, "500",
                    message="Error in processing: {str(e)}",
                    extra=item.dict())  # Log input data and error message
        increment_request_counter(endpoint, method, "500")
        raise HTTPException(status_code=500, detail=str(e))


#
#

#
#
#
#
@app.get("/offence-payment/offence-payment-overview", tags=["Vehicle Offence & Payment"])
async def offence_payment_overview(
        OrganisationId: int = Depends(get_OrganisationId),
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        paymentstatusname: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
):
    try:
        # Log request details
        log_request(
            endpoint="/offence-payment/offence-payment-overview",
            method="GET",
            status="Processing",
            message="Received request for offence payment overview",
            extra={
                "OrganisationId": OrganisationId,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "paymentstatusname": paymentstatusname,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
            },
        )

        # Generate filter conditions
        where_clause, values = offence_payment_filter_conditions(
            OrganisationId=OrganisationId,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            paymentstatusname=paymentstatusname,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Construct query
        query = f"""
                        SELECT
                        COUNT(*) as total_offence_count,
                        COUNT(CASE WHEN paymentstatusname = 'Not Paid' THEN 1 END) AS not_paid_count,
                        COUNT(CASE WHEN paymentstatusname = 'Paid' THEN 1 END) AS paid_count,
                        SUM(fineamount) AS total_fine_amount,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount ELSE 0 END) AS total_paid_fine_amount,
                        SUM(CASE WHEN paymentstatusname = 'Not Paid' THEN fineamount ELSE 0 END) AS total_unpaid_fine_amount,
                        ROUND((SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount ELSE 0 END) / SUM(fineamount)) * 100, 2) AS conversion_rate_percentage
                        FROM dbo.offencepaymentdata
                        {where_clause}
                    """

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Prepare FusionCharts data
        fusioncharts_data = {
            "data": []
        }

        for row in rows:
            fusioncharts_data["data"].append({
                "Total Vehicle Offence": row[0] if row[0] is not None else 0,
                "Total Paid Offences": row[2] if row[2] is not None else 0,
                "Total Unpaid Offences": row[1] if row[1] is not None else 0,
                "Total Fine Amount": row[3] if row[3] is not None else 0,
                "Total Amount Received": row[4] if row[4] is not None else 0,
                "Total Receivables": row[5] if row[5] is not None else 0,
                "Conversion Rate": f"{row[6]:.2f}%" if row[6] is not None else "0.00%"
            })

        # Log success and increment metrics
        log_request(
            endpoint="/offence-payment/offence-payment-overview",
            method="GET",
            status="200",
            message="Successfully processed offence payment overview request",
        )
        increment_request_counter(
            endpoint="/offence-payment/offence-payment-overview",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /offence-payment/offence-payment-overview: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/offence-payment/offence-payment-overview",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )

@app.get("/offence-payment/offence_count_by_payment_status", tags=["Vehicle Offence & Payment"])
async def get_offence_count_by_payment_status(
        OrganisationId: int = Depends(get_OrganisationId),
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        paymentstatusname: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
):
    try:
        # Log request details
        log_request(
            endpoint="/offence-payment/offence_count_by_payment_status",
            method="GET",
            status="Processing",
            message="Received request for offence count by payment status",
            extra={
                "OrganisationId": OrganisationId,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "paymentstatusname": paymentstatusname,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
            },
        )

        # Generate filter conditions
        where_clause, values = offence_payment_filter_conditions(
            OrganisationId=OrganisationId,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            paymentstatusname=paymentstatusname,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Construct query
        query = f"""
                SELECT paymentstatusname, COUNT(*) AS paymentstatusname_count
                FROM dbo.offencepaymentdata
                {where_clause}
                GROUP BY paymentstatusname
                ORDER BY paymentstatusname
                """

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Offence Count by Payment Status",
                "use3DLighting": "0",
                "showPercentValues": "1",
                "decimals": "1",
                "showLegend": "1",
                "legendPosition": "bottom",
                "legendCaption": "Service Status",
                "useDataPlotColorForLabels": "1",
                "type": "pie2d",
                "theme": "fusion",
            },
            "data": []
        }

        for row in rows:
            fusioncharts_data["data"].append({
                "label": str(row[0]),
                "value": str(row[1]),
            })

        # Log success and increment metrics
        log_request(
            endpoint="/offence-payment/offence_count_by_payment_status",
            method="GET",
            status="200",
            message="Successfully processed offence count by payment status request",
        )
        increment_request_counter(
            endpoint="/offence-payment/offence_count_by_payment_status",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /offence-payment/offence_count_by_payment_status: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/offence-payment/offence_count_by_payment_status",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )


#
@app.get("/offence-payment/offence-count-by-status-drill-down", tags=["Vehicle Offence & Payment"])
async def get_offence_count_by_status(
        OrganisationId: int = Depends(get_OrganisationId),
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        paymentstatusname: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
        chart_type: Optional[Literal["combined", "stacked"]] = "combined"
):
    try:
        # Log request details
        log_request(
            endpoint="/offence-payment/offence-count-by-status-drill-down",
            method="GET",
            status="Processing",
            message="Received request for offence count by status drill down",
            extra={
                "OrganisationId": OrganisationId,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "paymentstatusname": paymentstatusname,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
                "chart_type": chart_type
            },
        )

        # Generate filter conditions
        where_clause, values = offence_payment_filter_conditions(
            OrganisationId=OrganisationId,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            paymentstatusname=paymentstatusname,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Construct query based on drill-down option
        query = select_offence_query(drill_down, where_clause)

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts based on the chart type
        if chart_type == "combined":
            fusioncharts_data = select_offence_fusion_chart_data_combined(drill_down, rows)
        elif chart_type == "stacked":
            fusioncharts_data = select_offence_fusion_chart_data_stacked(drill_down, rows)
        else:
            raise ValueError("Invalid chart type. Please use 'combined' or 'stacked'.")

        # Log success and increment metrics
        log_request(
            endpoint="/offence-payment/offence-count-by-status-drill-down",
            method="GET",
            status="200",
            message="Successfully processed offence count by status drill-down request",
        )
        increment_request_counter(
            endpoint="/offence-payment/offence-count-by-status-drill-down",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /offence-payment/offence-count-by-status-drill-down: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/offence-payment/offence-count-by-status-drill-down",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )

#
#
@app.get("/offence-payment/offence-amount-by-status-drill-down", tags=["Vehicle Offence & Payment"])
async def get_offence_amount_by_status(
        OrganisationId: int = Depends(get_OrganisationId),
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        paymentstatusname: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
        chart_type: Optional[Literal["combined", "stacked"]] = "combined"
):
    try:
        # Log request details
        log_request(
            endpoint="/offence-payment/offence-amount-by-status-drill-down",
            method="GET",
            status="Processing",
            message="Received request for offence amount by status drill down",
            extra={
                "OrganisationId": OrganisationId,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "paymentstatusname": paymentstatusname,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
                "chart_type": chart_type
            },
        )

        # Generate filter conditions
        where_clause, values = offence_payment_filter_conditions(
            OrganisationId=OrganisationId,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            paymentstatusname=paymentstatusname,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Construct query based on drill-down option
        query = select_offence_amount_query(drill_down, where_clause)

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts based on the chart type
        if chart_type == "combined":
            fusioncharts_data = select_offence_amount_fusion_chart_data_combined(drill_down, rows)
        elif chart_type == "stacked":
            fusioncharts_data = select_offence_amount_fusion_chart_data_stacked(drill_down, rows)
        else:
            raise ValueError("Invalid chart type. Please use 'combined' or 'stacked'.")

        # Log success and increment metrics
        log_request(
            endpoint="/offence-payment/offence-amount-by-status-drill-down",
            method="GET",
            status="200",
            message="Successfully processed offence amount by status drill-down request",
        )
        increment_request_counter(
            endpoint="/offence-payment/offence-amount-by-status-drill-down",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /offence-payment/offence-amount-by-status-drill-down: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/offence-payment/offence-amount-by-status-drill-down",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )


@app.get("/offence-payment/conversion-rate-drill-down", tags=["Vehicle Offence & Payment"])
async def get_conversion_rate(
        OrganisationId: int = Depends(get_OrganisationId),
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: Optional[str] = None,
        paymentstatusname: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[Literal["year", "quarter", "month", "day"]] = "year",
        chart_type: Optional[Literal["column", "line"]] = "line"
):
    try:
        # Log request details
        log_request(
            endpoint="/offence-payment/conversion-rate-drill-down",
            method="GET",
            status="Processing",
            message="Received request for conversion rate drill down",
            extra={
                "OrganisationId": OrganisationId,
                "year": year,
                "quarter": quarter,
                "month": month,
                "weeknumber": weeknumber,
                "weekday": weekday,
                "day": day,
                "vehicle_id": vehicle_id,
                "vehicle_regnumber": vehicle_regnumber,
                "paymentstatusname": paymentstatusname,
                "created_at_start": created_at_start,
                "created_at_end": created_at_end,
                "drill_down": drill_down,
                "chart_type": chart_type
            },
        )

        # Generate filter conditions
        where_clause, values = offence_payment_filter_conditions(
            OrganisationId=OrganisationId,
            year=year,
            quarter=quarter,
            month=month,
            weeknumber=weeknumber,
            weekday=weekday,
            day=day,
            vehicle_id=vehicle_id,
            vehicle_regnumber=vehicle_regnumber,
            paymentstatusname=paymentstatusname,
            created_at_start=created_at_start,
            created_at_end=created_at_end,
        )

        # Construct query based on drill-down option
        query = select_conversion_rate_query(drill_down, where_clause)

        # Execute query
        cursor = connection.cursor()
        cursor.execute(query, values)
        rows = cursor.fetchall()

        # Format the data for FusionCharts based on the chart type
        if chart_type == "column":
            fusioncharts_data = select_conversion_rate_fusion_chart("scrollColumn2D", drill_down, rows)
        elif chart_type == "line":
            fusioncharts_data = select_conversion_rate_fusion_chart("scrollline2d", drill_down, rows)
        else:
            raise ValueError("Invalid chart type. Please use 'column' or 'line'.")

        # Log success and increment metrics
        log_request(
            endpoint="/offence-payment/conversion-rate-drill-down",
            method="GET",
            status="200",
            message="Successfully processed conversion rate drill-down request",
        )
        increment_request_counter(
            endpoint="/offence-payment/conversion-rate-drill-down",
            method="GET",
            status="200",
        )

        return fusioncharts_data

    except Exception as e:
        # Log error and increment error metrics
        logger.error("Error occurred in /offence-payment/conversion-rate-drill-down: %s", str(e), exc_info=True)
        increment_request_counter(
            endpoint="/offence-payment/conversion-rate-drill-down",
            method="GET",
            status="500",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the request.",
        )








#Depends(get_OrganisationId)

# @app.get("/offence-payment/fine_amount_received_by_payment_medium")
# async def fine_amount_received_by_payment_medium(
#         OrganisationId: int = Depends(get_OrganisationId),
#         args: OffencePaymentFunctionArguments = Depends(),
#     ):
#     arg_dict = args.dict(exclude_unset=True)
#     print(arg_dict)
#     arg_dict["OrganisationId"] = OrganisationId
#     period = arg_dict["period"]
#
#     try:
#         where_clause, values = ParametersLogic(arg_dict)
#
#         if where_clause == "":
#             condition = "paymentstatusname = ?"
#             where_clause = f"WHERE {condition}"
#             values.append("Paid")
#         else:
#             where_clause += " AND paymentstatusname = ?"
#             values.append("Paid")
#
#         query = f"""
#                 SELECT paymentmediumname, SUM(fineamount) AS received_fine_amount
#                 FROM dbo.offencepaymentdata
#                 {where_clause}
#                 GROUP BY paymentmediumname;
#             """
#
#         cursor = connection.cursor()
#         cursor.execute(query, values)
#         rows = cursor.fetchall()
#
#         # Format the data for FusionCharts
#         fusioncharts_data = {
#             "chart": {
#                 "caption": "Received Fine Amount by Payment Medium",
#                 "subCaption": "Payment Media",
#                 "use3DLighting": "0",
#                 "showPercentValues": "1",
#                 "decimals": "1",
#                 "showLegend": "1",
#                 "legendPosition": "bottom",
#                 "legendCaption": "Payment Medium",
#                 "useDataPlotColorForLabels": "1",
#                 "type": "pie2d",
#                 "theme": "fusion",
#             },
#             "data": []
#         }
#
#         for row in rows:
#             fusioncharts_data["data"].append({
#                 "label": str(row.paymentmediumname),
#                 "value": row.received_fine_amount,
#
#             })
#
#         return fusioncharts_data
#
#     except Exception as e:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))