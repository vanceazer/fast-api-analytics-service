from fastapi import FastAPI, HTTPException, Query, status, Header, Depends
from database import connect_to_database, vehicle_tracking_collection
from datetime import datetime
from typing import Dict, Tuple, Optional, List
import json
import requests
# from pymongo import ASCENDING
# from bson import ObjectId
from opentelemetry.trace import get_tracer
from opentelemetry.metrics import get_meter
import logging



default_year = datetime.now().year


# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Tracer and Meter setup
tracer = get_tracer("eTraffika-data-service")
meter = get_meter("eTraffika-data-service")
request_counter = meter.create_counter(
    name="http_request_count",
    description="Counts the number of HTTP requests",
    unit="1"
)


def log_request(endpoint: str, method: str, status: str, message: str, extra=None):
    """Log HTTP request details with optional extra context."""
    log_data = {
        "endpoint": endpoint,
        "method": method,
        "status": status,
        "message": message,
    }
    if extra:
        # Remove any None values
        log_data.update({k: v for k, v in extra.items() if v is not None})
    logger.info(log_data)


def increment_request_counter(endpoint: str, method: str, status: str):
    """Increment the OpenTelemetry request counter."""
    request_counter.add(1, {"endpoint": endpoint, "method": method, "status": status})


def trace_operation(operation_name: str):
    """
    Decorator for tracing operations using OpenTelemetry.
    Usage:
    @trace_operation("operation-name")
    def my_function():
        ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            with tracer.start_as_current_span(operation_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator




# function to populate the device history table in the SQL server db

async def populate_devicehistory(production_date, values: dict):
    try:

        # created_at = datetime.utcnow()
        created_at = production_date
        print(created_at)
        year = created_at.year
        print(year)
        quarter = (created_at.month - 1) // 3 + 1
        print(quarter)
        month = created_at.strftime("%B")
        print(month)
        weeknumber = created_at.isocalendar()[1]
        print(weeknumber)
        weekday = created_at.strftime("%A")
        print(weekday)
        day = int(created_at.day)
        print(day)

        devicehistory_query = """
                    INSERT INTO dbo.devicehistory (
                    createdat, devicename, year, quarter, month, weeknumber, weekday, day, vehicleId, regnumber,
                    organisationid, make, model, color, istaxi, state, latitude, longitude
                    )

                    VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?
                    )
                """

        parameters = (
            created_at,
            values['devise']['name'],
            year,
            quarter,
            month,
            weeknumber,
            weekday,
            day,
            values['vehicle']['id'],
            values['vehicle']['regNumber'],
            values['organisation']['id'],
            values['make'],
            values['model'],
            values['color'],
            values['isTaxi'],
            values['state'],
            values['location']['latitude'],
            values['location']['longitude']
        )


        connection = connect_to_database()
        cursor = connection.cursor()
        print("Connection to device history made!!!")
        cursor.execute(devicehistory_query, parameters)
        connection.commit()
        print("Committed data to device history!!!")
        # cursor.close()
        connection.close()

    except Exception as e:
        print(f"Error in populate_devicehistory: {e}")



# function to populate vehicle tracking collection in mongodb
async def update_or_insert_vehicle_tracking_data(production_date, values: dict):
    try:
        current_time = production_date    # Use the provided date or the current time
        # time_now = datetime.utcnow()

        vehicle_data = await vehicle_tracking_collection.find_one({"vehicle.id": values['vehicle']['id']})
        print("Connection to vehicle tracking history made!!!")
        location_entry = {
            "device_name": values['devise']['name'],
            "device_type": values['devise']['deviceType'],
            "longitude": values['location']['longitude'],
            "latitude": values['location']['latitude'],
            "address": values['location']['address'],
            "organisation_id": values['organisation']['id'],
            "organisation_name": values['organisation']['name'],
            "created_date": current_time
        }

        if vehicle_data:
            print("Updating existing vehicle location record")
            # Update existing vehicle location record
            update_query = {"vehicle.id": values['vehicle']['id']}
            update_values = {
                "$push": {"location_history": location_entry},
                "$set": {"updated_date": current_time}
            }
            await vehicle_tracking_collection.update_one(update_query, update_values)
            print("Committed data to vehicle tracking history!!!")
        else:
            print("Inserting new vehicle location record")
            # Insert new vehicle location record
            new_vehicle_location_data = {
                "vehicle": {
                    "id": values['vehicle']['id'],
                    "regNumber": values['vehicle']['regNumber']
                },
                "location_history": [location_entry],
                "created_date": current_time,
                "updated_date": current_time
            }
            await vehicle_tracking_collection.insert_one(new_vehicle_location_data)
            print("Committed data to vehicle tracking history!!!")

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))



async def process_message(item):
    print("Starting function....")
    connection = connect_to_database()
    print("Connected to DB")
    query = """
        INSERT INTO dbo.instancelograwdata (
            image_url, 
            organisation_id, 
            organisation_name, 
            device_name, 
            device_type,
            latitude, 
            longitude, 
            address, 
            vehicle_id, 
            vehicle_regnumber, 
            make, 
            model, 
            color, 
            istaxi, 
            isanonymous, 
            hasviolation, 
            state, 
            createdat
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    item['createdAt'] = datetime.fromisoformat(item['createdAt'])

    parameters = (
        item['imageUrl'],
        item['organisation']['id'],
        item['organisation']['name'],
        item['devise']['name'],
        item['devise']['deviceType'],
        item['location']['latitude'],
        item['location']['longitude'],
        item['location']['address'],
        item['vehicle']['id'],
        item['vehicle']['regNumber'],
        item['make'],
        item['model'],
        item['color'],
        item['isTaxi'],
        item['isAnonymous'],
        item['hasViolation'],
        item['state'],
        item['createdAt']
    )

    print("Parameters Inputted....")
    cursor = connection.cursor()
    cursor.execute(query, parameters)
    connection.commit()

    print("Query Committed")
    await populate_devicehistory(item['createdAt'], item)

    await update_or_insert_vehicle_tracking_data(item['createdAt'], item)

    cursor.close()
    connection.close()
    print("Created raw data Successfully")

    # return status.HTTP_201_CREATED


def preprocess_insert_violation_scheduler_data(item):
# production_date = datetime.now(tz=ZoneInfo("Africa/Lagos"))
    try:
        # production_date = datetime.now()
        # production_date = generate_random_utc_date()

        created_at = datetime.now()
        print(created_at)
        year = created_at.year
        print(year)
        quarter = (created_at.month - 1) // 3 + 1
        print(quarter)
        month = created_at.strftime("%B")
        print(month)
        weeknumber = created_at.isocalendar()[1]
        print(weeknumber)
        weekday = created_at.strftime("%A")
        print(weekday)
        day = int(created_at.day)
        print(day)


        values = item.dict()
        values['createdat'] = created_at.isoformat()
        values['year'] = year
        values['quarter'] = quarter
        values['month'] = month
        values['weeknumber'] = weeknumber
        values['weekday'] = weekday
        values['day'] = day



        # Change the key from 'imageUrl' to 'image_url' in the values dictionary
        values['violationschedulerid'] = values.pop('id')
        values['transactionid'] = values.pop('transactionId')
        values['androidid'] = values.pop('androidId')
        values['approvalmode'] = values.pop('approvalMode')
        values['handletype'] = values.pop('handleType')
        values['instancelogid'] = values.pop('instanceLogId')
        values['isconvertedtooffense'] = values.pop('isConvertedToOffense')
        values['isprocessed'] = values.pop('isProcessed')
        values['offensecode'] = values.pop('offenseCode')

        # Handle nested parameters: organisation
        values['organisation_id'] = values['organisation']['id']
        values['organisation_name'] = values['organisation']['name']
        del values['organisation']
        # Handle nested parameters: devise
        values['device_name'] = values['devise']['name']
        values['device_type'] = values['devise']['deviceType']
        del values['devise']

        # Handle nested parameters: location
        values['latitude'] = values['location']['latitude']
        values['longitude'] = values['location']['longitude']
        values['address'] = values['location']['address']
        del values['location']

        # Handle nested parameters: vehicle
        values['vehicle_id'] = values['vehicle']['id']
        values['vehicle_regnumber'] = values['vehicle']['regNumber']
        del values['vehicle']


        return (values, "INSERT")

    except:

        updated_date = datetime.now()

        values = item.dict()
        #print(values)
        values['approvalmode'] = values.pop('approval_mode')
        values['reviewer'] = values.pop('reviewer')
        values['userid'] = values['user']['id']
        values['username'] = values['user']['name']
        values['rejection_reason'] = json.dumps(values.pop('reason'))
        values['reviewcomment'] = values.pop('review_comment')
        values['reviewDate'] = updated_date.isoformat()
        values['isprocessed'] = 1
        if values['approvalmode'] == 2:
            values['isconvertedtooffense'] = 1
        else:
            values['isconvertedtooffense'] = 0

        return (values, "UPDATE")


async def process_violation_scheduler_message(item):
    item_status = item[1]
    item = item[0]
    # print(item_status)
    # print(item)

    connection = connect_to_database()
    offence_endpoint = 'https://etraffica-core.ngrok.app/api/v1/Offense/organisation/code'
    # offence_endpoint = 'http://core/api/v1/Offense/organisation/code'




    if item_status == "INSERT":
        # offence_code = item['offensecode']
        # item['organisation_id']
        body = {
                "code": item['offensecode'],
                "organisationId": int(item['organisation_id'])
            }

        print(body)
        response = requests.post(offence_endpoint, json=body)
        # print(response.status_code)
        # print(offence.json())
        if response.status_code == 200:
            print('Offence data retrieved successfully')
            response_data = response.json()  # Parse the response JSON
            item['offensename'] = response_data['offense']['name']

        else:
            print(f'Error Code : {response.status_code}')
            print("No matching offense found in the etraffica-db Offenses table.")
            item['offensename'] = None


        query = """
            INSERT INTO dbo.violationschedulerdata (
                violationschedulerid, transactionid, androidid, approvalmode, createdat, year, quarter, month,
                weeknumber, weekday, day, device_name, device_type, handletype, instancelogid, isconvertedtooffense,
                isprocessed, latitude, longitude, address, offensename,  offensecode, organisation_id,
                organisation_name, vehicle_id, vehicle_regnumber
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        item['createdat'] = datetime.fromisoformat(item['createdat'])

        parameters = (
            item['violationschedulerid'],
            item['transactionid'],
            item['androidid'],
            item['approvalmode'],
            item['createdat'],
            item['year'],
            item['quarter'],
            item['month'],
            item['weeknumber'],
            item['weekday'],
            item['day'],
            item['device_name'],
            item['device_type'],
            item['handletype'],
            item['instancelogid'],
            item['isconvertedtooffense'],
            item['isprocessed'],
            item['latitude'],
            item['longitude'],
            item['address'],
            item['offensename'],
            item['offensecode'],
            item['organisation_id'],
            item['organisation_name'],
            item['vehicle_id'],
            item['vehicle_regnumber'],
        )
        print(parameters)

        cursor = connection.cursor()
        cursor.execute(query, parameters)
        connection.commit()
        cursor.close()
        connection.close()
        print("Created Violation Scheduler data Successfully")

    elif item_status == "UPDATE":
        print("IN")
        print(item["isprocessed"])
        # Serialize rejected_reason to JSON format if it's an array
        # item['rejected_reason'] = json.dumps(item['rejected_reason'])

        query = f"""
                    UPDATE dbo.violationschedulerdata
                    SET
                        approvalmode = '{item["approvalmode"]}',
                        reviewer = '{item["reviewer"]}',  
                        reviewdate = '{item["reviewDate"]}',
                        isprocessed = ({item["isprocessed"]}),
                        username = '{item["username"]}',
                        user_id = '{item["userid"]}',
                        rejection_reason = '{item["rejection_reason"]}',
                        reviewcomment = '{item["reviewcomment"]}',
                        isconvertedtooffense = '{item["isconvertedtooffense"]}'
                    WHERE violationschedulerid = '{item["violation_scheduler_id"]}';
                """
        print("OUT")
        # parameters = (item["reviewDate"], item["isprocessed"], item["reviewer"], item["user_id"], item["violation_id"])

        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
        print("Updated Violation Scheduler data Successfully")



# async def process_rejected_reasons(item):
#     production_date = datetime.now()
#     # createdat = production_date.isoformat()
#     #createdat = datetime.fromisoformat(createdat)
#
#     connection = connect_to_database()
#     cursor = connection.cursor()
#
#     parameters = (item, production_date)
#     query = """
#          INSERT INTO dbo.rejectReasons (
#                 reasonname, createdat
#             )
#             VALUES (?, ?)
#         """
#     cursor.execute(query, parameters)
#     connection.commit()
#     cursor.close()
#     connection.close()



def preprocess_insert_offence_payment_data(item):
    try:
        # production_date = datetime.now(tz=ZoneInfo("Africa/Lagos"))
        # production_date = datetime.now()
        # production_date = generate_random_utc_date()

        created_at = datetime.now()
        print(created_at)
        year = created_at.year
        print(year)
        quarter = (created_at.month - 1) // 3 + 1
        print(quarter)
        month = created_at.strftime("%B")
        print(month)
        weeknumber = created_at.isocalendar()[1]
        print(weeknumber)
        weekday = created_at.strftime("%A")
        print(weekday)
        day = int(created_at.day)
        print(day)

        values = item.dict()
        values['createdat'] = created_at.isoformat()
        values['createdat_year'] = year
        values['createdat_quarter'] = quarter
        values['createdat_month'] = month
        values['createdat_weeknumber'] = weeknumber
        values['createdat_weekday'] = weekday
        values['createdat_day'] = day

        # values['offencepaymentid'] = values.pop('id')
        values['vehicleoffenceid'] = values.pop('vehicle_offence_id')

        # Handle nested parameters: organisation
        values['organisation_id'] = values['organisation']['id']
        values['organisation_name'] = values['organisation']['name']
        del values['organisation']

        # Handle nested parameters: vehicle
        values['vehicle_id'] = values['vehicle']['id']
        values['vehicle_regnumber'] = values['vehicle']['regNumber']
        del values['vehicle']

        # Handle nested parameters: offence
        values['offence_id'] = values['offence']['id']
        values['offence_name'] = values['offence']['name']
        values['offence_code'] = values['offence']['code']
        values['fineamount'] = values['offence']['fine_amount']
        values['finepoint'] = values['offence']['fine_point']
        del values['offence']

        values['invoice_id'] = values.pop('invoice_id')

        values['paymentstatusid'] = values['payment_status']['id']
        values['paymentstatusname'] = values['payment_status']['name']
        del values['payment_status']





        return (values)
    except:
     print('Error encountered')


async def process_offence_payment_message(item):
    try:
        connection = connect_to_database()

        query = """
                INSERT INTO dbo.offencepaymentdata (
                    vehicleoffenceid, createdat, createdat_year, createdat_quarter, createdat_month,
                    createdat_weeknumber, createdat_weekday, createdat_day, organisation_id, organisation_name,
                    vehicle_id, vehicle_regnumber, offence_id, offence_name, offence_code, fineamount, finepoint,
                    paymentstatusid, paymentstatusname, invoice_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        item['createdat'] = datetime.fromisoformat(item['createdat'])

        parameters = (
            item['vehicleoffenceid'],
            item['createdat'],
            item['createdat_year'],
            item['createdat_quarter'],
            item['createdat_month'],
            item['createdat_weeknumber'],
            item['createdat_weekday'],
            item['createdat_day'],
            item['organisation_id'],
            item['organisation_name'],
            item['vehicle_id'],
            item['vehicle_regnumber'],
            item['offence_id'],
            item['offence_name'],
            item['offence_code'],
            item['fineamount'],
            item['finepoint'],
            item['paymentstatusid'],
            item['paymentstatusname'],
            item['invoice_id']
        )

        cursor = connection.cursor()
        cursor.execute(query, parameters)
        connection.commit()
        cursor.close()
        connection.close()

        print("Created Vehicle Offence data Successfully")

    except Exception as e:
        print(f"Error inserting into the database: {e}")
        raise





def preprocess_offence_payment_update_data(item):
    updated_date = datetime.now()
    print(updated_date)
    year = updated_date.year
    print(year)
    quarter = (updated_date.month - 1) // 3 + 1
    print(quarter)
    month = updated_date.strftime("%B")
    print(month)
    weeknumber = updated_date.isocalendar()[1]
    print(weeknumber)
    weekday = updated_date.strftime("%A")
    print(weekday)
    day = int(updated_date.day)
    print(day)

    values = item.dict()
    values['updatedat'] = updated_date.isoformat()
    values['updatedat_year'] = year
    values['updatedat_quarter'] = quarter
    values['updatedat_month'] = month
    values['updatedat_weeknumber'] = weeknumber
    values['updatedat_weekday'] = weekday
    values['updatedat_day'] = day

    # print(values)
    # Handle nested parameters: payment status
    values['paymentstatusid'] = values['payment_status']['id']
    values['paymentstatusname'] = values['payment_status']['name']
    del values['payment_status']
    # print(values['paymentstatusid'])

    # Handle nested parameters: payment medium
    values['paymentmediumid'] = values['payment_medium']['id']
    values['paymentmediumname'] = values['payment_medium']['name']
    del values['payment_medium']

    values['invoice_id'] = values.pop('invoice_id')
    values['paymentpartner'] = values.pop('payment_partner')
    print('Preprocessing done')
    print(values)

    return (values)


async def process_offence_payment_update_message(item):
    try:
        connection = connect_to_database()

        updatedat = datetime.strptime(item["updatedat"], "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
                UPDATE dbo.offencepaymentdata
                SET
                    updatedat = '{updatedat}',  -- Correctly formatted datetime
                    updatedat_year = {item["updatedat_year"]},
                    updatedat_quarter = {item["updatedat_quarter"]},
                    updatedat_month = '{item["updatedat_month"]}',
                    updatedat_weeknumber = {item["updatedat_weeknumber"]},
                    updatedat_weekday = '{item["updatedat_weekday"]}',
                    updatedat_day = {item["updatedat_day"]},
                    paymentstatusid = {item["paymentstatusid"]},
                    paymentstatusname = '{item["paymentstatusname"]}',
                    paymentmediumid = {item["paymentmediumid"]},
                    paymentmediumname = '{item["paymentmediumname"]}',
                    paymentpartner = '{item["paymentpartner"]}'
                WHERE invoice_id = '{item["invoice_id"]}';
            """

        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        connection.close()
        print("Updated Offence Payment data Successfully")

    except Exception as e:
        print(f"Error inserting offence payment update data into the database: {e}")
        raise

def is_None(value):
    if value == 0:
        return True
    elif value == 'None':
        return True
    else:
        return False


def year_key(dict_):
    dict_list = list(dict_.keys())
    if 'updatedat_year' in dict_list:
        return 'updatedat_year'
    else:
        return 'year'

def ParametersLogic(arg_dict):
    conditions = []
    values = []
    print(arg_dict)
    # Map the key "OrganisationId" to "organisation_id" in your SQL query
    column_mapping = {
        "OrganisationId": "organisation_id"
    }
    for key, value in arg_dict.items():
        none_state = is_None(value)
        print(key, none_state)

        column_name = column_mapping.get(key, key)  # Map the key to the corresponding column name
        if column_name == "organisation_id" and value != 13:
            conditions.append(f"{column_name} = ?")
            values.append(value)
        elif column_name == "organisation_id" and value == 13:
            # Skip adding this condition when OrganisationId == 13 to return all data
            pass
        elif key == "created_at_start"  and not none_state:
            datetime_format = "%Y-%m-%dT%H:%M"
            try:
                datetime_D = datetime.strptime(value, datetime_format)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                    detail="Invalid datetime format provided")
            conditions.append("createdat >= ?")
            values.append(datetime_D)
        elif key == "created_at_end" and not none_state:
            datetime_format = "%Y-%m-%dT%H:%M"
            try:
                datetime_D = datetime.strptime(value, datetime_format)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                    detail="Invalid datetime format provided")
            conditions.append("createdat <= ?")
            values.append(datetime_D)
        elif not none_state:
            conditions.append(f"{key} = ?")
            values.append(value)
            print(value)

    year_word = year_key(arg_dict)
    print("year_word is ", year_word)

    if (arg_dict["created_at_start"] == 'None') and (arg_dict[f'{year_word}'] == 0) and (arg_dict['period'] != "year"):
        conditions.append(f"{year_word} = ?")
        values.append(default_year)

    conditions_str = " AND ".join(conditions)
    where_clause = f"WHERE {conditions_str}" if conditions else ""
    print(where_clause, values)
    return where_clause, values



def violation_scheduler_filter_conditions(
        OrganisationId: int,
        device_name: Optional[str] = None,
        device_type: Optional[str] = None,
        approvalmode: Optional[int] = None,
        # name: Optional[str] = None,
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
        drill_down: Optional[str] = "year",
        user_id: Optional[str] = None
) -> Tuple[str, list]:
    conditions = []
    values = []

    # Apply OrganisationId filter if not 13
    if OrganisationId != 13:
        conditions.append("organisation_id = ?")
        values.append(OrganisationId)

    if device_name is not None:
        conditions.append("device_name = ?")
        values.append(device_name)
    if vehicle_id is not None:
        conditions.append("vehicle_id = ?")
        values.append(vehicle_id)
    if vehicle_regnumber is not None:
        conditions.append("vehicle_regnumber = ?")
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
    if weeknumber is not None:
        conditions.append("weeknumber = ?")
        values.append(weeknumber)
    if weekday is not None:
        conditions.append("weekday = ?")
        values.append(weekday)
    if device_type is not None:
        conditions.append("device_type = ?")
        values.append(device_type)
    if approvalmode is not None:
        conditions.append("approvalmode = ?")
        values.append(approvalmode)
    if day is not None:
        conditions.append("day = ?")
        values.append(day)
    if user_id is not None:
        conditions.append("user_id = ?")
        values.append(user_id)

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
        # conditions.append("year = ?")
        # values.append(today.year)
        conditions.append("month = ?")
        values.append(today.strftime("%B"))
        conditions.append("day = ?")
        values.append(today.day)

    if drill_down == "day" and created_at_start is None and created_at_end is None and year is None and month is None:
        today = datetime.today()
        # Append current year and month for the day drill down
        # conditions.append("year = ?")
        # values.append(today.year)
        conditions.append("month = ?")
        values.append(today.strftime("%B"))  # Full month name like 'January'



    if (created_at_start is None) and (year is None) and (drill_down != "year"):
        conditions.append("year = ?")
        values.append(default_year)

    conditions_str = " AND ".join(conditions)
    where_clause = f"WHERE {conditions_str}" if conditions else ""

    return where_clause, values



def installation_filter_conditions(
        OrganisationId: int,
        #installation_name: List[str],
        installation_id: Optional[str] = None,
        isonline: Optional[bool] = None,

) -> Tuple[str, list]:
    conditions = []
    values = []

    print("Start logic")
    # Apply OrganisationId filter if not 13
    if OrganisationId != 13:
        conditions.append("organisation_id = ?")
        values.append(OrganisationId)

    if installation_id is not None:
        conditions.append("installationId = ?")
        values.append(installation_id)

    # if installation_name is not None and len(installation_name) > 0:
    #     placeholders = ', '.join(['?' for _ in installation_name])
    #     conditions.append(f"installation_name IN ({placeholders})")
    #     values.extend(installation_name)

    if isonline is not None:
        conditions.append("isonline = ?")
        values.append(isonline)

    # print(installation_name)
    #
    # placeholders = ', '.join(['?' for _ in installation_name])
    # conditions.append(f"installation_name IN ({placeholders})")
    # values.extend(installation_name)


    # conditions_str = " AND ".join(conditions)
    #where_clause = f"WHERE {conditions_str}" if conditions else ""

    return conditions, values


def installation_filter_conditions_2(
        OrganisationId: int,
        installation_name: Optional[str] = None,
        installation_id: Optional[str] = None,
        isonline: Optional[bool] = None,

) -> Tuple[str, list]:
    conditions = []
    values = []

    # print("Start logic")
    # Apply OrganisationId filter if not 13
    if OrganisationId != 13:
        conditions.append("organisation_id = ?")
        values.append(OrganisationId)

    if installation_id is not None:
        conditions.append("installationId = ?")
        values.append(installation_id)

    if installation_name is not None:
        conditions.append("installation_name = ?")
        values.append(installation_name)

    if isonline is not None:
        conditions.append("isonline = ?")
        values.append(isonline)

    # print(installation_name)
    #
    # placeholders = ', '.join(['?' for _ in installation_name])
    # conditions.append(f"installation_name IN ({placeholders})")
    # values.extend(installation_name)


    conditions_str = " AND ".join(conditions)
    where_clause = f"WHERE {conditions_str}" if conditions else ""

    return where_clause, values



def service_filter_conditions(
        OrganisationId: int,
        # service_name: Optional[str] = None,
        isonline: Optional[bool] = None,

) -> Tuple[str, list]:
    conditions = []
    values = []

    # Apply OrganisationId filter if not 13
    if OrganisationId != 13:
        conditions.append("organisation_id = ?")
        values.append(OrganisationId)

    # if service_name is not None:
    #     conditions.append("service_name = ?")
    #     values.append(service_name)

    if isonline is not None:
        conditions.append("isonline = ?")
        values.append(isonline)


    # conditions_str = " AND ".join(conditions)
    # where_clause = f"WHERE {conditions_str}" if conditions else ""

    return conditions, values


def service_filter_conditions_2(
        OrganisationId: int,
        service_name: Optional[str] = None,
        isonline: Optional[bool] = None,

) -> Tuple[str, list]:
    conditions = []
    values = []

    # Apply OrganisationId filter if not 13
    if OrganisationId != 13:
        conditions.append("organisation_id = ?")
        values.append(OrganisationId)

    if service_name is not None:
        conditions.append("service_name = ?")
        values.append(service_name)

    if isonline is not None:
        conditions.append("isonline = ?")
        values.append(isonline)


    conditions_str = " AND ".join(conditions)
    where_clause = f"WHERE {conditions_str}" if conditions else ""

    return where_clause, values


def watchlist_filter_conditions(
        OrganisationId: int,
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
        drill_down: Optional[str] = "year",

) -> Tuple[str, list]:
    conditions = []
    values = []

    # Apply OrganisationId filter if not 13
    if OrganisationId != 13:
        conditions.append("organisation_id = ?")
        values.append(OrganisationId)

    if device_name is not None:
        conditions.append("device_name = ?")
        values.append(device_name)
    if vehicle_id is not None:
        conditions.append("vehicle_id = ?")
        values.append(vehicle_id)
    if vehicle_regnumber is not None:
        conditions.append("vehicle_regnumber = ?")
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
    if weeknumber is not None:
        conditions.append("weeknumber = ?")
        values.append(weeknumber)
    if weekday is not None:
        conditions.append("weekday = ?")
        values.append(weekday)
    if device_type is not None:
        conditions.append("device_type = ?")
        values.append(device_type)

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
        # conditions.append("year = ?")
        # values.append(today.year)
        conditions.append("month = ?")
        values.append(today.strftime("%B"))
        conditions.append("day = ?")
        values.append(today.day)

    if drill_down == "day" and created_at_start is None and created_at_end is None and year is None and month is None:
        today = datetime.today()
        # Append current year and month for the day drill down
        # conditions.append("year = ?")
        # values.append(today.year)
        conditions.append("month = ?")
        values.append(today.strftime("%B"))  # Full month name like 'January'

    if (created_at_start is None) and (year is None) and (drill_down != "year"):
        conditions.append("year = ?")
        values.append(default_year)

    conditions_str = " AND ".join(conditions)
    where_clause = f"WHERE {conditions_str}" if conditions else ""

    return where_clause, values


def preprocess_watchlist_hits(item):
# production_date = datetime.now(tz=ZoneInfo("Africa/Lagos"))
    try:

        created_at = datetime.now()
        print(created_at)
        year = created_at.year
        print(year)
        quarter = (created_at.month - 1) // 3 + 1
        print(quarter)
        month = created_at.strftime("%B")
        print(month)
        weeknumber = created_at.isocalendar()[1]
        print(weeknumber)
        weekday = created_at.strftime("%A")
        print(weekday)
        day = int(created_at.day)
        print(day)


        values = item.dict()
        values['createdat'] = created_at.isoformat()
        values['year'] = year
        values['quarter'] = quarter
        values['month'] = month
        values['weeknumber'] = weeknumber
        values['weekday'] = weekday
        values['day'] = day



        # Handle nested parameters: organisation
        values['organisation_id'] = values['organisation']['id']
        values['organisation_name'] = values['organisation']['name']
        del values['organisation']
        # Handle nested parameters: devise
        values['device_name'] = values['devise']['name']
        del values['devise']

        # Handle nested parameters: location
        values['latitude'] = values['location']['latitude']
        values['longitude'] = values['location']['longitude']
        values['address'] = values['location']['address']
        del values['location']

        # Handle nested parameters: vehicle
        values['vehicle_id'] = values['vehicle']['id']
        values['vehicle_regnumber'] = values['vehicle']['regNumber']
        del values['vehicle']

        print('Watchlist hits preprocessing done')


        return values

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))



async def process_watchlist_hits(item):
    # item = item[0]
    # # print(item_status)
    # # print(item)

    connection = connect_to_database()


    query = """
        INSERT INTO dbo.watchlisthits (
            createdat, year, quarter, month,
            weeknumber, weekday, day, device_name, latitude, longitude, address, organisation_id,
            organisation_name, vehicle_id, vehicle_regnumber
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    item['createdat'] = datetime.fromisoformat(item['createdat'])

    parameters = (
        item['createdat'],
        item['year'],
        item['quarter'],
        item['month'],
        item['weeknumber'],
        item['weekday'],
        item['day'],
        item['device_name'],
        item['latitude'],
        item['longitude'],
        item['address'],
        item['organisation_id'],
        item['organisation_name'],
        item['vehicle_id'],
        item['vehicle_regnumber']
    )
    # print(parameters)

    cursor = connection.cursor()
    cursor.execute(query, parameters)
    connection.commit()
    cursor.close()
    connection.close()
    print("Created Watchlist Hits data Successfully")


def preprocess_installation_health_data(item):
# production_date = datetime.now(tz=ZoneInfo("Africa/Lagos"))
    try:

        created_at = datetime.now()
        print(created_at)
        year = created_at.year
        print(year)
        quarter = (created_at.month - 1) // 3 + 1
        print(quarter)
        month = created_at.strftime("%B")
        print(month)
        weeknumber = created_at.isocalendar()[1]
        print(weeknumber)
        weekday = created_at.strftime("%A")
        print(weekday)
        day = int(created_at.day)
        print(day)


        values = item.dict()

        values['createdat'] = created_at.strftime('%Y-%m-%d %H:%M:%S')
        values['updatedat'] = created_at.strftime('%Y-%m-%d %H:%M:%S')


        values['year'] = year
        values['quarter'] = quarter
        values['month'] = month
        values['weeknumber'] = weeknumber
        values['weekday'] = weekday
        values['day'] = day


        # values['updatedat'] = created_at.isoformat()

        values['isonline'] = values.pop('isOnline')



        # Handle nested parameters: organisation
        values['organisation_id'] = values['organisation']['id']
        values['organisation_name'] = values['organisation']['name']
        del values['organisation']
        # Handle nested parameters: devise
        values['installation_name'] = values['installation']['name']
        values['installation_id'] = values['installation']['id']
        del values['installation']

        # Handle nested parameters: location
        values['latitude'] = values['location']['latitude']
        values['longitude'] = values['location']['longitude']
        values['address'] = values['location']['address']
        del values['location']

       # print('Watchlist hits preprocessing done')


        return values

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))



# async def insert_installation_status(item):
#     connection = connect_to_database()
#
#     query = """
#                 MERGE INTO installationstatus AS target
#                 USING (SELECT ? AS installation_id, ? AS installation_name, ? AS latitude, ? AS longitude,
#                               ? AS address, ? AS isonline, ? AS organisation_id, ? AS organisation_name,
#                               ? AS createdat, ? AS updatedat) AS source
#                 ON target.installation_id = source.installation_id
#                 WHEN MATCHED THEN
#                     UPDATE SET
#                         isonline = source.isonline,
#                         updatedat = source.updatedat
#                 WHEN NOT MATCHED THEN
#                     INSERT (installation_id, installation_name, latitude, longitude, address, isonline, organisation_id,
#                             organisation_name, createdat, updatedat)
#                     VALUES (source.installation_id, source.installation_name, source.latitude, source.longitude, source.address,
#                             source.isonline, source.organisation_id, source.organisation_name, source.createdat, source.updatedat);
#             """
#
#     # item['createdat'] = datetime.fromisoformat(item['createdat'])
#
#     parameters = (
#         str(item['installation_id']),
#         item['installation_name'],
#         item['latitude'],
#         item['longitude'],
#         item['address'],
#         item['isonline'],
#         item['organisation_id'],
#         item['organisation_name'],
#         item['createdat'],
#         item['updatedat']
#     )
#
#
#     print(query)
#     print(parameters)
#
#     cursor = connection.cursor()
#     cursor.execute(query, parameters)
#     connection.commit()
#     cursor.close()
#     connection.close()
#     print("Installation Status Inserted Successfully")

async def insert_installation_status(item):
    connection = connect_to_database()

    # Query to check if installation_id exists
    check_query = """
        SELECT COUNT(1)
        FROM installationstatus
        WHERE installationId = ?
    """

    # Insert query if installation_id does not exist
    insert_query = """
        INSERT INTO installationstatus (installationId, installation_name, latitude, longitude, address, isonline, organisation_id, 
                                        organisation_name, createdat, updatedat)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Update query if installation_id exists
    update_query = """
        UPDATE installationstatus
        SET isonline = ?, updatedat = ?
        WHERE installationId = ?
    """

    parameters = (
        str(item['installation_id']),
        item['installation_name'],
        item['latitude'],
        item['longitude'],
        item['address'],
        item['isonline'],
        item['organisation_id'],
        item['organisation_name'],
        item['createdat'],
        item['updatedat']
    )

    try:
        cursor = connection.cursor()

        # Check if installation_id exists
        cursor.execute(check_query, (str(item['installation_id']),))
        result = cursor.fetchone()

        if result[0] > 0:
            # Installation ID exists, perform an UPDATE
            cursor.execute(update_query, (item['isonline'], item['updatedat'], str(item['installation_id'])))
            print(f"Installation {item['installation_id']} updated successfully.")
        else:
            # Installation ID does not exist, perform an INSERT
            cursor.execute(insert_query, parameters)
            print(f"Installation {item['installation_id']} inserted successfully.")

        # Commit the transaction
        connection.commit()

    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()  # Rollback in case of error
    finally:
        # Close resources
        cursor.close()
        connection.close()
        print("Database connection closed.")



async def process_installation_health_data(item):
    # item = item[0]
    # # print(item_status)
    # # print(item)

    connection = connect_to_database()


    query = """
        INSERT INTO dbo.installationhealthhistory (
            createdat, year, quarter, month,
            weeknumber, weekday, day, installation_name, installationId, organisation_id, organisation_name, isonline
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    item['createdat'] = datetime.fromisoformat(item['createdat'])

    parameters = (
        item['createdat'],
        item['year'],
        item['quarter'],
        item['month'],
        item['weeknumber'],
        item['weekday'],
        item['day'],
        item['installation_name'],
        str(item['installation_id']),
        item['organisation_id'],
        item['organisation_name'],
        item['isonline']
    )

    cursor = connection.cursor()
    cursor.execute(query, parameters)
    connection.commit()
    print('Installation Health History Data Pushed successfully')

    await insert_installation_status(item)

    cursor.close()
    connection.close()
    print("Created Installation Health data Successfully")




def preprocess_service_health_data(item):
# production_date = datetime.now(tz=ZoneInfo("Africa/Lagos"))
    try:

        created_at = datetime.now()
        print(created_at)
        year = created_at.year
        print(year)
        quarter = (created_at.month - 1) // 3 + 1
        print(quarter)
        month = created_at.strftime("%B")
        print(month)
        weeknumber = created_at.isocalendar()[1]
        print(weeknumber)
        weekday = created_at.strftime("%A")
        print(weekday)
        day = int(created_at.day)
        print(day)


        values = item.dict()

        values['createdat'] = created_at.strftime('%Y-%m-%d %H:%M:%S')
        values['updatedat'] = created_at.strftime('%Y-%m-%d %H:%M:%S')


        values['year'] = year
        values['quarter'] = quarter
        values['month'] = month
        values['weeknumber'] = weeknumber
        values['weekday'] = weekday
        values['day'] = day


        # values['updatedat'] = created_at.isoformat()

        values['isonline'] = values.pop('isOnline')



        # Handle nested parameters: organisation
        values['organisation_id'] = values['organisation']['id']
        values['organisation_name'] = values['organisation']['name']
        del values['organisation']

        values['installation_name'] = values.pop('installation_name')
        values['service_name'] = values.pop('service_name')

        # Handle nested parameters: location
        values['latitude'] = values['location']['latitude']
        values['longitude'] = values['location']['longitude']
        values['address'] = values['location']['address']
        del values['location']

       # print('Watchlist hits preprocessing done')


        return values

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))




async def insert_service_status(item):
    connection = connect_to_database()

    query = """
                MERGE INTO servicestatus AS target
                USING (SELECT ? AS installation_name,? AS service_name, ? AS latitude, ? AS longitude, 
                              ? AS address, ? AS isonline, ? AS organisation_id, ? AS organisation_name,
                              ? AS createdat, ? AS updatedat) AS source
                ON target.service_name = source.service_name
                WHEN MATCHED THEN 
                    UPDATE SET 
                        isonline = source.isonline,
                        updatedat = source.updatedat
                WHEN NOT MATCHED THEN
                    INSERT (installation_name, service_name, latitude, longitude, address, isonline, organisation_id, 
                            organisation_name, createdat, updatedat)
                    VALUES (source.installation_name, source.service_name, source.latitude, source.longitude, source.address, 
                            source.isonline, source.organisation_id, source.organisation_name, source.createdat, source.updatedat);
            """

    # item['createdat'] = datetime.fromisoformat(item['createdat'])

    parameters = (
        item['installation_name'],
        item['service_name'],
        item['latitude'],
        item['longitude'],
        item['address'],
        item['isonline'],
        item['organisation_id'],
        item['organisation_name'],
        item['createdat'],
        item['updatedat']
    )


    # print(query)
    # print(parameters)

    cursor = connection.cursor()
    cursor.execute(query, parameters)
    connection.commit()
    cursor.close()
    connection.close()
    print("service Status Inserted Successfully")





async def process_service_health_data(item):
    # item = item[0]
    # # print(item_status)
    # # print(item)

    connection = connect_to_database()


    query = """
        INSERT INTO dbo.servicehealthhistory (
            createdat, year, quarter, month,
            weeknumber, weekday, day, installation_name, service_name, organisation_id, organisation_name, isonline
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    item['createdat'] = datetime.fromisoformat(item['createdat'])

    parameters = (
        item['createdat'],
        item['year'],
        item['quarter'],
        item['month'],
        item['weeknumber'],
        item['weekday'],
        item['day'],
        item['installation_name'],
        item['service_name'],
        item['organisation_id'],
        item['organisation_name'],
        item['isonline']
    )

    cursor = connection.cursor()
    cursor.execute(query, parameters)
    connection.commit()
    print('service Health History Data Pushed successfully')

    await insert_service_status(item)

    cursor.close()
    connection.close()
    print("Created service Health data Successfully")





def offence_payment_filter_conditions(
        OrganisationId: int,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[str] = None,
        weeknumber: Optional[int] = None,
        weekday: Optional[str] = None,
        day: Optional[int] = None,
        vehicle_id: Optional[str] = None,
        vehicle_regnumber: str = None,
        paymentstatusname: Optional[str] = None,
        created_at_start: Optional[str] = None,
        created_at_end: Optional[str] = None,
        drill_down: Optional[str] = "year"
) -> Tuple[str, list]:
    conditions = []
    values = []

    # Apply OrganisationId filter if not 13
    if OrganisationId != 13:
        conditions.append("organisation_id = ?")
        values.append(OrganisationId)

    if paymentstatusname is not None:
        conditions.append("device_name = ?")
        values.append(paymentstatusname)
    if vehicle_id is not None:
        conditions.append("vehicle_id = ?")
        values.append(vehicle_id)
    if vehicle_regnumber is not None:
        conditions.append("vehicle_regnumber = ?")
        values.append(vehicle_regnumber)
    if year is not None:
        conditions.append("createdat_year = ?")
        values.append(year)
    if quarter is not None:
        conditions.append("createdat_quarter = ?")
        values.append(quarter)
    if month is not None:
        conditions.append("createdat_month = ?")
        values.append(month)
    if weeknumber is not None:
        conditions.append("createdat_weeknumber = ?")
        values.append(weeknumber)
    if weekday is not None:
        conditions.append("createdat_weekday = ?")
        values.append(weekday)
    if day is not None:
        conditions.append("createdat_day = ?")
        values.append(day)

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

    # if drill_down == "hour" and created_at_start is None and created_at_end is None and year is None and month is None and day is None:
    #     today = datetime.today()
    #     # conditions.append("year = ?")
    #     # values.append(today.year)
    #     conditions.append("month = ?")
    #     values.append(today.strftime("%B"))
    #     conditions.append("day = ?")
    #     values.append(today.day)

    if drill_down == "day" and created_at_start is None and created_at_end is None and year is None and month is None:
        today = datetime.today()
        # Append current year and month for the day drill down
        # conditions.append("year = ?")
        # values.append(today.year)
        conditions.append("month = ?")
        values.append(today.strftime("%B"))  # Full month name like 'January'



    if (created_at_start is None) and (year is None) and (drill_down != "year"):
        conditions.append("year = ?")
        values.append(default_year)

    conditions_str = " AND ".join(conditions)
    where_clause = f"WHERE {conditions_str}" if conditions else ""

    return where_clause, values















