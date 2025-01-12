# from databases import Database
# from models import Instance
# from typing import Optional
# from sqlalchemy import create_engine
from motor.motor_asyncio import AsyncIOMotorClient
import pyodbc
# import os
# from dotenv import load_dotenv
#
# load_dotenv()
# #
# #
# #
#
# RABBITMQ_CONNECTION_STRING = os.getenv('RABBITMQ_URL')
#
# # Establish mongodb connection
# # MONGO_DATABASE_URL = "mongodb+srv://ifeoluwa:Admin1234@clustervtm.hr3lps4.mongodb.net/"
# # MONGO_DATABASE_URL = "mongodb://root:rootpassword@mongodb:27017"
# MONGO_DATABASE_URL = os.getenv('MONGODB_URI')
# # # # print(MONGO_DATABASE_URL)
# db_connection_string = os.getenv('connection_string')
# #
# # #
# client = AsyncIOMotorClient(MONGO_DATABASE_URL)
# db = client.dashboard_service
# vehicle_tracking_collection = db.vehicle_tracking_data
#
# #
# db_watchlist = client["etraffica-db"]
# watchlist_collection = db_watchlist.watchlist
# #
# # #
# # # # # Establish SQL Server connection to local SQL server
# def connect_to_server():
#
#     return pyodbc.connect(db_connection_string)
#
#
#  # Establish database connection to local SQL Server
# def connect_to_database():
#
#     return pyodbc.connect(db_connection_string)


# Local connectin parameters
RABBITMQ_CONNECTION_STRING = "amqps://nmmjpcog:C3OHNzXiHShx5LKZih3btA12rTRZUq6y@possum.lmq.cloudamqp.com/nmmjpcog"
MONGO_DATABASE_URL = "mongodb+srv://ifeoluwa:Admin1234@clustervtm.hr3lps4.mongodb.net/"

client = AsyncIOMotorClient(MONGO_DATABASE_URL)
db = client.dashboard_service

vehicle_tracking_collection = db.vehicle_tracking_data

# # # Establish SQL Server connection to local SQL server
def connect_to_server():
    server = 'localhost,60313'
    username = 'sa'
    password = 'admin'
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password}'
    return pyodbc.connect(conn_str)


 # Establish database connection to local SQL Server
def connect_to_database():
    server = 'localhost,60313'
    database = 'dashboard_service'
    username = 'sa'
    password = 'admin'
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(conn_str)




#
# # PRODUCTION  Server CONNECTIONS
# #
#
# # Establish mongodb connection
# MONGO_DATABASE_URL = "mongodb://root:rootpassword@mongodb:27017/"
#
# RABBITMQ_CONNECTION_STRING = "amqp://guest:guest@rabbitmq:5672/"
#
#
# client = AsyncIOMotorClient(MONGO_DATABASE_URL)
# db = client.dashboard_service
# vehicle_tracking_collection = db.vehicle_tracking_data
#
# # db_watchlist = client["etraffica-db"]
# # watchlist_collection = db_watchlist.watchlist
#
#
# # Establish SQL Server connection to PRODUCTION SQL Server
# def connect_to_server():
#     server = 'sql-server'
#     username = 'sa'
#     password = 'Letmein7c3@r4!k2'
#     conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password}'
#     return pyodbc.connect(conn_str)
#
# # Establish database connection to PRODUCTION SQL Server
# def connect_to_database():
#     server = 'sql-server'
#     database = 'dashboard_service'
#     username = 'sa'
#     password = 'Letmein7c3@r4!k2'
#
#     # Use the correct ODBC driver name for SQL Server
#     conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
#     return pyodbc.connect(conn_str)
#



# #
# # Remote SQL Server


# MONGO_DATABASE_URL = "mongodb://root:rootpassword@8.tcp.eu.ngrok.io:23549/?tls=false"
# RABBITMQ_CONNECTION_STRING = "amqps://vtubulyb:C7W0n9PAuyNa7xZcUsdaMZc4PrgUI0a_@shark.rmq.cloudamqp.com/vtubulyb"
#
#
# client = AsyncIOMotorClient(MONGO_DATABASE_URL)
# db = client.dashboard_service
#
# vehicle_tracking_collection = db.vehicle_tracking_data
#
#
# # client_2 = AsyncIOMotorClient(MONGO_DATABASE_URL)
# # db_watchlist = client["etraffica-db"]
# # watchlist_collection = db_watchlist.watchlist
# # 4.tcp.eu.ngrok.io:12108
# # Establish SQL Server connection to Remote SQL Server
# def connect_to_server():
#     server = '8.tcp.eu.ngrok.io,23548'
#     username = 'sa'
#     password = 'Letmein7c3@r4!k2'
#     conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};UID={username};PWD={password}'
#     return pyodbc.connect(conn_str)
#
# # Establish database connection to Remote SQL Server
# def connect_to_database():
#     server = '8.tcp.eu.ngrok.io,23548'
#     database = 'dashboard_service'
#     username = 'sa'
#     password = 'Letmein7c3@r4!k2'
#
#     # Use the correct ODBC driver name for SQL Server
#     conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
#     return pyodbc.connect(conn_str)






# print("Start Initialization and connection")









# function to check and create database if it doesn't exist
def initialize_database():
    try:
        server_connection = connect_to_server()
        print("Server connection established!!!")
        server_connection.autocommit = True  # Disable transactions for the connection
        cursor = server_connection.cursor()
        print(("Server dashboard service connection Successfull"))

        # Check if the database exists, and create it if it does not
        cursor.execute("""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'dashboard_service')
            BEGIN
                CREATE DATABASE dashboard_service
            END
        """)

        # Check again to verify if the database now exists
        cursor.execute("SELECT name FROM sys.databases WHERE name = 'dashboard_service'")
        db_exists = cursor.fetchone()

        if db_exists:
            print('Database "dashboard_service" exists or was created successfully.')
        else:
            print('Failed to create the database.')

        cursor.close()
        server_connection.close()
    except pyodbc.Error as e:
        print("Error:", e)


# function to create tables in newly created database
def initialize_tables():
    connection = connect_to_database()
    cursor = connection.cursor()

    # Ensure tables are created
    table_creation_queries = [
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='devicehistory' AND xtype='U')
        CREATE TABLE devicehistory (
            id INT IDENTITY(1,1) PRIMARY KEY,
            createdat DATETIME,
            devicename NVARCHAR(100),
            year INT,
            quarter INT,
            month NVARCHAR(20),
            weeknumber INT,
            weekday NVARCHAR(20),
            day INT,
            vehicleId NVARCHAR(50),
            regnumber NVARCHAR(50),
            organisationid INT,
            make NVARCHAR(50),
            model NVARCHAR(50),
            color NVARCHAR(50),
            istaxi BIT,
            state NVARCHAR(50),
            latitude NVARCHAR(50),
            longitude NVARCHAR(50)
        )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='instancelograwdata' AND xtype='U')
        CREATE TABLE instancelograwdata (
            id INT IDENTITY(1,1) PRIMARY KEY,
            image_url NVARCHAR(255),
            organisation_id INT,
            organisation_name NVARCHAR(255),
            device_name NVARCHAR(100),
            device_type NVARCHAR(100),
            latitude NVARCHAR(50),
            longitude NVARCHAR(50),
            address NVARCHAR(255),
            vehicle_id NVARCHAR(50),
            vehicle_regnumber NVARCHAR(50),
            make NVARCHAR(50),
            model NVARCHAR(50),
            color NVARCHAR(50),
            istaxi BIT,
            isanonymous BIT,
            hasviolation BIT,
            state NVARCHAR(50),
            createdat DATETIME
        )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='violationschedulerdata' AND xtype='U')
        CREATE TABLE violationschedulerdata (
            id INT IDENTITY(1,1) PRIMARY KEY,
            violationschedulerid VARCHAR(255),
            transactionid VARCHAR(255),
            androidid VARCHAR(255),
            approvalmode INT,
            batchid VARCHAR(255),
            createdat DATETIME,
            year INT,
            quarter INT,
            month NVARCHAR(20),
            weeknumber INT,
            weekday NVARCHAR(20),
            day INT,
            device_name VARCHAR(255),
            device_type VARCHAR(255),
            offensename VARCHAR(255),
            handletype VARCHAR(255),
            instancelogid VARCHAR(255),
            isconvertedtooffense BIT,
            isprocessed BIT,
            latitude VARCHAR(255),
            longitude VARCHAR(255),
            address VARCHAR(255),
            offender VARCHAR(255),
            offensecode VARCHAR(255),
            organisation_id INT,
            organisation_name VARCHAR(255),
            rejection_reason VARCHAR(255),
            review VARCHAR(255),
            reviewcomment VARCHAR(255),
            reviewdate VARCHAR(255),
            reviewer VARCHAR(255),
            user_id VARCHAR(255),
            username VARCHAR(255),
            vehicle_id VARCHAR(255),
            vehicle_regnumber VARCHAR(255),
            vehicleoffenseid VARCHAR(255)
            
        )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='offencepaymentdata' AND xtype='U')
        CREATE TABLE offencepaymentdata (
            id INT IDENTITY(1,1) PRIMARY KEY,
            vehicleoffenceid NVARCHAR(255),
            organisation_id INT,
            organisation_name NVARCHAR(255),
            createdat DATETIME,
            createdat_year INT,
            createdat_quarter INT,
            createdat_month NVARCHAR(20),
            createdat_weeknumber NVARCHAR(20),
            createdat_weekday NVARCHAR(20),
            createdat_day NVARCHAR(20),
            updatedat DATETIME,
            updatedat_year INT,
            updatedat_quarter INT,
            updatedat_month NVARCHAR(20),
            updatedat_weeknumber NVARCHAR(20),
            updatedat_weekday NVARCHAR(20),
            updatedat_day NVARCHAR(20),
            vehicle_id NVARCHAR(255),
            vehicle_regnumber NVARCHAR(50),
            offence_id NVARCHAR(255),
            offence_name NVARCHAR(255),
            offence_code NVARCHAR(50),
            fineamount DECIMAL(18, 2),
            finepoint INT,
            paymentstatusid INT,
            paymentstatusname NVARCHAR(255),
            paymentmediumid INT,
            paymentmediumname NVARCHAR(255),
            invoice_id NVARCHAR(255),
            paymentpartner NVARCHAR(255),
            
         )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='installationstatus' AND xtype='U')
        CREATE TABLE installationstatus (
            id INT IDENTITY(1,1) PRIMARY KEY,
            installationId VARCHAR(100),
            installation_name NVARCHAR(100),
            latitude NVARCHAR(50),
            longitude NVARCHAR(50),
            address NVARCHAR(50),
            isonline BIT,
            organisation_id INT,
            organisation_name VARCHAR(255),
            createdat DATETIME,
            updatedat DATETIME
        )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='installationhealthhistory' AND xtype='U')
        CREATE TABLE installationhealthhistory (
            id INT IDENTITY(1,1) PRIMARY KEY,
            installationId VARCHAR(100),
            installation_name NVARCHAR(100),
            isonline BIT,
            organisation_id INT,
            organisation_name VARCHAR(255),
            createdat DATETIME,
            year INT,
            quarter INT,
            month NVARCHAR(20),
            weeknumber INT,
            weekday NVARCHAR(20),
            day INT,
        )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='servicestatus' AND xtype='U')
        CREATE TABLE servicestatus (
            id INT IDENTITY(1,1) PRIMARY KEY,
            installation_name NVARCHAR(100),
            service_name NVARCHAR(100),
            latitude NVARCHAR(50),
            longitude NVARCHAR(50),
            address NVARCHAR(50),
            isonline BIT,
            organisation_id INT,
            organisation_name VARCHAR(255),
            createdat DATETIME,
            updatedat DATETIME
        )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='servicehealthhistory' AND xtype='U')
        CREATE TABLE servicehealthhistory (
            id INT IDENTITY(1,1) PRIMARY KEY,
            installation_name NVARCHAR(100),
            service_name NVARCHAR(100),
            isonline BIT,
            organisation_id INT,
            organisation_name VARCHAR(255),
            createdat DATETIME,
            year INT,
            quarter INT,
            month NVARCHAR(20),
            weeknumber INT,
            weekday NVARCHAR(20),
            day INT,
        )
        """,
        """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='watchlisthits' AND xtype='U')
        CREATE TABLE watchlisthits (
            id INT IDENTITY(1,1) PRIMARY KEY,
            vehicle_id  NVARCHAR(50),
            vehicle_regnumber NVARCHAR(100),
            latitude NVARCHAR(50),
            longitude NVARCHAR(50),
            address NVARCHAR(50),
            device_name VARCHAR(255),
            organisation_id INT,
            organisation_name VARCHAR(255),
            createdat DATETIME,
            year INT,
            quarter INT,
            month NVARCHAR(20),
            weeknumber INT,
            weekday NVARCHAR(20),
            day INT,
        )
        """
    ]
    for query in table_creation_queries:
        cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()


# wrapping the function to create database and table
def initialize_database_and_tables():
    # initialize_master_database()
    initialize_database()
    initialize_tables()


