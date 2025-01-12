import aio_pika
import json
import asyncio
import tracemalloc
from database import connect_to_database, RABBITMQ_CONNECTION_STRING
from datetime import datetime
from utils import *
#populate_devicehistory, update_or_insert_vehicle_tracking_data

# Enable tracing of memory allocations
tracemalloc.start()


async def callback(message: aio_pika.IncomingMessage):
    async with message.process():
        item = json.loads(message.body)
        try:
            production_date = datetime.now()

            item_dict = item.dict()
            item_dict['createdAt'] = production_date.isoformat()

            await process_message(item)
        except Exception as e:
            print(f"Error processing message: {e}")

async def start_consuming():
    connection_params = RABBITMQ_CONNECTION_STRING
    connection = await aio_pika.connect_robust(connection_params)
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue("dashboard-service-queue", durable=True)
    await queue.consume(callback)

    print('Waiting for raw location messages. To exit press CTRL+C')
    await asyncio.Future()  # This keeps the coroutine running



async def callback_violation_scheduler(message: aio_pika.IncomingMessage):
    async with message.process():
        item = json.loads(message.body)
        try:
            print("Violation Scheduler data received and queued for processing")
            processed_item = preprocess_insert_violation_scheduler_data(item)

            await process_violation_scheduler_message(processed_item)
        except Exception as e:
            print(f"Error processing violation scheduler message: {e}")

async def start_consuming_violation_scheduler():
    connection_params = RABBITMQ_CONNECTION_STRING
    connection = await aio_pika.connect_robust(connection_params)
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue("violation-scheduler-queue", durable=True)
    await queue.consume(callback_violation_scheduler)

    print('Waiting for violation scheduler messages. To exit press CTRL+C')
    await asyncio.Future()  # This keeps the coroutine running

#
# async def callback_camera_status(message: aio_pika.IncomingMessage):
#     async with message.process():
#         item = json.loads(message.body)
#         try:
#             print("Camera Status data received and queued for processing")
#             processed_item = preprocess_insert_camera_status_data(item)
#
#             await process_camera_staus_message(processed_item)
#         except Exception as e:
#             print(f"Error processing violation scheduler message: {e}")
#
# async def start_consuming_camera_status():
#     connection_params = RABBITMQ_CONNECTION_STRING
#     connection = await aio_pika.connect_robust(connection_params)
#     channel = await connection.channel()
#
#     await channel.set_qos(prefetch_count=1)
#     queue = await channel.declare_queue("camera-status-queue", durable=True)
#     await queue.consume(callback_camera_status)
#
#     print('Waiting for camera status messages. To exit press CTRL+C')
#     await asyncio.Future()  # This keeps the coroutine running
#

# async def callback_rejectedReasons(message: aio_pika.IncomingMessage):
#     async with message.process():
#         item = json.loads(message.body)
#         try:
#             print("Rejected reason snippet received and pushed for processing")
#             await process_rejected_reasons(item)
#         except Exception as e:
#             print(f"Error processing violation scheduler message: {e}")
#
# async def start_consuming_rejectedReasons():
#     connection_params = RABBITMQ_CONNECTION_STRING
#     connection = await aio_pika.connect_robust(connection_params)
#     channel = await connection.channel()
#
#     await channel.set_qos(prefetch_count=1)
#     queue = await channel.declare_queue("rejected-reasons-queue", durable=True)
#     await queue.consume(callback_rejectedReasons)
#
#     print('Waiting for rejected reasons messages. To exit press CTRL+C')
#     await asyncio.Future()  # This keeps the coroutine running


async def callback_offence_payment_scheduler(message: aio_pika.IncomingMessage):
    async with message.process():
        item = json.loads(message.body)
        try:
            print("Offence Payment data received and queued for processing")

            processed_item = preprocess_insert_offence_payment_data(item)

            await process_offence_payment_message(processed_item)
        except Exception as e:
            print(f"Error processing offence payment message: {e}")

async def start_consuming_offence_payment_data():
    connection_params = RABBITMQ_CONNECTION_STRING
    try:
        connection = await aio_pika.connect_robust(connection_params, timeout=30)  # Increased timeout
        channel = await connection.channel()

        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue("offence-payment-queue", durable=True)
        await queue.consume(callback_offence_payment_scheduler)

        print('Waiting for offence payment messages. To exit press CTRL+C')
        await asyncio.Future()  # This keeps the coroutine running

    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        await asyncio.sleep(5)  # Retry delay
        await start_consuming_offence_payment_data()  # Retry


async def callback_offence_payment_update_scheduler(message: aio_pika.IncomingMessage):
    async with message.process():
        item = json.loads(message.body)
        try:
            print("Offence Payment update data received and queued for processing")

            processed_item = preprocess_offence_payment_update_data(item)

            await process_offence_payment_update_message(processed_item)
        except Exception as e:
            print(f"Error processing offence payment update message: {e}")

async def start_consuming_offence_payment_update_data():
    connection_params = RABBITMQ_CONNECTION_STRING
    try:
        connection = await aio_pika.connect_robust(connection_params, timeout=30)  # Increased timeout
        channel = await connection.channel()

        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue("offence-payment-update-queue", durable=True)
        await queue.consume(callback_offence_payment_update_scheduler)

        print('Waiting for offence payment update messages. To exit press CTRL+C')
        await asyncio.Future()  # This keeps the coroutine running

    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}")
        await asyncio.sleep(5)  # Retry delay
        await start_consuming_offence_payment_data()  # Retry


async def callback_watchlist_hits(message: aio_pika.IncomingMessage):
    async with message.process():
        item = json.loads(message.body)
        try:
            print("Watchlist Hits data received and queued for processing")
            processed_item = preprocess_watchlist_hits(item)

            await process_watchlist_hits(processed_item)
        except Exception as e:
            print(f"Error processing watchlist hits message: {e}")

async def start_consuming_watchlist_hits():
    connection_params = RABBITMQ_CONNECTION_STRING
    connection = await aio_pika.connect_robust(connection_params)
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue("watchlist-hits-queue", durable=True)
    await queue.consume(callback_watchlist_hits)

    print('Waiting for watchlist hits messages. To exit press CTRL+C')
    await asyncio.Future()  # This keeps the coroutine running

async def callback_installation_health(message: aio_pika.IncomingMessage):
    async with message.process():
        item = json.loads(message.body)
        try:
            print("installation Health data received and queued for processing")
            processed_item = preprocess_installation_health_data(item)

            await process_installation_health_data(processed_item)
        except Exception as e:
            print(f"Error processing installation health message: {e}")

async def start_consuming_installation_health_data():
    connection_params = RABBITMQ_CONNECTION_STRING
    connection = await aio_pika.connect_robust(connection_params)
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue("installation-health-queue", durable=True)
    await queue.consume(callback_installation_health)

    print('Waiting for installation health messages. To exit press CTRL+C')
    await asyncio.Future()  # This keeps the coroutine running




async def callback_service_health(message: aio_pika.IncomingMessage):
    async with message.process():
        item = json.loads(message.body)
        try:
            print("service Health data received and queued for processing")
            processed_item = preprocess_service_health_data(item)

            await process_service_health_data(processed_item)
        except Exception as e:
            print(f"Error processing service health message: {e}")

async def start_consuming_service_health_data():
    connection_params = RABBITMQ_CONNECTION_STRING
    connection = await aio_pika.connect_robust(connection_params)
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=1)
    queue = await channel.declare_queue("service-health-queue", durable=True)
    await queue.consume(callback_service_health)

    print('Waiting for service health messages. To exit press CTRL+C')
    await asyncio.Future()  # This keeps the coroutine running



print('Consumer Service Started Successfully')

# Main function to run both consumers
async def main():  # Main function to gather both consumers
    await asyncio.gather(start_consuming(),
                         start_consuming_violation_scheduler(),
                         start_consuming_watchlist_hits(),
                         # start_consuming_installation_health_data(),
                         # start_consuming_service_health_data(),
                         # start_consuming_offence_payment_data(),
                         start_consuming_offence_payment_update_data()
                         )


if __name__ == "__main__":
    asyncio.run(main())  # Run the main coroutine
