"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Garrett Kopp
    Date: September 30, 2023

"""

import pika
import sys
import logging
from collections import deque
import datetime

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define global variables for temperature monitoring
SMOKER_TEMPERATURES = deque(maxlen=int(2.5 / 0.5))  # Max length for the smoker time window
FOOD_TEMPERATURES = deque(maxlen=int(10 / 0.5))    # Max length for the food time window

SMOKER_ALERT_THRESHOLD = 15.0  # Smoker temperature decrease threshold (in degrees F)
FOOD_STALL_THRESHOLD = 1.0     # Food temperature change threshold (in degrees F)

def process_temperature(body, temperature_name, temperature_deque, alert_threshold):
    try:
        # break up the message sent from the producer file into 2 strings
        timestamp_str, temperature_str = body.decode().split(',')
        # convert the timestamp string into a datetime object
        timestamp = datetime.datetime.strptime(timestamp_str, '%m/%d/%y %H:%M:%S')

        # Strip leading and trailing whitespace, handle 'None' values, and convert to float
        # Added this as I was having trouble with None values
        temperature = float(temperature_str.strip()) if temperature_str.strip().lower() != 'none' else None
        # Add timestamp and recorded temp to deque
        temperature_deque.append((timestamp, temperature))

        # Check for alert condition
        if len(temperature_deque) == temperature_deque.maxlen:
            # Handle None values that may occur
            valid_temperatures = [temp[1] for temp in temperature_deque if temp[1] is not None]

            # Handle missing temperature values
            if len(valid_temperatures) < 2:
                logging.warning(f"Not enough valid temperatures for {temperature_name} callback.")
                return
            # Record temperature values and the difference in time between them from the deque
            first_temp = valid_temperatures[0]
            last_temp = valid_temperatures[-1]
            time_diff = (temperature_deque[-1][0] - temperature_deque[0][0]).total_seconds() / 60.0  # Convert to minutes
            # initial temperature and time reading
            logging.info(f"{temperature_name} - First Temp: {first_temp}, Last Temp: {last_temp}, Time Diff: {time_diff} minutes")
            # if temperature and time change threshold is passed, log the event
            if abs(first_temp - last_temp) >= alert_threshold and time_diff <= get_time_window(temperature_name):
                logging.info(f"{temperature_name} Alert: Temperature change >= {alert_threshold}°F in {time_diff} minutes.")

    except Exception as e:
        logging.error(f"Error in {temperature_name} callback: {e}")

def get_time_window(temperature_name):
    if temperature_name == "Smoker":
        return 2.5  # Smoker time window is 2.5 minutes
    elif temperature_name == "Food A":
        return 10.0   # Food time window is 10 minutes
    elif temperature_name == "Food B":
        return 10.0   # Food time window is 10 minutes
    else:
        return None

def smoker_callback(ch, method, properties, body):
    process_temperature(body, "Smoker", SMOKER_TEMPERATURES, SMOKER_ALERT_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_a_callback(ch, method, properties, body):
    process_temperature(body, "Food A", FOOD_TEMPERATURES, FOOD_STALL_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_b_callback(ch, method, properties, body):
    process_temperature(body, "Food B", FOOD_TEMPERATURES, FOOD_STALL_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main(hn: str = "localhost"):
    """ Continuously listen for task messages on multiple named queues."""

    # create a list of queues and their associated callback functions
    queues_and_callbacks = [
        ("01-smoker", smoker_callback),
        ("02-food-A", food_a_callback),
        ("03-food-B", food_b_callback)
    ]

    try:
        # Modify this line to include your RabbitMQ username and password
        credentials = pika.PlainCredentials('guest', '12121212')

        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=hn, credentials=credentials)
        )

        # use the connection to create a communication channel
        channel = connection.channel()

        for queue_name, callback_function in queues_and_callbacks:
            # use the channel to declare a durable queue
            # a durable queue will survive a RabbitMQ server restart
            # and help ensure messages are processed in order
            # messages will not be deleted until the consumer acknowledges
            channel.queue_declare(queue=queue_name, durable=True)

            # The QoS level controls the # of messages
            # that can be in-flight (unacknowledged by the consumer)
            # at any given time.
            # Set the prefetch count to one to limit the number of messages
            # being consumed and processed concurrently.
            # This helps prevent a worker from becoming overwhelmed
            # and improve the overall system performance.
            # prefetch_count = Per consumer limit of unacknowledged messages
            channel.basic_qos(prefetch_count=1)

            # configure the channel to listen on a specific queue,
            # use the appropriate callback function,
            # and do not auto-acknowledge the message (let the callback handle it)
            channel.basic_consume(queue=queue_name, on_message_callback=callback_function, auto_ack=False)

        # log a message for the user
        logging.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logging.info("")
        logging.error("ERROR: Something went wrong.")
        logging.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("")
        logging.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logging.info("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")




