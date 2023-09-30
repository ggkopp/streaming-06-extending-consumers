"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Garrett Kopp
    Date: September 30, 2023

"""

import pika
import sys
import csv
import time
import logging
from datetime import datetime

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# RabbitMQ configuration
rabbit_host = 'localhost'
rabbit_port = 5672  # Default RabbitMQ port for message communication

# My login credentials
rabbitmq_username = 'guest'
rabbitmq_password = '12121212'

queues = {
    '01-smoker': 'Channel1',
    '02-food-A': 'Channel2',
    '03-food-B': 'Channel3'
}

# Function to send a message to a queue
def send_message(channel, queue_name, message):
    try:
        channel.basic_publish(exchange="", routing_key=queue_name, body=message)
        logging.info(f"Sent to {queue_name} Queue: {message}")
    except Exception as e:
        logging.error(f"Error sending message to {queue_name} Queue: {str(e)}")

# Function to check for significant smoker temperature decrease event
def check_smoker_alert(channel, prev_timestamp, prev_smoker_temp, timestamp, smoker_temp):
    if prev_timestamp is not None and prev_smoker_temp is not None:
        # Convert timestamps to datetime objects
        prev_timestamp = datetime.strptime(prev_timestamp, "%m/%d/%y %H:%M:%S")
        timestamp = datetime.strptime(timestamp, "%m/%d/%y %H:%M:%S")
        
        # Calculate the time difference in minutes
        time_change = (timestamp - prev_timestamp).total_seconds() / 60

        if time_change <= 2.5:
            if smoker_temp and smoker_temp != 'NONE':  # Check for empty or 'NONE'
                try:
                    prev_smoker_temp = float(prev_smoker_temp)
                    smoker_temp = float(smoker_temp)
                    temp_change = prev_smoker_temp - smoker_temp
                    if temp_change > 15:
                        send_message(channel, "01-smoker", "Smoker Alert!")
                except ValueError:
                    logging.error("Error converting temperature to float.")
                
# Function to read and send data from CSV
def main():
    try:
        # Create credentials with the provided username and password
        credentials = pika.PlainCredentials(username=rabbitmq_username, password=rabbitmq_password)

        # Create a blocking connection to the RabbitMQ server with credentials
        conn = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host, credentials=credentials))
        # Use the connection to create a communication channel
        ch = conn.channel()
        
        # Delete the queues if they exist
        ch.queue_delete(queue="01-smoker")
        ch.queue_delete(queue="02-food-A")
        ch.queue_delete(queue="03-food-B")

        # Use the channel to declare durable queues
        ch.queue_declare(queue="01-smoker", durable=True)
        ch.queue_declare(queue="02-food-A", durable=True)
        ch.queue_declare(queue="03-food-B", durable=True)

        prev_timestamp = None
        prev_smoker_temp = None

        # Define the CSV file name
        SMOKER_FILE_NAME = "smoker-temps.csv"

        # Open and read the CSV file
        with open(SMOKER_FILE_NAME, "r", newline="") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header row if it exists

            # Iterate over each row in the CSV and assign the values to variables
            for row in reader:
                timestamp = row[0]
                smoker_temp = row[1] if row[1] else "NONE"  # Send "NONE" if empty
                food_a_temp = row[2] if row[2] else "NONE"  # Send "NONE" if empty
                food_b_temp = row[3] if row[3] else "NONE"  # Send "NONE" if empty

                # Check for smoker temperature alert
                check_smoker_alert(ch, prev_timestamp, prev_smoker_temp, timestamp, smoker_temp)

                # Check if "Food A" and "Food B" temperatures are valid numbers before converting to float
                if food_a_temp != "NONE":
                    try:
                        food_a_temp = float(food_a_temp)
                    except ValueError:
                        logging.error("Error converting Food A temperature to float.")
                        food_a_temp = "NONE"
                
                if food_b_temp != "NONE":
                    try:
                        food_b_temp = float(food_b_temp)
                    except ValueError:
                        logging.error("Error converting Food B temperature to float.")
                        food_b_temp = "NONE"

                # Send messages to RabbitMQ queues
                send_message(ch, "01-smoker", f"{timestamp}, {smoker_temp}")
                send_message(ch, "02-food-A", f"{timestamp}, {food_a_temp}")
                send_message(ch, "03-food-B", f"{timestamp}, {food_b_temp}")

                prev_timestamp = timestamp
                prev_smoker_temp = smoker_temp

                # Wait for 30 seconds before processing the next batch of records
                time.sleep(.2)

                # Log the sent messages
                logging.info(f"Sent: Timestamp={timestamp}, Smoker Temp={smoker_temp}, Food A Temp={food_a_temp}, Food B Temp={food_b_temp}")

    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # Close the connection to the server
        conn.close()

# Standard Python idiom to indicate the main program entry point
if __name__ == "__main__":
    main()











