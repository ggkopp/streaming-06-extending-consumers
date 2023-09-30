# streaming-06-extending-consumers

## Author: Garrett Kopp
## GitHub: https://github.com/ggkopp/streaming-06-extending-consumers

## Summary: 
This undertaking involves the implementation of a temperature surveillance system across three distinct sites: a smoker, a food stall designated as Food A, and another named Food B. It operates by fetching temperature data from a CSV file and subsequently dispatching this data to RabbitMQ queues, enabling real-time monitoring. The system is designed to trigger alerts when it detects noteworthy fluctuations in temperature, thereby ensuring stringent adherence to food safety standards and upholding the quality control of products.

### Install the following for this project:

Git,
Python 3.7+ (3.11+ preferred),
VS Code Editor,
VS Code Extension: Python (by Microsoft),
RabbitMQ Server installed and running locally,
pika 1.3.2 (or recent),
Logger,
Webbrowser,
sys,
Time,
csv

## Smart Smoker System
Read about the Smart Smoker system here: 

Smart Smoker
We read one value every half minute. (sleep_secs = 30)
smoker-temps.csv has 4 columns:

[0] Time = Date-time stamp for the sensor reading
[1] Channel1 = Smoker Temp --> send to message queue "01-smoker"
[2] Channe2 = Food A Temp --> send to message queue "02-food-A"
[3] Channe3 = Food B Temp --> send to message queue "02-food-B"

## We want know if:

The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)
Any food temperature changes less than 1 degree F in 10 minutes (food stall!)
Time Windows

Smoker time window is 2.5 minutes
Food time window is 10 minutes

## Deque Max Length

At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 

## Condition To monitor

If smoker temp decreases by 15 F or more in 2.5 min (or 5 readings)  --> smoker alert!
If food temp change in temp is 1 F or less in 10 min (or 20 readings)  --> food stall alert!

## Requirements

RabbitMQ server running
pika installed in your active environment
RabbitMQ Admin

See http://localhost:15672/Links to an external site.
General Design 

How many producer processes do you need to read the temperatures: One producer, built last project.
How many listening queues do we use: three queues, named as listed above.
How many listening callback functions do we need (Hint: one per queue): Three callback functions are needed.

## Screenshots of conditions for monitoring and RabbitMQ while implementinng the time window, deque max lengths.

![Alt text](<Emitter and Consumer with Food Alerts.png>)

![Alt text](<Emitter and Consumer with Smoker Alerts.png>)

![Alt text](<RabbitMQ and emmit.png>)