#####################################################################
# Producer.py                                                       #
# This file is used to generate large enough dataset and store      #
# in csv file since we are not using live API call. This data       #
# file will serve as an input for the Kafka Producer program        #
#####################################################################

import csv
import json
import random
import time
from datetime import date, datetime, time as dt_time
from kafka import KafkaProducer

# Define Kafka producer configuration
bootstrap_servers = ['localhost:9092']
orders_topic = 'spa_orders'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Read trading instruments data from CSV file
instruments = []
with open('trading_data.csv', 'r') as file:
    reader = csv.DictReader(file)
    # Iterate over each row in the CSV file
    for row in reader:
        instrument = {
            'instrument_id': row['instrument_id'],
            'name': row['name'],
            'price': float(row['price']),
            'quantity': float(row['quantity'])
        }
        # Append each row to the list of instruments
        instruments.append(instrument)


# Function to get the valid until datetime for an order
def validity_time():
    today = datetime.today()
    # Define a valid until time of 4pm
    four_pm = dt_time(16, 0)
    # Combine today's date with 4pm to get the valid until datetime
    valid_until = datetime.combine(today, four_pm)
    return valid_until

# Function to check if current time is within trading hours
def within_trading_hours():
    current_time = datetime.now().time()
    return current_time >= dt_time(9, 0) and current_time <= dt_time(16,0)
    
# Function to check if current day is a weekday
def is_weekday():
    today = date.today()
    return today.weekday() < 5

# Generate and send trading data to consumer
while True:
    if within_trading_hours() and is_weekday():
        timestamp = int(time.time())
        
        # Iterate over each instrument
        for instrument in instruments:
            # Generate a random volatility value between 0 and 0.3
            volatility = random.uniform(0,0.3)
            
            # Calculate the price based on the instrument's price and the volatility
            price = instrument["price"] * (1 + volatility)
            
            # Create an order for the current instrument based on the latest price information available
            order = {
                "timestamp": timestamp,
                "symbol": instrument["instrument_id"],
                "name": instrument["name"],
                "side": random.choice(["buy", "sell"]),
                "quantity": random.randint(1, 10),
                "price": price,
                "valid_until": validity_time().strftime('%Y-%m-%d %H:%M:%S')
            }
            print("Order: " , order)
            
            # Send the order to the Kafka topic
            producer.send(orders_topic, value=order)
            
            # Generate next order after 15 seconds
            time.sleep(15)
    else:
        print("Off Business Hours")
        break