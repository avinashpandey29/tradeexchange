#####################################################################
# Data Generator.py                                                 #
# This file is used to generate large enough dataset and store      #
# in csv file since we are not using live API call. This data       #
# file will serve as an input for the Kafka Producer program        #
#####################################################################

import csv
import time
import json
import random
from kafka import KafkaProducer

# Define Kafka producer configuration
bootstrap_servers = ['localhost:9092']

# Define CSV file configuration
csv_file_name = 'trading_data.csv'
csv_field_names = ['timestamp', 'instrument_id', 'name', 'price', 'quantity']

# Define trading instruments and their attributes. Here, code can be done to read data from an API in real-time
instruments = [
    {
        "instrument_id": "INFY",
        "name": "Infosys",
        "price": 1500.0,
        "volatility": 0.01
    },
    {
        "instrument_id": "TCS",
        "name": "Tata Consultancy Services",
        "price": 2500.0,
        "volatility": 0.02
    },
    {
        "instrument_id": "Wipro",
        "name": "WIpro Inc.",
        "price": 800.0,
        "volatility": 0.03
    },
    {
        "instrument_id": "REL",
        "name": "Reliance",
        "price": 3500.0,
        "volatility": 0.01
    }
]

# Generate and send trading data to csv file.
while True:
    timestamp = int(time.time())  
    
    # Open the CSV file for appending
    with open(csv_file_name, mode='a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_field_names)
        
        # If the file is empty, write the header row
        if csv_file.tell() == 0:
            writer.writeheader()
        
        # Loop over each instrument in the list
        for instrument in instruments:
            # Generate a new price for the instrument
            price = instrument["price"] * (1 + random.uniform(-instrument["volatility"], instrument["volatility"]))  
            
            # Create a dictionary with the generated data
            data = {
                "timestamp": timestamp,
                "instrument_id": instrument["instrument_id"],
                "name": instrument["name"],
                "price": price,
                "quantity": random.randint(1, 10)
            }
            
            # Write the data to the CSV file
            writer.writerow(data)
    
    # Sleep for 60 seconds to generate data every minute
    time.sleep(60)
