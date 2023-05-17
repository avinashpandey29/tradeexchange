from kafka import KafkaConsumer
import json
from datetime import date, datetime, timedelta, time as dt_time
import os
import csv

match_making_input = []
matched_trades = []
instruments = []
match_output_file = 'matched_trades.csv'
sma_output_file = 'sma_calculator.csv'

# create Kafka consumer
consumer = KafkaConsumer(
    'spa_orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Function to check if current time is within trading hours
def within_trading_hours():
    current_time = datetime.now().time()
    return current_time >= dt_time(9, 0) and current_time <= dt_time(16,0)
    
# Function to check if current day is a weekday
def is_weekday():
    today = date.today()
    return today.weekday() < 5

# Function to check if stock is valid based on its validity period
def is_valid_stock(trade_date):
    # Extract the date part from the `valid_until` string and convert it to a `date` object
    valid_until_date = datetime.strptime(trade_date,'%Y-%m-%d %H:%M:%S').date()

    # Compare the current date with the `valid_until` date
    return (date.today() == valid_until_date)


# function to apply match-making algorithm
def trade_matching(incoming_trade):
    flag = 0
    valid_flag = 1
    for each_trade in match_making_input:
        valid_flag = is_valid_stock(each_trade.get('valid_until'))
        if valid_flag:
            if (each_trade.get('side') != incoming_trade.get('side')) and (each_trade.get('symbol') == incoming_trade.get('symbol')):
                
                if each_trade.get('quantity') == incoming_trade.get('quantity') and each_trade.get('price') == incoming_trade.get('price'):
                    match_making_input.remove(each_trade) # remove each trade
                    flag = 1
                    # add data to matched trades
                    symbol = each_trade.get('symbol')
                    price = each_trade.get('price')
                    quantity = each_trade.get('quantity')
                    name = each_trade.get('name')
                    matched_trades.append({'symbol': symbol, 'name': name, 'price': price, 'quantity': quantity})
                    # Write matched trades to CSV file
                    with open(match_output_file, mode='a', newline='') as csv_file:
                        fieldnames = ['symbol', 'name', 'price','quantity']
                        writer = csv.DictWriter(csv_file, fieldnames)
                        if csv_file.tell() == 0:  # If file is empty, write header
                            writer.writeheader()
                        writer.writerow({'symbol': symbol, 'name': name, 'price': price, 'quantity': quantity})
                
                elif each_trade.get('side') == 'buy' and each_trade.get('quantity') <= incoming_trade.get('quantity') and each_trade.get('price') >= incoming_trade.get('price'):
                    match_making_input.remove(each_trade) # remove each trade
                    flag = 1
                    # add data to matched trades
                    symbol = each_trade.get('symbol')
                    price = incoming_trade.get('price')
                    quantity = each_trade.get('quantity')
                    name = each_trade.get('name')
                    matched_trades.append({'symbol': symbol, 'name': name, 'price': price, 'quantity': quantity})
                    # Write matched trades to CSV file
                    with open(match_output_file, mode='a', newline='') as csv_file:
                        fieldnames = ['symbol', 'name', 'price','quantity']
                        writer = csv.DictWriter(csv_file, fieldnames)
                        if csv_file.tell() == 0:  # If file is empty, write header
                            writer.writeheader()
                        writer.writerow({'symbol': symbol, 'name': name, 'price': price, 'quantity': quantity})
                
                elif each_trade.get('side') == 'sell' and each_trade.get('quantity') >= incoming_trade.get('quantity') and each_trade.get('price') <= incoming_trade.get('price'):
                    match_making_input.remove(each_trade) # remove each trade
                    flag = 1
                    # add data to matched trades
                    symbol = each_trade.get('symbol')
                    price = incoming_trade.get('price')
                    quantity = incoming_trade.get('quantity')
                    name = each_trade.get('name')
                    matched_trades.append({'symbol': symbol, 'name': name, 'price': price, 'quantity': quantity})
                    # Write matched trades to CSV file
                    with open(match_output_file, mode='a', newline='') as csv_file:
                        fieldnames = ['symbol', 'name', 'price','quantity']
                        writer = csv.DictWriter(csv_file, fieldnames)
                        if csv_file.tell() == 0:  # If file is empty, write header
                            writer.writeheader()
                        writer.writerow({'symbol': symbol, 'name': name, 'price': price, 'quantity': quantity})
                
                break
            else:
                pass
    
    if flag == 0 and valid_flag:
        match_making_input.append(incoming_trade)
    
    return matched_trades if not match_making_input else []

# function to calculate SMA closing price in 5-minute sliding window for the last 10 minutes
def sma_calculator(instrument):
    closing_prices = []
    with open(match_output_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['symbol'] == instrument:
                closing_prices.append(float(row['price']))
    if len(closing_prices) < 10:
        return None
    sma = sum(closing_prices[-10:]) / 10
    
    # Write SMA closing price to CSV file
    with open(sma_output_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([datetime.now(), instrument, sma])
    
    return sma

# Function to calculate the maximum profit stock
def profit_calculator():
    max_profit = 0
    max_profit_stock = None
    
    if not os.path.exists(match_output_file):
        return (None,0)
    
    for instrument in instruments:
        symbol = instrument['symbol']
        opening_prices = []
        closing_prices = []
        with open(match_output_file, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                #print(row)
                if 'symbol' in row and row['symbol'] == symbol:
                    closing_prices.append(float(row['price']))
                    if len(closing_prices) > 1:
                        # ln = reader.line_num()
                        # line_num is an attribute of the reader object that indicates the current line number of the CSV file
                        opening_prices.append(float(row['price'])) 
        if len(opening_prices) < 10 or len(closing_prices) < 10:
            continue
        avg_closing_price = sum(closing_prices[-10:]) / 10
        avg_opening_price = sum(opening_prices[-10:]) / 10
        profit = avg_closing_price - avg_opening_price
        if profit > max_profit:
            max_profit = profit
            max_profit_stock = instrument
    return (max_profit_stock,max_profit)



# process messages from Kafka topic
while True:
    if within_trading_hours() and is_weekday():
        print("Reading Orders Data from topic")
        for message in consumer:
            print(message)
            incoming_data = message.value 
            # match_making_input.append(message.value)
            # apply match-making algorithm
            print("Calling Match Making")
            instruments.append(incoming_data)
            trade_match_output = trade_matching(incoming_data)
            print(trade_match_output)
        
            # calculate SMA for each instrument in sma_input
            print("Calling SMA")
            for instrument in trade_match_output:
                #print(instrument)
                sma = sma_calculator(instrument['symbol'])
                if sma:
                    print(f"SMA for {instrument['name']} ({instrument['symbol']}): {sma:.2f}")
                else:
                    print(f"Not enough data for {instrument['name']} ({instrument['symbol']})")
            
            # calculate the maximum profit stock
            print("Calling Profit Calculator")
            (max_profit_stock,max_profit) = profit_calculator()
            if max_profit_stock:
                print(f"Stock giving maximum profit: {max_profit_stock['name']} ({max_profit_stock['symbol']}) ({max_profit})")
            
            else:
                print("No stock found with 10-minute closing prices.")
    else:
        print("Off Business Hours")
        break
