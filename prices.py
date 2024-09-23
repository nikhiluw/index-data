import yfinance as yf
import csv
import time
import os
import logging
import signal
import sys
from datetime import datetime

# Symbols for fetching prices
symbols = {
    "BankNifty": "^NSEBANK",
    "Nifty50": "^NSEI",
    "BTC/USDT": "BTC-USD",
    "ETH/USDT": "ETH-USD"
}

# File paths for saving data
file_paths = {
    "BankNifty": "./output/banknifty/",
    "Nifty50": "./output/nifty/",
    "BTC/USDT": "./output/btcusdt/",
    "ETH/USDT": "./output/ethusdt/"
}

# Set up logging
logging.basicConfig(filename='price_fetch_log.txt', level=logging.INFO)

# Function to fetch price
def get_price(symbol):
    data = yf.Ticker(symbol)
    current_price = data.history(period="1d")['Close'].iloc[0]
    return round(current_price, 2)  # Round the price to 2 decimal places

# Function to create or append data to CSV
def write_to_csv(file_name, price, ticker):
    file_exists = os.path.isfile(file_name)

    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)

        # Write the header only if the file does not exist
        if not file_exists:
            writer.writerow(["Timestamp", f"{ticker} Spot Price"])

        # Write the data row (price and current timestamp)
        writer.writerow([time.ctime(), price])

# Graceful shutdown
def signal_handler(sig, frame):
    print('Exiting gracefully...')
    sys.exit(0)

# Main function to fetch prices and save to CSV periodically
def main():
    # Create directories if they do not exist
    for path in file_paths.values():
        os.makedirs(os.path.dirname(path), exist_ok=True)

    refresh_interval = 1  # seconds

    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            for name, symbol in symbols.items():
                current_price = get_price(symbol)
                print(f"{name} Spot Price: {current_price}")

                # Generate file name based on current date
                current_date = datetime.now().strftime("%Y-%m-%d")
                csv_file = f"{file_paths[name]}_{name.replace('/', '-')}_{current_date}.csv"  # Replace '/' for filename safety

                # Write the price data to the CSV file
                write_to_csv(csv_file, current_price, name)

                # Log the successful fetch
                logging.info(f"Fetched {name} price: {current_price} at {time.ctime()}")

            # Wait for the next refresh
            time.sleep(refresh_interval)

        except Exception as e:
            logging.error(f"Error fetching or writing price: {e}")

if __name__ == "__main__":
    main()
