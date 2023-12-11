import logging
import os
from datetime import datetime, timedelta

import requests
import json

from lib import DbHelper
from processor.lambda_function import process_qandl_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    try:
        if "symbol" in event:
            symbol = event["symbol"]
            symbol = symbol.lower()
            response = get_from_qandl(symbol)
            process_response = process_qandl_data(response, symbol)
            if not process_response.empty:
                return process_response
        # logger.info('## EVENT')
        # logger.info(event)
        #
        # all_symbols_data = {}
        #
        # if "symbols" in event:
        #     for symbol in event["symbols"]:
        #         try:
        #             symbol = symbol.lower()
        #             data = fetch_alpha_vantage_data(symbol)
        #             json_data = json.loads(data)
        #             all_symbols_data[symbol] = json_data
        #             resp = write_file(symbol, json_data)
        #             print(f'--> wrote file resp {resp}')
        #         except Exception as e:
        #             logger.error(e)
        #             print(f'error: {e}')
        #
        #     return {"response": "success"}
        # return {"response": "No symbols found"}
    except Exception as e:
        logger.error(e)
        raise e


def write_file(symbol: str, data: json) -> bool:
    try:
        str_data = json.dumps(data)
        f = open(f"/home/drich/financedata/{symbol}.json", "w")
        f.write(str_data)
        f.close()
        return True
    except Exception as e:
        logger.error(e)
        print(f'error: {e}')


def fetch_alpha_vantage_data(symbol: str):
    if symbol:
        api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={symbol}&apikey={api_key}'

        # Send a GET request
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the content of the response
            # print(response.text)
            return response.text
        else:
            # Print an error message if the request was not successful
            print(f"Request failed with status code {response.status_code}")


def get_symbol_list(filepath: str):
    symbols = []

    try:
        f = open(filepath, "r")
        for lines in f:
            symbols.append(lines.strip())
    except Exception as e:
        print(f'error: {e}')

    return symbols


def get_from_qandl(symbol: str):
    if symbol:
        api_key = os.environ.get('QANDL_API_KEY')
        url = f'https://www.quandl.com/api/v3/datasets/WIKI/{symbol}/data.json?api_key={api_key}'

        # Send a GET request
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the content of the response
            # print(response.text)
            return response.text
        else:
            # Print an error message if the request was not successful
            print(f"---> response fail: {response.text}")
            raise Exception(f"Request failed with message {response.text}")


def get_from_market_data(symbol: str, start_date: str, end_date: str):
    if symbol:
        # need to add time constraint or get limited results
        api_key = os.environ.get('MARKET_DATA_API_KEY')
        if start_date and end_date:
            url = f'https://api.marketdata.app/v1/stocks/candles/D/{symbol}?token={api_key}&from={start_date}&to={end_date}'
        else:
            current_date = datetime.now()
            one_year_ago = current_date - timedelta(days=365)
            one_year_ago_formatted = one_year_ago.strftime("%Y-%m-%d")
            current_date_formatted = current_date.strftime("%Y-%m-%d")
            url = f'https://api.marketdata.app/v1/stocks/candles/D/{symbol}?token={api_key}&from={start_date}&to={end_date}'
        # Send a GET request
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the content of the response
            # print(response.text)
            decoded = response.content.decode()
            json_data = json.loads(decoded)
            return json_data["l"]

        else:
            # Print an error message if the request was not successful
            response_message = response.text
            print(f"---> response fail: {response.text}")



def sic_lookup(symbol: str):
    if symbol:
        api_key = os.environ.get('POLYGON_API_KEY')

        url = f'https://api.polygon.io/v3/reference/tickers/AAPL?apiKey={api_key}'

        response = requests.get(url)

        if response.status_code == 200:
            json_data = json.loads(response.text)
            sic_code = json_data["results"]["sic_code"]
            return sic_code
        else:
            print(f"--> response fail: {response.text}")


