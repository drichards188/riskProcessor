import logging
import os

import requests
import json

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



