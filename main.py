import json
from enum import Enum

import pandas as pd

import hackathon
import retrieve_data
import processor
import store_data
from processor import process_market_data
from processor.lambda_function import sic_lookup_table, calculate_correlation
from retrieve_data import get_from_market_data
from store_data.lambda_function import store_sharpe_ratio


def retrieve_nasdaq_symbols():
    try:
        nasdaq_list = processor.get_json(["nasdaq_list"])
        df = pd.DataFrame(nasdaq_list)

        nasdaq_symbol_list = []

        for key in df.iterrows():
            key = key[1][0]
            symbol = key["Symbol"]
            nasdaq_symbol_list.append(symbol)
        print("nasdaq symbols collected")

    except Exception as e:
        print(f'--> error is: {e}')


def read_txt_symbols():
    filepath = "/home/drich/financedata/nasdaq_symbols.txt"
    symbol_list = []

    try:
        with open(filepath) as f:
            symbols = f.readlines()
            symbol_list = symbols[0].strip('][').split(', ')
            print(f'-->first is {symbol_list[0]}')

        store_response = store_data.store_symbols(symbol_list, "nasdaq")
        print(f'--> store_response is: {store_response}')
    except Exception as e:
        print(f'error: {e}')


def read_copied_txt_symbols():
    filepath = "/home/drich/financedata/spy_symbols.txt"

    try:
        with open(filepath) as f:
            raw_symbols = f.read().splitlines()
            symbols = []
            for sub in raw_symbols:
                if sub == "[" or sub == "]":
                    continue
                symbols.append(sub.replace(",", ""))
            print(f'--> symbol is {symbols[1]}')

        store_response = store_data.store_symbols(symbols, "spy")
        print(f'--> store_response is: {store_response}')

    except Exception as e:
        print(f'error: {e}')


def run_retrieve_symbols_data(symbols: list):
    if symbols:
        event = {"symbols": symbols}
        response = retrieve_data.handler(event, None)
        print(f'--> response is: {response}')
        return response


def run_store_symbols_data(symbols: list):
    for symbol in symbols:
        event = {"symbol": symbol}
        response = store_data.handler(event, None)
        print(f'--> response is: {response}')
        return response


if __name__ == '__main__':
    # response = retrieve_data.get_from_market_data("lulu")
    # print(response)

    command: str = "transcript_correlation"

    if command == "retrieve":
        response = retrieve_data.handler({"symbol": "lulu"}, None)
        storage_response = store_data.store_df(response, "stocks")

    elif command == "transcript_correlation":
        processor.run_transcript_correlation("./LULU-Q32023.txt", "./LULU-Q22023.txt")

    elif command == "efficiency":
        try:
            market_data = retrieve_data.get_from_market_data("lulu", "2023-10-5", "2023-12-8")
            response = processor.process_efficiency_ratio(market_data)
            df = pd.DataFrame(response, columns=["efficiency_ratio"])
            print(f'--> response is: {response}')
        except Exception as e:
            print(f'--> error is: {e}')

    elif command == "hackathon":
        response = hackathon.lambda_function.lambda_handler({"symbol": "LULU", "expression": "portfolio"}, None)

    elif command == "market_data":
        response = retrieve_data.get_from_market_data("lulu", "2023-10-5", "2023-12-8")

    elif command == "correlation":
        response = calculate_correlation("msft", "aapl")
        print(f'--> response is: {response}')

    # response: list[str] = get_from_market_data("msft")
    # record_count = response.count()
    # processor_response = process_market_data(response)

    elif command == "sharpe_ratio":
        response: dict = processor.calc_sharpe_ratio_sql("lulu", '2022-10-01', '2023-10-01')
        response = store_sharpe_ratio(response["sharpe_ratio"], "lulu", response["start_date"], response["end_date"])
        if response == True:
            print(f'-->sharpe ratio calculated and stored')
        else:
            print(f'-->error storing sharpe ratio response is: {response}')

    # response = sic_lookup("lulu")
    elif command == "sic_lookup":
        sic_lookup_table("indexSymbols")

    # symbols = ["lulu"]
    # run_retrieve_symbols_data(symbols)
    # run_store_symbols_data(symbols)
    # read_copied_txt_symbols()
    # read_txt_symbols()
    # retrieve_nasdaq_symbols()
