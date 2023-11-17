import json

import pandas as pd

import retrieve_data
import processor
import store_data


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
    symbols = ["msft"]
    run_retrieve_symbols_data(symbols)
    run_store_symbols_data(symbols)
    # read_copied_txt_symbols()
    # read_txt_symbols()
    # retrieve_nasdaq_symbols()
