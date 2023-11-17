import os

from processor import get_json
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

host = os.environ.get('DB_HOST')
password = os.environ.get('DB_PASSWORD')

engine = create_engine(f"mysql+mysqlconnector://admin:{password}@{host}:3306/marketData")


def handler(event, context):
    try:
        symbol = event["symbol"]
        symbol = symbol.lower()
        data = get_json([symbol])
        df = pd.DataFrame(data[symbol]["Weekly Time Series"])

        symbols = []
        dates = []
        closes = []
        for key in df:


            # only use keys with "close" in it
            # clean_key = key.split(".", 2)[0]

            close = df[key]["4. close"]
            closes.append(close)
            symbols.append(symbol)
            dates.append(key)

        all_closes = {
            "Date": dates,
            "Symbol": symbols,
            "Close": closes
        }
        final_df = pd.DataFrame(all_closes)
        # print(f'--> final_df is: {final_df.head()}')
        sql_df = final_df.to_sql('stocks', engine, if_exists='append')
        print(f'--> sql_df is: {sql_df}')
        return True
    except Exception as e:
        print(f'--> error is: {e}')
        raise e


def store_symbols(symbol_list: list[str], index_symbol: str):
    last_updated = []
    index = []
    for symbol in symbol_list:
        last_updated.append("2023-17-11")
        index.append(index_symbol)
    symbol_frame = {
        "IndexName": index,
        "Symbol": symbol_list,
        "LastUpdated": last_updated
    }

    df = pd.DataFrame(symbol_frame)
    index.clear()
    symbol_list.clear()
    last_updated.clear()

    execute_sql(df, 'indexSymbols')

    print(f'--> df is: {df.head()}')
    return True


def execute_sql(df, table_name: str):
    try:
        df.to_sql(table_name, engine, if_exists='append')
    except Exception as e:
        print(f'--> error is: {e}')
        raise e


event = {"symbol": "aapl"}
context = {}
# handler(event, context)
