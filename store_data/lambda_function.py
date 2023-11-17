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
        data = get_json([symbol])
        df = pd.json_normalize(data[symbol]["Weekly Time Series"])

        symbols = []
        dates = []
        closes = []
        for key in df:
            if "close" not in key:
                continue

            # only use keys with "close" in it
            clean_key = key.split(".", 2)[0]
            for row in df[key]:
                # print(f'--> row is: {row}')
                symbols.append(symbol)
                dates.append(clean_key)
                closes.append(row)
        all_closes = {
            "Date": dates,
            "Symbol": symbols,
            "Close": closes
        }
        final_df = pd.DataFrame(all_closes)
        # print(f'--> final_df is: {final_df.head()}')
        sql_df = final_df.to_sql('stocks', engine, if_exists='append')
        print(f'--> sql_df is: {sql_df}')
    except Exception as e:
        print(f'--> error is: {e}')
        raise e


event = {"symbol": "aapl"}
context = {}
handler(event, context)
