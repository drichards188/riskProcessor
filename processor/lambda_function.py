import logging
import json
import os
import time
from array import array

import numpy as np
import pandas as pd
import requests

from lib.db_helper import DbHelper

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    try:
        result = handle_get_sharpe_ratio("btc")
        print(f'--> result is: {result}')

        # logger.info('## ENVIRONMENT VARIABLES')
        # logger.info(os.environ['AWS_LAMBDA_LOG_GROUP_NAME'])
        # logger.info(os.environ['AWS_LAMBDA_LOG_STREAM_NAME'])
        logger.info('## EVENT')
        logger.info(event)
        return {"response": result}
    except Exception as e:
        logger.error(e)
        print(f'--> error is: {e}')
        raise e


def get_json(filenubs: list) -> dict:
    if filenubs:
        data: dict = {}
        for symbol in filenubs:
            symbol = symbol.lower()
            filepath = f"/home/drich/financedata/{symbol}.json"

            try:
                with open(filepath) as f:
                    json_data = json.load(f)
                    data[symbol] = json_data

            except Exception as e:
                print(f'error: {e}')
        return data


def calculate_week_difference(week_data):
    difference = float(week_data["4. close"]) - float(week_data["1. open"])
    rounded_difference = round(difference, 2)
    print(f'--> difference is: {rounded_difference}')
    return rounded_difference


def handle_get_sharpe_ratio(security_symbol: str) -> float:
    jsonData: dict = get_json([f'/home/drich/financedata/{security_symbol}.json'])

    day_difference_by_week: array[float] = []
    risk_free_rate: array[float] = []

    data_by_week = jsonData["Weekly Time Series"]
    for week in data_by_week:
        print(data_by_week[week])
        difference = calculate_week_difference(data_by_week[week])
        day_difference_by_week.append(difference)

    for day in day_difference_by_week:
        risk_free_rate.append(0.001)

    df = pd.DataFrame({
        'portfolio': day_difference_by_week,
        'risk_free': risk_free_rate
    })

    # Calculate excess returns
    df['excess_return'] = df['portfolio'] - df['risk_free']

    # Calculate the Sharpe Ratio
    sharpe_ratio = np.mean(df['excess_return']) / np.std(df['excess_return'])

    # Annualize the Sharpe Ratio
    annual_factor = np.sqrt(252)  # Use 252 for daily returns, 52 for weekly returns, 12 for monthly returns
    sharpe_ratio_annualized = sharpe_ratio * annual_factor

    print('Sharpe Ratio (Annualized):', sharpe_ratio_annualized)
    return sharpe_ratio_annualized


def calc_sharpe_ratio_sql(symbol: str) -> tuple:
    try:
        statement = f"SELECT Close FROM stocks WHERE Symbol='{symbol}'"
        db_helper = DbHelper()
        result = db_helper.execute_query(statement)

        if result is not None:
            results = []
            excess_returns = []
            risk_free_rate = 0.001
            data_point_count = 0
            sharpe_ratio = float()
            for row in result:
                # since selecting only Close, 0 column returned
                results.append(row[0])

            i = 1
            for close in results:
                if i + 1 < len(results):
                    difference = float(close) - float(results[i])
                    rounded_difference = round(difference, 2)
                    excess_returns.append(rounded_difference / risk_free_rate)
                    data_point_count += 1
                    i += 1

            sharpe_ratio = np.mean(excess_returns) / np.std(excess_returns)

            return sharpe_ratio, data_point_count
        else:
            raise Exception("No data found")
    except Exception as e:
        print(f'--> error is: {e}')
        raise e


def evaluate_sharpe_ratio(ratio: float) -> object:
    result = ""

    if ratio >= 2.99:
        result = "Excellent"
    elif ratio >= 2.00:
        result = "Very good"
    elif ratio >= 1.99:
        result = "Good"
    elif ratio >= 1.00:
        result = "Adequate"
    elif ratio < 1:
        result = "Bad"
    else:
        result = "eval error"

    print('result of sharpe eval {result}')
    return {"ratio": ratio, "eval": result}


def process_qandl_data(data: json, symbol: str) -> pd.DataFrame:
    if data:
        json_data = json.loads(data)
        column_rows = json_data["dataset_data"]["column_names"]
        specific_data = json_data["dataset_data"]["data"]

        final_pd = {}
        symbol_list = []
        close_list = []
        # index_list = []
        date_list = []
        i = 0
        while i < len(specific_data):
            symbol_list.append(symbol)
            close_list.append(specific_data[i][4])
            date_list.append(specific_data[i][0])
            final_pd[specific_data[i][0]] = [symbol, "nasdaq", specific_data[i][4]]
            i += 1

        final_pd = {"Date": date_list, "Symbol": symbol_list, "Close": close_list}
        df = pd.DataFrame(final_pd)
        return df

    else:
        return pd.DataFrame()


def process_market_data(data):
    return True


# todo
def calculate_correlation(symbol1: str, symbol2: str):
    # when pulling the data, we have to compare the same dates
    db_helper = DbHelper()

    symbol1_data = []
    symbol2_data = []

    statement = f"SELECT Date, Close FROM stocks WHERE Symbol='{symbol1}'"
    result = db_helper.execute_query(statement)
    for row in result:
        symbol1_data.append(row)

    statement = f"SELECT Date, Close FROM stocks WHERE Symbol='{symbol2}'"
    result = db_helper.execute_query(statement)
    for row in result:
        symbol2_data.append(row)

    df1 = pd.DataFrame(symbol1_data)
    df2 = pd.DataFrame(symbol2_data)


    merged_df = pd.merge(df1, df2, on='Date')

    df1 = []
    df2 = []

    try:
        correlation = merged_df["Close_x"].corr(merged_df["Close_y"])
    except Exception as e:
        print(f'--> error is: {e}')
        raise e

    return True


def sic_lookup_table(table: str):
    if table:
        api_key = os.environ.get('POLYGON_API_KEY')
        db_helper = DbHelper()

        statement = f"SELECT Symbol FROM indexSymbols;"

        result = db_helper.execute_query(statement)

        symbols = []
        industry_titles = []

        for row in result:
            symbol = row[0]
            symbols.append(symbol)

        i = 363
        while i < len(symbols):
            if i % 5 == 0 and i != 0:
                print(f"sleeping on {i}")
                time.sleep(61)
            s = symbols[i]
            clean_symbol = s.strip("'")

            url = f'https://api.polygon.io/v3/reference/tickers/{clean_symbol}?apiKey={api_key}'

            response = requests.get(url)

            if response.status_code == 200:
                json_data = json.loads(response.text)
                if "results" in json_data and "sic_code" in json_data["results"]:
                    sic_code = json_data["results"]["sic_code"]

                try:
                    industry_title = translate_sic_code(sic_code)

                    statement = f"UPDATE indexSymbols SET Industry='{industry_title}' WHERE Symbol='{s}';"

                    print(f"updating {s}")
                    db_helper.execute_update(statement)

                    print("Industry title updated")

                    i += 1
                except Exception as e:
                    print(f'--> error is: {e}')
                    industry_titles.append("None")
                    i += 1
            else:
                print(f"--> response fail: {response.text}")
                i += 1


def translate_sic_code(code: str) -> str:
    if code:
        db_helper = DbHelper()
        statement = f"SELECT `Industry Title` FROM sicCodes WHERE `SIC Code`={code};"
        result = db_helper.execute_query(statement)

        for row in result:
            industry_title = row[0]
            return industry_title
