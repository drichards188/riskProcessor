import logging
import json
from array import array

import numpy as np
import pandas as pd

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


def calculate_correlation():
    return
