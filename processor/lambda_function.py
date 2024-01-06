import logging
import json
import os
import time
from array import array
import re
from collections import Counter
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

def process_exchange_json(data: dict):
    symbols = []
    exchange_name = []
    last_updated = []
    for company in data["nyse_list"]:
        symbols.append(company["ACT Symbol"])
        exchange_name.append("nyse")
        last_updated.append("2023-11-12")

    return {"symbol": symbols, "exchange": exchange_name, "last_updated": last_updated}


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


def calc_sharpe_ratio_sql(symbol: str, start_date: str, end_date: str) -> dict:
    try:
        statement = f"SELECT Close FROM stocks WHERE Symbol='{symbol}' AND Date >= '{start_date}' AND Date <= '{end_date}';"
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

            reply = {"sharpe_ratio": sharpe_ratio, "data_point_count": data_point_count, "start_date": start_date,
                     "end_date": end_date}

            return reply
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

# todo corr function is wrong??
def calculate_correlation(symbol1: str, symbol2: str):
    # when pulling the data, we have to compare the same dates
    db_helper = DbHelper()

    symbol1_data = []
    symbol2_data = []

    statement = f"SELECT week_date, close FROM stocks_week WHERE symbol='{symbol1}'"
    result = db_helper.execute_query(statement)
    for row in result:
        symbol1_data.append(row)

    if symbol1_data is [] or symbol1_data is None:
        return "error"

    statement = f"SELECT week_date, close FROM stocks_week WHERE symbol='{symbol2}'"
    result = db_helper.execute_query(statement)
    for row in result:
        symbol2_data.append(row)

    if symbol2_data is [] or symbol2_data is None or len(symbol2_data) == 0:
        return "error"

    df1 = pd.DataFrame(symbol1_data, columns=['Date', symbol1])
    df2 = pd.DataFrame(symbol2_data, columns=['Date', symbol2])

    merged_df = pd.merge(df1, df2, on='Date')

    keys = merged_df.keys()

    key1 = keys[1]
    key2 = keys[2]

    df1 = None
    df2 = None

    try:
        # todo is the corr number wrong because I'm missing data?
        correlation = merged_df[key1].corr(merged_df[key2])
    except Exception as e:
        print(f'--> error is: {e}')
        return "error"
        # raise e

    return correlation

def lookup_sector(symbol: str) -> str:
    if symbol:
        db_helper = DbHelper()
        statement = f"SELECT Sector FROM spdr_sectors WHERE Symbol='{symbol}';"
        result = db_helper.execute_query(statement)

        for row in result:
            sector = row[0]
            return sector


def get_sector_symbols(sector: str) -> list:
    if sector:
        db_helper = DbHelper()
        statement = f"SELECT Symbol FROM spdr_sectors WHERE Sector='{sector}';"
        result = db_helper.execute_query(statement)

        symbols = []

        for row in result:
            symbol = row[0]
            symbols.append(symbol)

        return symbols

def get_matching_sector_symbols(sector: str) -> str:
    if sector:
        db_helper = DbHelper()
        statement = f"SELECT match_name FROM index_matches WHERE index_name='{sector}';"
        result = db_helper.execute_query(statement)

        for row in result:
            symbol = row[0]
            return symbol


def sic_lookup_table(table: str):
    if table:
        api_key = os.environ.get('POLYGON_API_KEY')
        db_helper = DbHelper()

        statement = f"SELECT Symbol FROM exchangeSymbols;"

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


def process_efficiency_ratio(closes: list):
    date_list: list = []

    # closes: list = [360.2, 354.66, 363.57, 371.44, 368.24, 366.99, 374.4, 401.835, 412.19, 404.77, 394.58, 389.0,
    #                 392.08, 398.335, 396.12, 386.0, 384.37, 387.75, 389.01, 385.26, 398.7, 406.27, 404.505, 406.0,
    #                 409.32, 404.69, 404.89, 409.8, 421.69, 431.4452, 416.551, 421.04, 422.775, 425.75, 428.19, 426.3501,
    #                 430.05, 427.09, 428.595, 438.6, 447.6, 452.23, 452.525, 458.267, 460.6, 448.81]
    i = 0
    while i < len(closes):
        date_list.append(i)
        i += 1

    data = {"day": date_list, "close": closes}
    df = pd.DataFrame(data)

    numpy_close = np.log(closes)

    df['direction'] = df['close'].diff(3).abs()
    df['volatility'] = df['close'].diff().abs().rolling(window=3).sum()
    efficiency_ratio = df['direction'] / df['volatility']
    return efficiency_ratio


def translate_sic_code(code: str) -> str:
    if code:
        db_helper = DbHelper()
        statement = f"SELECT `Industry Title` FROM sicCodes WHERE `SIC Code`={code};"
        result = db_helper.execute_query(statement)

        for row in result:
            industry_title = row[0]
            return industry_title


def most_common_words(filename):
    # Read file
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, f"../lib/{filename}")
    try:
        with open(filename, 'r') as file:
            text = file.read()
    except Exception as e:
        print(f"most_common_words exception is {e}")
        return e

    # Tokenize the text into words (considering only alphanumeric words)
    words = re.findall(r'\b\w+\b', text.lower())

    # Count the frequency of each word
    word_count = Counter(words)

    return dict(word_count)


def clean_transcript_count(map_reduce: dict) -> dict:
    delete_words = ["the", "and", "a", "an", "in", "on", "at", "for", "with", "without", "about", "against", "between",
                    "into", "through", "during", "before", "after", "to", "from", "in", "out", "on", "off", "again",
                    "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both",
                    "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same",
                    "so", "than", "too", "very", "can", "will", "just", "should", "now", "we", "our", "of", "you",
                    "that", "i", "is", "are", "it", "s", "re", "as", "this", "have"]
    for word in delete_words:
        if word in map_reduce:
            del map_reduce[word]

    return map_reduce


def run_map_reduce(filepath: str):
    try:
        word_frequencies = most_common_words(filepath)

        cleaned_count = clean_transcript_count(word_frequencies)

        sorted_dict = dict(sorted(cleaned_count.items(), key=lambda item: item[1], reverse=True))

        return sorted_dict
    except Exception as e:
        print(f"run_map_reduce exception is {e}")
        return e


def run_transcript_correlation(filepath1: str, filepath2: str):
    # file = "../processor/LULU-Q32023.txt"
    filepath1 = "LULU-Q32023.txt"
    filepath2 = "LULU-Q22023.txt"
    try:
        lulu1 = run_map_reduce(filepath1)
        lulu2 = run_map_reduce(filepath2)

        comparison = {}

        keyword = []
        transcript1 = []
        transcript2 = []

        for word in lulu1:
            if word in lulu2:
                keyword.append(word)
                transcript1.append(lulu1[word])
                transcript2.append(lulu2[word])
                # comparison[word] = [lulu1[word],lulu2[word]]

        df_data = {"keyword": keyword, "transcript1": transcript1, "transcript2": transcript2}

        df = pd.DataFrame(df_data)

        print(df.head())
    except Exception as e:
        print(f"exception is {e}")
    print("--> done")
