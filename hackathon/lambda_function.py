import os
from datetime import timedelta, datetime

import requests
import json
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar


def lambda_handler(event, context):
    print("---> lambda_handler is running")
    try:
        transcript_response = get_transcript(event["symbol"], event["expression"])
        changes = []
        percent_changes = []

        for date in transcript_response:
            print(f'--> transcript date is: {date}')

            next_day = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=3)
            next_day = next_day.strftime("%Y-%m-%d")

            closes = get_from_market_data(event["symbol"], date, next_day)

            change = closes[1] - closes[0]
            percent_change = change / closes[0] * 100
            percent_changes.append(str(round(percent_change, 2)))
            changes.append(str(round(change, 2)))

        print({"changes": changes, "percent": percent_changes})
        return {"changes": changes, "percent": percent_changes}

    except Exception as e:
        print(f'--> lambda_handler error is: {e}')
        raise e


def get_from_market_data(symbol: str, start_date: str, end_date: str):
    print("---> get_from_market_data is running")
    if symbol:
        # need to add time constraint or get limited results
        api_key = "Vnc3ZlBHSWRtMGtRMFFqb3BmM0V6NEJpRmMzdGRmeUo4ellEdE00TkJXVT0"
        if start_date and end_date:
            url = f'https://api.marketdata.app/v1/stocks/candles/D/{symbol}?token={api_key}&from={start_date}&to={end_date}'
        else:
            current_date = datetime.now()
            one_year_ago = current_date - timedelta(days=365)
            one_year_ago_formatted = one_year_ago.strftime("%Y-%m-%d")
            current_date_formatted = current_date.strftime("%Y-%m-%d")
            url = f'https://api.marketdata.app/v1/stocks/candles/D/{symbol}?token={api_key}&from={start_date}&to={end_date}'
        # Send a GET request
        try:
            response = requests.get(url)
        except Exception as e:
            print(f'--> market data error is: {e}')

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


def get_us_trading_days(start_date, end_date):
    print("---> get_us_trading_days is running")
    """
    Returns a list of US trading days (weekdays excluding US market holidays)
    between the given start and end dates.

    :param start_date: The start date in 'YYYY-MM-DD' format.
    :param end_date: The end date in 'YYYY-MM-DD' format.
    :return: A list of trading days between the start and end dates.
    """
    try:
        # Create a date range for weekdays between start and end dates
        date_range = pd.bdate_range(start=start_date, end=end_date)

        # Create a calendar of US Federal Holidays
        cal = USFederalHolidayCalendar()

        # Get a list of US Federal Holidays in the date range
        holidays = cal.holidays(start=date_range.min(), end=date_range.max())

        # Filter out the holidays from the date range
        trading_days = pd.to_datetime([d for d in date_range if d not in holidays])

        # Format dates as strings in 'YYYY-MM-DD' format
        formatted_dates = trading_days.strftime('%Y-%m-%d').tolist()

        return formatted_dates
    except Exception as e:
        print(f'--> get_us_trading_days error is: {e}')


def get_transcript(symbol: str, expression: str):
    print("---> get_transcript is running")
    try:
        if symbol:
            url = f'https://lpqj6fl9l8.execute-api.us-east-2.amazonaws.com/dev/transcript'
            data = {
                "symbol": symbol,
                "expression": expression
            }
            # Send a GET request
            response = requests.post(url, json.dumps(data))

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                return json_data

            else:
                # Print an error message if the request was not successful
                print(f"---> response fail: {response.text}")
                raise Exception(f"Request failed with message {response.text}")
    except Exception as e:
        print(f'--> get_transcript error is: {e}')
