import datetime
import json
import logging
import os.path
import sys

import requests
from tqdm import tqdm
import pandas as pd
import pyotp
import requests.exceptions
from SmartApi import SmartConnect

from config import *

obj = SmartConnect(api_key=apikey)


def login():
    print("")
    logging.info("LOGGING IN")
    try:
        data = obj.generateSession(username, password, pyotp.TOTP(token).now())
    except requests.exceptions.Timeout:
        logging.error("TIMEOUT ERROR OCCURED")
        return False, False
    if data is None:
        logging.error("LOGIN FAILED")
        return False, False
    try:
        refreshToken = data['data']['refreshToken']
    except TypeError:
        logging.error("LOGIN FAILED")
        return False, False
    auth_token = data['data']['jwtToken']
    try:
        feed_token = obj.getfeedToken()
    except requests.exceptions.Timeout:
        logging.error("TIMEOUT ERROR OCCURED")
        return False, False
    logging.info("LOGGED IN SUCCESSFULLY")
    return auth_token, feed_token


def get_stock_token(symbol, exchange="NSE"):
    print("")
    logging.info("LOOKING FOR TOKEN")

    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'

    if os.path.exists("token.json"):
        try:
            token_df = pd.read_json("token.json")
        except ValueError:
            d = requests.get(url).json()
            token_df = pd.DataFrame.from_dict(d)
            with open("token.json", "w") as f:
                json.dump(d, f)
    else:
        open("token.json", "x")  # Create the file
        d = requests.get(url).json()
        token_df = pd.DataFrame.from_dict(d)
        with open("token.json", "w") as f:
            json.dump(d, f)

    if exchange == "NSE":
        symbol += "-EQ"
    # Get the Token id for HDFC Bank
    data = token_df[
        (token_df['symbol'].str.contains(symbol, case=False)) & (token_df['exch_seg'].str.contains(exchange))]
    if data.empty:
        logging.info(f"TOKEN NOT FOUND FOR {symbol} IN {exchange}")
        return False, None
    try:
        token = int(''.join(str(token) for token in (data['token'].values)))
    except Exception as e:
        logging.error("ERROR EXTRACTING TOKEN : ", e)
        return False, None
    logging.info("TOKEN FOUND")
    return True, token


def get_stock_data(exchange, symbol, token, dates, timeperiods, path):
    print("")
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except Exception as e:
            logging.error("ERROR CREATING PATH : ", e)
            return False

    if os.path.exists(f"{path}\\{symbol}.xlsx"):
        logging.info("FILE ALREADY EXISTS")
        logging.info("REMOVING FILE")
        os.remove(f"{path}\\{symbol}.xlsx")
        logging.info("FILE REMOVED SUCCESSFULLY")

    try:
        writer = pd.ExcelWriter(f"{path}\\{symbol}.xlsx", engine='openpyxl',
                                datetime_format='DD-MM-YYYY HH:MM:SS', date_format='DD-MM-YYYY')
    except FileNotFoundError:
        open(f"{path}\\{symbol}.xlsx", "x")  # Create blank file
        writer = pd.ExcelWriter(f"{path}\\{symbol}.xlsx", engine='openpyxl', mode='a',
                                datetime_format='DD-MM-YYYY HH:MM:SS', date_format='DD-MM-YYYY')
    datas = {}
    print("")
    logging.info("GETTING DATA")
    total_iterations = len(timeperiods) * len(dates)  # Calculate total iterations
    with tqdm(total=total_iterations, desc="Fetching data") as pbar:  # Use tqdm for progress bar
        for interval in timeperiods:
            for date in dates:
                try:
                    historicParam = {
                        "exchange": exchange,
                        "symboltoken": token,
                        "interval": interval,
                        "fromdate": date[0],
                        "todate": date[1],
                    }
                    pbar.set_description(
                        f"GETTING DATA OF : {symbol} OF INTERVAL {interval}")
                    pbar.update(1)  # Update progress bar after each iteration
                    try:
                        api_response = obj.getCandleData(historicParam)
                    except requests.exceptions.Timeout:
                        logging.error("TIMEOUT ERROR OCCURED")
                        return False
                    data = api_response['data']
                    if api_response['status']:
                        pbar.set_postfix({"status": "SUCCESS"})
                    else:
                        pbar.set_postfix({"status": "FAILED"})
                        logging.error(
                            f"ERROR GETTING DATA OF : {symbol} FROM {date[0]} TO {date[1]} WITH INTERVAL {interval}")
                        logging.error(f"ERRORCODE : {api_response['errorcode']}, MESSAGE : {api_response['message']}")
                    columns = ['DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
                    df = pd.DataFrame(data, columns=columns)
                    df['DateTime'] = pd.to_datetime(df['DateTime'])
                    df.set_index('DateTime', inplace=True)
                    df.index = df.index.strftime('%d-%m-%Y %H:%M:%S')
                    datas.setdefault(interval, []).append(df)
                except Exception as e:
                    logging.error("ERROR GETTING DATA : ", e)
                    return False
    logging.info("GETTING DATA (SUCCESSFULLY)\n")
    logging.info(f'SAVING DATA IN {path}\\{symbol}.xlsx')
    for interval, data in datas.items():
        try:
            combined_data = pd.concat(data)
            combined_data.to_excel(writer, sheet_name=f"{symbol}_{interval}")
        except Exception as e:
            logging.error(f"ERROR SAVING DATAFRAME OF INTERVAL {interval}: ", e)
    writer.close()
    logging.info(f"DATA SAVED IN {path}\\{symbol}.xlsx (SUCCESSFULLY)")
    return True


def date_manager(start_date, end_date):
    date_list = []
    current_date = start_date
    end_date = (end_date + datetime.timedelta(days=1))
    while current_date <= end_date:
        next_date = current_date + datetime.timedelta(days=6)
        next_date = min(next_date, end_date)
        date_list.append([current_date.strftime("%Y-%m-%d %H:%M"), next_date.strftime("%Y-%m-%d %H:%M")])
        current_date = next_date + datetime.timedelta(days=1)
    return date_list


def input_manager():
    print("")
    while True:
        exchange = input("Enter Exchange (NSE, BSE): ").upper()
        if exchange not in ["NSE", "BSE"]:
            logging.error("INVALID EXCHANGE")
            continue
        break

    while True:
        symbol = (input("Enter Symbol: ")).upper()
        if not symbol.isalpha():
            logging.error("INVALID SYMBOL")
            continue
        break

    while True:
        from_date = input("Enter From Date (DD-MM-YYYY): ")
        try:
            start_date = datetime.date(int(from_date.split("-")[2]), int(from_date.split("-")[1]),
                                       int(from_date.split("-")[0]))
        except Exception as e:
            logging.error("INVALID DATE", e)
            continue
        break

    while True:
        to_date = input("Enter To Date (DD-MM-YYYY)/(TODAY): ")
        if to_date.upper() == "TODAY":
            end_date = datetime.date.today()
        else:
            try:
                end_date = datetime.date(int(to_date.split("-")[2]), int(to_date.split("-")[1]),
                                         int(to_date.split("-")[0]))
            except Exception as e:
                logging.error("INVALID DATE", e)
                continue
        break

    while True:
        intervals = input("Enter Intervals (1m, 3m, 5m, 10m, 15m, 30m, 1h, 1d): ").replace(" ", "").split(",")
        if not all(interval in ["1m", "3m", "5m", "10m", "15m", "30m", "1h", "1d"] for interval in intervals):
            logging.error("INVALID INTERVAL")
            continue
        else:
            interval_format = {
                '1m': 'ONE_MINUTE',
                '3m': 'THREE_MINUTE',
                '5m': 'FIVE_MINUTE',
                '10m': 'TEN_MINUTE',
                '15m': 'FIFTEEN_MINUTE',
                '30m': 'THIRTY_MINUTE',
                '1h': 'ONE_HOUR',
                '1d': 'ONE_DAY'
            }
            intervals = [interval_format[interval] for interval in intervals]
        break
    data = {'exchange': exchange, 'symbol': symbol, 'from_date': start_date, 'to_date': end_date,
            'intervals': intervals}
    return data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

auth_token, feed_token = login()
if not auth_token and not feed_token:
    logging.error("LOGIN FAILED")
    exit(0)

inputs = input_manager()
exchange = inputs['exchange']
symbol = inputs['symbol']
from_date = inputs['from_date']
to_date = inputs['to_date']
intervals = inputs['intervals']

status, token = get_stock_token(symbol, exchange)
if status:
    dates = date_manager(from_date, to_date)
    get_stock_data(exchange, symbol, token, dates, intervals, "data")
