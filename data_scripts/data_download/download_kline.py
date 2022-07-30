import os
import hashlib
import requests
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import zipfile
import argparse

# Config
PATH = 'https://data.binance.vision/data/futures/um/daily/klines/'
CHECKSUM = '.CHECKSUM'
CHUNK_SIZE = 128

# utility


def make_path(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def checksum(file):
    '''
    Using sha256 to calculate the checksum of a file
    Returns: True if the checksum is correct, False otherwise
    '''
    sha256_hash = hashlib.sha256()
    with open(file, "rb") as f:
        while True:
            byte_block = f.read(4096)
            if not byte_block:
                break
            sha256_hash.update(byte_block)
    calculated_checksum = sha256_hash.hexdigest()

    checksum_file = file+CHECKSUM
    valid_checksum = ''
    with open(checksum_file) as f:
        valid_checksum = f.read().split()[0]

    return calculated_checksum == valid_checksum


def unzip_file(file, save_path):
    '''
    Unzip a file to a folder
    '''
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(save_path)
        print('{} has been unzipped'.format(file))


def unzip_files(src_path, save_path):
    '''
    Unzip all files in a folder to a folder
    '''
    for file in os.listdir(src_path):
        if file.endswith('.zip'):
            try:
                unzip_file(os.path.join(src_path, file), save_path)
                print('{} has been unzipped'.format(file))
            except:
                print('{} unzip fail'.format(file))

# download


def data_download(symbol, date, interval, save_path, chunk_size=CHUNK_SIZE):
    '''
    Download a file from binance.vision
    Returns: True if the download is successful, False otherwise
    '''
    filename = '{}-{}-{}.zip'.format(symbol, interval, date)
    path = PATH + symbol + '/' + interval + '/' + filename
    checksum_path = path + CHECKSUM

    data_save_path = os.path.join(save_path, filename)
    checksum_save_path = data_save_path + CHECKSUM

    download(path, data_save_path, chunk_size)
    download(checksum_path, checksum_save_path, chunk_size)

    return checksum(data_save_path)


def download(url, save_path, chunk_size=CHUNK_SIZE):
    '''
    Download a file from a url
    '''
    try:
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
        return True
    except:
        print('{} download fail'.format(url))
        return False

# main fucntion


def main(symbol, dates, interval, download_folder, unzip_folder, chunk_size=CHUNK_SIZE):
    '''
    Download and unzip all files for a symbol and interval
    '''
    make_path(download_folder)
    print('Downloading {} for {}'.format(symbol, interval))
    for date in dates:
        if unzip_file:
            unzip_files(download_folder, download_folder)
        if data_download(symbol, date, interval, download_folder, chunk_size):
            print('{}-{}-{} download success'.format(symbol, interval, date))
        else:
            print('{}-{}-{} download fail'.format(symbol, interval, date))

    print('Unzipping {} for {}'.format(symbol, interval))
    unzip_files(download_folder, unzip_folder)
    print('Program finished')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        "--symbol", help="The symbol + coin type, e.g. ETHUSDT", type=str, required=True)
    parser.add_argument(
        "--start", help="The start date to download, e.g. 01-01-2022", type=str, required=True)
    parser.add_argument(
        "--end", help="The end date to download, e.g. 01-01-2022", type=str, required=True)
    parser.add_argument(
        "--interval", help="The interval to download, e.g. 1m", type=str, required=False)

    # init
    symbol = ''
    start = ''
    end = ''
    interval = '1m'

    # parse
    args = parser.parse_args()
    if args.symbol:
        symbol = args.symbol
    if args.start:
        start = args.start
    if args.end:
        end = args.end
    if args.interval:
        interval = args.interval

    # folders
    download_folder = 'kline_data_{}/download/{}'.format(interval, symbol)
    unzip_folder = 'kline_data_{}/unzip/{}'.format(interval, symbol)

    # dates
    start_date = datetime.strptime(start, '%d-%m-%Y')
    end_date = datetime.strptime(end, '%d-%m-%Y')

    dates = [start_date + timedelta(days=x)
             for x in range((end_date - start_date).days + 1)]
    datas = [date.strftime('%Y-%m-%d') for date in dates]

    main(symbol, datas, interval, download_folder, unzip_folder)
