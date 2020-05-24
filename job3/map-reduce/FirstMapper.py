#!/usr/bin/env python

import sys
import csv

START_INTERVAL = 2016
END_INTERVAL = 2018

interval = range(START_INTERVAL, END_INTERVAL + 1)

tckrToCompanyNameMap = {}

# reading from the distributed cache
with open('historical_stocks.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    # ignore first row
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, name, sector, _ = row
            if sector != 'N/A':
                tckrToCompanyNameMap[ticker] = name
        else:
            firstLine = False

for line in sys.stdin:
    # turn each row into a list of strings
    specifics = line.strip().split(',')
    if len(specifics) == 8:
        ticker, _, close, _, _, _, _, date = specifics
        # ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])

            # check if year is in range START_INTERVAL-END_INTERVAL
            # check if the ticker has a corresponding sector (we filter out
            # tickers whose sectors are N/A)
            if year in interval and ticker in tckrToCompanyNameMap:
                name = tckrToCompanyNameMap[ticker]
                print('{}\t{}\t{}\t{}'.format(name,
                                              ticker,
                                              date,
                                              close))
