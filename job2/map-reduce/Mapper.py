#!/usr/bin/env python

import sys
import csv

START_INTERVAL = 2008
END_INTERVAL = 2018

interval = range(START_INTERVAL, END_INTERVAL + 1)

tckrToSectorMap = {}

# reading from the distributed cache
with open('historical_stocks.csv') as csv_file:

    csv_reader = csv.reader(csv_file, delimiter=',')
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, _, sector, _ = row
            if sector != 'N/A':
                tckrToSectorMap[ticker] = sector
        else:
            firstLine = False

for line in sys.stdin:
    # turn each row into a list of strings
    specifics = line.strip().split(',')
    if len(specifics) == 8:
        ticker, _, close, _, _, _, volume, date = specifics
        # ignore file's first row
        if ticker != 'ticker':
            year = int(date[0:4])

            # check if year is in range START_INTERVAL-END_INTERVAL
            # check if the ticker has a corresponding sector (we filter out tickers whose sectors are N/A)
            if year in interval and ticker in tckrToSectorMap:
                sector = tckrToSectorMap[ticker]
                print('{}\t{}\t{}\t{}\t{}'.format(sector, ticker, date, close, volume))
