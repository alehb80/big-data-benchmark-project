#!/usr/bin/env python

import sys

START_INTERVAL = 2008
END_INTERVAL = 2018

interval = range(START_INTERVAL, END_INTERVAL + 1)

# Need to skip first row of the csv file, that's the job of this variable
firstLine = True

for row in sys.stdin:

    # Cut empty lines
    if row == "":
        continue

    # Skip only the first row, where are located the name of the columns
    if firstLine:
        firstLine = False
        continue

    # turn each row into a list of strings
    specifics = row.strip().split(',')
    if len(specifics) == 8:
        ticker, _, close, _, low, high, volume, date = specifics

        # ignore file's first row
        ## TO-DO try removing this if-statement
        if ticker != 'ticker':
            year = int(date[0:4])
            # check if year is in range START_INTERVAL-END_INTERVAL
            if year in interval:
                print('{}\t{}\t{}\t{}\t{}\t{}'.format(ticker, date, close, low, high, volume))
