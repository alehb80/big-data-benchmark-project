#!/usr/bin/env python

import sys

# global variables
stats = []
previousTckr = None
closeStartingPrice = None
closeFinalPrice = None
first_year = None
lastYear = None
minLowPrice = sys.maxsize
maxHighPrice = - sys.maxsize
totalVolume = 0
countVolume = 0

# field position in each row
TICKER = 0
DATE = 1
CLOSE = 2
LOW = 3
HIGH = 4
VOLUME = 5


# parse each value in value list
def getValues(valueList):
    ticker = valueList[TICKER].strip()
    year = valueList[DATE].strip()[0:4]
    close = float(valueList[CLOSE].strip())
    low = float(valueList[LOW].strip())
    high = float(valueList[HIGH].strip())
    volume = float(valueList[VOLUME].strip())
    return [ticker, year, close, low, high, volume]


# utility function for appending a new item into stats
def addToList():
    differenceClosePrice = closeFinalPrice - closeStartingPrice
    priceVariation = differenceClosePrice / closeStartingPrice
    avgVolume = totalVolume / countVolume

    record = {'ticker': previousTckr,
              'priceVariation': priceVariation * 100,
              'minLowPrice': minLowPrice,
              'maxHighPrice': maxHighPrice,
              'avgVolume': avgVolume
              }

    stats.append(record)


# main script
for line in sys.stdin:
    valueList = line.strip().split('\t')

    if len(valueList) == 6:
        ticker, year, close, low, high, volume = getValues(valueList)

        # caso in cui nel mapper considero un ticker diverso da quello precedente
        if previousTckr and previousTckr != ticker:
            # key value changed. Append a new item into stats list
            # and update values for the new key

            # TODO: inserire nella relazione
            # includo aziende nate dopo il 2008 o fallite prima del 2018
            addToList()

            # update variable values
            closeStartingPrice = close
            firstYear = year
            closeFinalPrice = close
            lastYear = year
            minLowPrice = low
            maxHighPrice = high
            totalVolume = volume
            countVolume = 1

        # caso in cui nel mapper considero lo stesso ticker di quello precedente
        else:
            # key value unchanged (or this is the first row of the file).
            # in case this is the first row of the file
            if not previousTckr:
                closeStartingPrice = close
                firstYear = year

            # Update values for the current key
            closeFinalPrice = close
            lastYear = year
            minLowPrice = min(minLowPrice, low)
            maxHighPrice = max(maxHighPrice, high)
            totalVolume += volume
            countVolume += 1

        previousTckr = ticker

# add last computed key into stats
if previousTckr:
    addToList()

sortedStats = sorted(stats, key=lambda k: k['priceVariation'], reverse=True)

# print sorted list
#for i in range(10):
#    item = sortedStats[i]
#    print('{}\t{}%\t{}\t{}\t{}'.format(item['ticker'],
#                                       item['priceVariation'],
#                                       item['minLowPrice'],
#                                       item['maxHighPrice'],
#                                       item['avgVolume']))

# print sorted list in partitioned datasets
for i in range(len(sortedStats)):
    item = sortedStats[i]
    print('{}\t{}%\t{}\t{}\t{}'.format(item['ticker'],
                                       item['priceVariation'],
                                       item['minLowPrice'],
                                       item['maxHighPrice'],
                                       item['avgVolume']))
