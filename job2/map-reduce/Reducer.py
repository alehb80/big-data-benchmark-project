#!/usr/bin/env python
import sys

# global variables
previousSector = None
previousTicker = None
previousYear = None
previousClose = 0

# field position of each row taken from stdin
SECTOR = 0
TICKER = 1
DATE = 2
CLOSE = 3
VOLUME = 4


# specifics structures to avoid date comparison
# they store specifics for a SINGLE sector
# so they will be resetted on each sector
# first specifics structure
'''
key: year, value: dictionary {
    totalVolume: totalVolumeValue,
    countVolume: countVolumeValue,
    closeStartingPrice: closeStartingPrice,
    closeFinalPrice: closeFinalPrice
    }
'''
yearToSectorTrend = {}

'''
key: year, value: dictionary {
    date: sumOfCloseValues
    }

where date is a string in YYYY-MM-DD format and
sumOfCloseValues is the sum of the close values of tickers which
share the same sector
'''
yearToSectorDailyClosePrice = {}


# utility function for printing a set of key value pairs
def printRecord():
    for year in sorted(yearToSectorTrend.keys()):
        sectorTrend = yearToSectorTrend[year]
        sectorDailyClosePrices = yearToSectorDailyClosePrice[year]
        totalVolume = sectorTrend['totalVolume']
        countVolume = sectorTrend['countVolume']
        priceVariation = (sectorTrend['closeFinalPrice'] - sectorTrend['closeStartingPrice']) / sectorTrend[
            'closeStartingPrice']
        averageClosePrice = getDailyCloseAverage(sectorDailyClosePrices)
        annualMeanVolume = (totalVolume / countVolume)
        print('{}\t{}\t{}\t{}\t{}'.format(previousSector, year, annualMeanVolume, priceVariation, averageClosePrice))


# given a dictionary returns the average of key values
def getDailyCloseAverage(yearToDailyClosePriceMap):
    count = len(yearToDailyClosePriceMap.keys())
    closeSum = sum(yearToDailyClosePriceMap.values())
    return closeSum / count


# add or set "value" to a specifics structure whose (first level) keys are years
def updateDict(dict, year, key, value):
    if year in dict:
        if key in dict[year]:
            dict[year][key] += value
        else:
            dict[year][key] = value
    else:
        dict[year] = {}
        dict[year][key] = value


# parse each value in value list
def getValues(valueList):
    sector = valueList[SECTOR].strip()
    ticker = valueList[TICKER].strip()
    date = valueList[DATE].strip()
    close = float(valueList[CLOSE].strip())
    volume = int(valueList[VOLUME].strip())
    return (sector, ticker, date, close, volume)


# main script
for row in sys.stdin:
    specifics = row.strip().split('\t')

    if len(specifics) == 5:
        sector, ticker, date, close, volume = getValues(specifics)
        year = date[0:4]

        if previousSector and previousSector != sector:
            # sector value (and consequently ticker value) changed.
            # So we set final close price for this ticker,
            # write a new record for the previous sector
            # and update global variables for the new sector
            updateDict(yearToSectorTrend, previousYear, 'closeFinalPrice', previousClose)
            printRecord()

            # reset our dictionaries
            yearToSectorTrend = {}
            yearToSectorDailyClosePrice = {}

            # this is the first available date for this new ticker's record,
            # so we update closeStartingPrice
            updateDict(yearToSectorTrend, year, 'closeStartingPrice', close)

            # update close and volume values for this sector in this year
            updateDict(yearToSectorTrend, year, 'totalVolume', volume)

            updateDict(yearToSectorTrend, year, 'countVolume', 1)

            updateDict(yearToSectorDailyClosePrice, year, date, close)

        else:
            # key value unchanged (or this is the first row of the file).
            # update total close and volume values for this sector in this year
            updateDict(yearToSectorTrend, year, 'totalVolume', volume)

            updateDict(yearToSectorTrend, year, 'countVolume', 1)

            updateDict(yearToSectorDailyClosePrice, year, date, close)

            # Two cases: same ticker or different ticker
            if previousTicker and previousTicker != ticker:
                # Case 1: Different ticker
                # this means that previous close value was the ending close
                # value for the previous ticker in the previous year
                updateDict(yearToSectorTrend, previousYear, 'closeFinalPrice', previousClose)

                # this also means that the current close value is the
                # first close value for this ticker
                updateDict(yearToSectorTrend, year, 'closeStartingPrice', close)

            else:
                # Case 2: same ticker or first row of the file
                # first row of the file
                if not previousTicker:
                    previousTicker = ticker
                    # this also means that the current close value is the
                    # first close value for this ticker
                    updateDict(yearToSectorTrend, year, 'closeStartingPrice', close)

                # we need to establish if the current year has changed.
                # If so we need to update the final close price for
                # the last ticker in the previous year and the
                # starting close value for the same ticker in the current year
                if previousYear and previousYear != year:
                    updateDict(yearToSectorTrend, previousYear, 'closeFinalPrice', previousClose)

                    updateDict(yearToSectorTrend, year, 'closeStartingPrice', close)

        # update global variables
        previousSector = sector
        previousTicker = ticker
        previousYear = year
        previousClose = close

# print last computed key
if previousSector:
    # this means that previous close value was the last value
    updateDict(yearToSectorTrend, previousYear, 'closeFinalPrice', previousClose)
    printRecord()
