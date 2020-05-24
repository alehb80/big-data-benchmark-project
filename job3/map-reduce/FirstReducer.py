#!/usr/bin/env python
import sys


# global variables
previousName = None
previousTicker = None
previousYear = None
previousClose = None

# field position of each row taken from stdin
NAME = 0
TICKER = 1
DATE = 2
CLOSE = 3

START_INTERVAL = 2016
END_INTERVAL = 2018

interval = list(range(START_INTERVAL, END_INTERVAL + 1))


# yearToCompanyTrend is specifics structure used to avoid date comparison
# it stores specifics for a SINGLE company name
# so it will be resetted on each company name
'''
key: year, value: dictionary {
    closeStartingPrice: closeStartingPrice,
    closeFinalPrice: closeFinalPrice,
    }
'''
yearToCompanyTrend = {}


# utility function for printing a set of key value pairs
def printRecord():
    # First, let's check that a non null value exists for each year
    if all(str(year) in yearToCompanyTrend for year in interval):

        yearToCompanyTrendKeys = yearToCompanyTrend.keys()
        # for company's name
        listOfSquareBrackets = ['{}'] * len(yearToCompanyTrendKeys) + ['{}']

        formattedString = '\t'.join(listOfSquareBrackets)

        # percentChangeMap = {'2016': None, '2017': None, '2018': None}
        # percentChangeMap is a dictionary whose keys are
        # years (taken from the yearToCompanyTrend keys) and
        # values are None (temporary)
        percentChangeMap = {year: None for year in yearToCompanyTrendKeys}

        for year in sorted(yearToCompanyTrend.keys()):
            companyTrendYear = yearToCompanyTrend[year]
            closePriceFinalValue = companyTrendYear['closeFinalPrice']
            closePriceStartingValue = companyTrendYear['closeStartingPrice']
            closeDifference = closePriceFinalValue - closePriceStartingValue
            priceVariation = closeDifference / closePriceStartingValue
            percentChangeMap[year] = int(round(priceVariation * 100))

        sortedPercentChangeMapKeys = sorted(percentChangeMap)
        sortedPercentChangeMapValues = [percentChangeMap[year] for year
                                        in sortedPercentChangeMapKeys]


        sortedPercentChangeMapValues.append(previousName)
        print(formattedString.format(*(sortedPercentChangeMapValues)))


# add or set "value" to yearToCompanyTrend[year]
# dictionary based on key existence
def updateCompanyTrend(dataStructure, year, key, value):
    if year in dataStructure:
        if key in dataStructure[year]:
            dataStructure[year][key] += value
        else:
            dataStructure[year][key] = value
    else:
        dataStructure[year] = {}
        dataStructure[year][key] = value


# parse each value in value list
def getValues(valueList):
    name = valueList[NAME].strip()
    ticker = valueList[TICKER].strip()
    year = valueList[DATE].strip()[0:4]
    close = float(valueList[CLOSE].strip())
    return (name, ticker, year, close)


# main script
for row in sys.stdin:
    valueList = row.strip().split('\t')

    if len(valueList) == 4:
        name, ticker, year, close = getValues(valueList)

        if previousName and previousName != name:
            # company name changed.
            # So we set final close price for the previous company
            # (company's ticker) in the previous year, write a new record for
            # the previous company and reset our dictionary
            updateCompanyTrend(yearToCompanyTrend, previousYear, 'closeFinalPrice', previousClose)
            printRecord()

            # reset our dictionary
            yearToCompanyTrend = {}

            # this is the first available date for this new company's record
            # so we update closeStartingPrice
            updateCompanyTrend(yearToCompanyTrend, year, 'closeStartingPrice', close)

        else:
            # key value unchanged (or this is the first row of the file).

            # Two cases: same ticker or different ticker
            # That is because a same company may have different
            # tickers
            if previousTicker and previousTicker != ticker:
                # Case 1: Different tickers
                # this means that previous close value was the ending
                # close value for the previous ticker in the previous year
                updateCompanyTrend(yearToCompanyTrend, previousYear, 'closeFinalPrice', previousClose)

                # this also means that the current close value is the first
                # close value for this ticker in this year
                updateCompanyTrend(yearToCompanyTrend, year, 'closeStartingPrice', close)

            else:
                # Case 2: same ticker or first row of the file
                # first row of the file

                # Two cases: same year or different year
                if previousYear and previousYear != year:
                    # Case 1: Different year
                    # this means that previous close value was the ending
                    # close value for the current ticker in the previous year
                    updateCompanyTrend(yearToCompanyTrend, previousYear, 'closeFinalPrice', previousClose)

                    # this also means that the current close value is the first
                    # close value for this company in this year
                    updateCompanyTrend(yearToCompanyTrend, year, 'closeStartingPrice', close)
                else:
                    # Another case: same year or first row of the file
                    # first row of the file
                    if not previousYear:
                        # this also means that the current close value is the
                        # first close value for this company in this year
                        updateCompanyTrend(yearToCompanyTrend, year, 'closeStartingPrice', close)

        # reset variable values
        previousName = name
        previousTicker = ticker
        previousYear = year
        previousClose = close

# print last computed key
if previousName:
    # this means that previous close value was the last value
    updateCompanyTrend(yearToCompanyTrend, previousYear, 'closeFinalPrice', previousClose)
    printRecord()
