#!/usr/bin/env python
import sys

# field position of each row taken from stdin
ANNUAL_VARIATION_2016 = 0
ANNUAL_VARIATION_2017 = 1
ANNUAL_VARIATION_2018 = 2
NAME = 3


# global variables
previousAnnualVariationTriplet = None

# list that stores info for a given company which has a given percent triplet
# so this list will be resetted on each new percent triplet
'''
[companyName1, companyName2, ..]
'''
listCompany = []


# *** utility functions ***

# utility function for printing a set of key value pairs
def printRecord():

    if len(listCompany) > 1:
        placeholders = '\t'.join(['{},'] * len(listCompany) + ['2016: {}%'] + ['2017: {}%'] + ['2018: {}%'])
        arglist = listCompany + list(previousAnnualVariationTriplet)
        out = placeholders.format(*arglist)
        print(out)


# parse each value in value list
def getValues(valueList):
    annualVariation2016 = valueList[ANNUAL_VARIATION_2016].strip()
    annualVariation2017 = valueList[ANNUAL_VARIATION_2017].strip()
    annualVariation2018 = valueList[ANNUAL_VARIATION_2018].strip()
    name = valueList[NAME].strip()
    return (annualVariation2016, annualVariation2017, annualVariation2018), name


# main script
for row in sys.stdin:
    specifics = row.strip().split('\t')

    if len(specifics) == 4:
        annualVariationTriplet, name = getValues(specifics)

        if previousAnnualVariationTriplet and previousAnnualVariationTriplet != annualVariationTriplet:
            # triplet changed.
            # So we write a new record for the previous company,
            # update global variables for the new company
            # and finally update company list for the new triplet
            printRecord()

            # reset variable values
            previousAnnualVariationTriplet = annualVariationTriplet

            # reset our list
            listCompany = []

            # add a new entry in listCompany global variable
            listCompany.append(name)

        else:
            # key value unchanged (or this is the first row of the file).
            previousAnnualVariationTriplet = annualVariationTriplet

            # add a new entry in listCompany global variable
            listCompany.append(name)

# print last computed key
if previousAnnualVariationTriplet:
    printRecord()
