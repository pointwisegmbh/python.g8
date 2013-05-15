import csv
import logging

# Exceptions
class CSVError(Exception):
    def __init__(self, errorString):
        self.errorStr = errorString
        self.args = [errorString]

#end Exceptions

# takes a csv row and sanitises the input a bit and returns it in a dictionary form
# with column names
class CsvRowParser:
    def __init__(self, columnTypes):
        self.__columnTypes = columnTypes
        self.__logger = logging.getLogger(__name__)

        return

    def __call__(self, csvRow):
        return self.__parseRow(csvRow)

    def __parseRow(self, csvRow):
        newRow = {}

        if len(self.__columnTypes) != len(csvRow):
            errorMsg = 'column mismatch, %d columns given in description, %d columns read in row, csv row: %s' % (
                len(self.__columnTypes),
                len(csvRow),
                csvRow)

            self.__logger.critical(errorMsg)
            raise CSVError(errorMsg)

        for index in range(0, len(csvRow), 1):
            try:
                newRow[self.__columnTypes[index][0]] = self.__columnTypes[index][1].fromString(csvRow[index])
            except ValueError as valError:
                self.__logger.error('Encountered ValueError trying to parse \'%s\' value: %s',
                    self.__columnTypes[index][0], csvRow[index])

                self.__logger.error('Value Error: %s', valError.args[0])

                raise valError

        return newRow
