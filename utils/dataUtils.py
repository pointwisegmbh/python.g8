# lets me specify data types, read in from strings (most likely from a CSV file) and convert/sanitise them for either MySQL or HDF5

import calendar
from datetime import datetime
import logging
from tables import *
#regex
import re

import iterUtils

# Exceptions
class HDF5Error(Exception):
    def __init__(self, errorString):
        self.errorStr = errorString
        self.args = [errorString]
        return

# End Exceptions


# Helper Functions

def nullEmptyVal(val):
    if val == None:
        return 'NULL'
    else:
        # remove any ,s
        return str(val).replace(',', '')

def parseDateTime(date, formatString, maxyear, logger):
    if date == '':
        return None
    else:
        datetimeObj = datetime.strptime(date, formatString)
        try: 
            # see comment below in DateTimeType.fromString.
            if maxyear != None and datetimeObj.year > maxyear:
                oldYear = datetimeObj.year
                newYear = datetimeObj.year - 100
            
                datetimeObj = datetime(newYear, datetimeObj.month, datetimeObj.day, datetimeObj.hour, datetimeObj.minute, datetimeObj.second)
            
                logger.debug('Year found beyond maxyear (%d > %d), changing it to %d, final date: %s', 
                    oldYear, maxyear, datetimeObj.year, datetimeObj.isoformat())

            
            return datetimeObj
        except ValueError as e:
            logger.error("ValueError processing date string \'%s\': %s" % (date, e.args[0]))
            return None

    return None

# End Helper Functions

# Data Types

class IntType:
    def __init__(self):
        return

    def fromString(self, inputStr):
        if inputStr == '':
            return None
        else:
            return int(inputStr.replace(',', ''))

    def toHDF5(self, inputInt):
        if inputInt == None:
            return 0

        return inputInt

    def toMySQL(self, inputInt):
        return nullEmptyVal(inputInt)

    def getValueList(self, inputInt):
        if inputInt == None:
            return ['NULL']

        return [inputInt]

    def getMySQLFormatString(self):
        return '%s'

    def mySQLType(self):
        return 'INT'

    def pyTablesType(self):
        return Int32Col()

class ShortType:
    def __init__(self, precision=1):
        self.__precision = precision
        return

    def fromString(self, inputStr):
        if inputStr == '':
            return None
        else:
            return int(inputStr.replace(',', ''))

    def toHDF5(self, inputInt):
        if inputInt == None:
            return 0

        return inputInt

    def toMySQL(self, inputInt):
        return nullEmptyVal(inputInt)

    def getValueList(self, inputInt):
        if inputInt == None:
            return ['NULL']

        return [inputInt]

    def getMySQLFormatString(self):
        return '%s'

    def mySQLType(self):
        return 'TINYINT(%d)' % (self.__precision)

    def pyTablesType(self):
        return Int8Col()


class LongType:
    def __init__(self, precision=20):
        self.__precision = precision

        return

    def fromString(self, inputStr):
        if inputStr == '':
            return None
        else:
            return long(inputStr.replace(',', ''))

    def toHDF5(self, inputInt):
        if inputInt == None:
            return 0

        return inputInt

    def toMySQL(self, inputInt):
        return nullEmptyVal(inputInt)

    def getMySQLFormatString(self):
        return '%s'

    def getValueList(self, inputInt):
        if inputInt == None:
            return ['NULL']

        return [inputInt]

    def mySQLType(self):
        return 'BIGINT(%d)' % (self.__precision)

    def pyTablesType(self):
        return Int64Col()

class FloatType:
    def __init__(self, numdigits, decplaces):
        self.__digits = numdigits
        self.__decPlaces = decplaces
        return

    def fromString(self, inputStr):
        if inputStr == '':
            return None
        else:
            return float(inputStr.replace(',', ''))

    def toHDF5(self, inputFloat):
        if inputFloat == None:
            return float('nan')

        return inputFloat

    def toMySQL(self, inputFloat):
        return nullEmptyVal(inputFloat)
        
    def getMySQLFormatString(self):
        return '%s'

    def getValueList(self, inputFloat):
        if inputFloat == None:
            return ['NULL']

        return [inputFloat]

    def mySQLType(self):
        return 'FLOAT(%d, %d)' % (self.__digits, self.__decPlaces)

    def pyTablesType(self):
        return Float32Col()

class Point2DFloatType:
    def __init__(self):
        self.__regexp = re.compile(r'\((?P<x>[0-9.,\-]+) (?P<y>[0-9.,\-]+)\)')

        self.__logger = logging.getLogger('Point2DFloatType')
        return

    def fromString(self, inputStr):
        if inputStr == '':
            return None
        else:
            matchResult = self.__regexp.match(inputStr)

            if matchResult == None:
                self.__logger.error('Failed to find coordinates in %s', inputStr)
                return None

            return (
                float(matchResult.group('x').replace(',', '')),
                float(matchResult.group('y').replace(',', ''))) 

    def toHDF5(self, inputTuple):
        if inputTuple == None:
            return complex(real=0, imag=0)

        realComp = inputTuple[0]
        imagComp = inputTuple[1]

        if realComp == None:
            realComp = float('nan')

        if imagComp == None:
            imagComp = float('nan')

        return complex(real=realComp, imag=imagComp)

    def toMySQL(self, inputTuple):
        if inputTuple == None:
            return 'NULL'

        return 'GeomFromText(\'POINT(%f %f)\')' % inputTuple

    def getMySQLFormatString(self):
        return 'GeomFromText(\'POINT(%s %s)\')'

    def getValueList(self, inputTuple):
        if inputTuple == None:
            return ['NULL', 'NULL']

        return [inputTuple[0], inputTuple[1]]

        
    def mySQLType(self):
        return 'POINT'

    def pyTablesType(self):
        # bit of a hack, pytables doesnt support storing arrays in tables AFAIK
        # so store it as a (double precision) complex number.
        return Complex128Col()


class StringType:
    def __init__(self, numBytes):
        self.__numBytes = numBytes
        self.__logger = logging.getLogger('StringType')
        return

    def __checkAndTruncateString(self, inputStr):
        if len(inputStr) > self.__numBytes:
            truncedString = inputStr[0:self.__numBytes - 1]
            self.__logger.warning('String (\'%s\') is %d chars, which is beyond the %d char limit for this datatype, its being truncated to %s', inputStr, len(inputStr), self.__numBytes, truncedString)

            return truncedString
        else:
            return inputStr


    def fromString(self, inputStr):
        return inputStr

    def toHDF5(self, inputStr):
        return self.__checkAndTruncateString(inputStr)

    def toMySQL(self, inputStr):
        return '\'%s\'' % (self.__checkAndTruncateString(
            inputStr.replace('\'', '').replace('%', 'percent')))
        
    def getMySQLFormatString(self):
        return '%s'

    def getValueList(self, inputString):
        if inputString == None:
            return ['']

        return [self.__checkAndTruncateString(
            inputString.replace('\'', ''))]

    def mySQLType(self):
        return 'VARCHAR(%d)' % (self.__numBytes)

    def pyTablesType(self):
        return StringCol(self.__numBytes)

class EnumType:
    def __init__(self, values):
        self.__values = values
        self.__logger = logging.getLogger('EnumType')

        self.__toIntMap = {value : enum for (enum, value) in enumerate(values)}

        self.__toStringMap = {enum : value for (enum, value) in enumerate(values)}

        return

    def __checkString(self, inputStr):
        if inputStr not in self.__values:
            self.__logger.error('invalid value for Enum type parsed, parsed value: \'%s\', allowed values: %s', inputStr, self.__values)
            return False

        else:
            return True

    def __checkEnum(self, inputEnum):
        if inputEnum not in self.__toStringMap:
            self.__logger.error('Out of bounds value for Enum type parsed, parsed value: \'%d\', allowed values: %s', inputEnum, self.__toStringMap)
            return False

        else:
            return True


    def fromString(self, inputStr):
        if not self.__checkString(inputStr):
            return None

        return self.__toIntMap[inputStr]

    def toHDF5(self, inputEnum):
        if not self.__checkEnum(inputEnum):
            return -1

        return inputEnum

    def toMySQL(self, inputEnum):
        if not self.__checkEnum(inputEnum):
            return '\'NULL\''

        return '\'%s\'' % self.__toStringMap[inputEnum]
        
    def getMySQLFormatString(self):
        return '%s'

    def getValueList(self, inputEnum):
        if inputEnum == None:
            return ['NULL']

        if not self.__checkEnum(inputEnum):
            return ['NULL']

        return [self.__toStringMap[inputEnum]]

    def mySQLType(self):
        return 'ENUM(%s)' % (', '.join(self.__values))

    def pyTablesType(self):
        return Int32Col()

class CharType:
    def __init__(self):
        return

    def fromString(self, inputStr):
        return inputStr

    def toHDF5(self, inputStr):
        if inputStr == '':
            return 0
        return ord(inputStr[0])

    def toMySQL(self, inputStr):
        return '\'%s\'' % (inputStr[0])
        
    def getMySQLFormatString(self):
        return '%s'

    def getValueList(self, inputStr):
        if inputStr == None:
            return ['NULL']

        if inputStr == '':
            return ['NULL']

        return [str(inputStr[0])]

    def mySQLType(self):
        return 'CHAR'

    def pyTablesType(self):
        return UInt8Col()

class DateType:
    def __init__(self, formatStr, maxyear=None):
        self.__formatString = formatStr
        self.__maxyear = maxyear

        self.__logger = logging.getLogger('DateType')

        return

    def setMaxYear(self, maxyear):
        self.__maxyear = maxyear
        return

    def fromString(self, date):
        return parseDateTime(date, self.__formatString, self.__maxyear, self.__logger)
        
    def toHDF5(self, dateTimeObj):
        # store it as a timestamp, seconds from epoch in HDF5.
        # assumes time is in UTC...
        if dateTimeObj == None:
            return 0

        return calendar.timegm(dateTimeObj.timetuple())

    def fromHDF5(self, timestamp):
        return datetime.utcfromtimestamp()

    def toMySQL(self, date):
        if date == None:
            return '\'NULL\''

        return '\'%s\'' % (date.isoformat())
        
    def getMySQLFormatString(self):
        return '%s'

    def getValueList(self, inputDateTime):
        if inputDateTime == None:
            return ['NULL']

        return [inputDateTime.isoformat()]

    def mySQLType(self):
        return 'DATE'

    def pyTablesType(self):
        return Time64Col()

class DateTimeType:
    def __init__(self, formatStr, maxyear=None):
        self.__formatString = formatStr
        self.__maxyear = maxyear

        self.__logger = logging.getLogger('DateTimeType')

        return

    def setMaxYear(self, maxyear):
        self.__maxyear = maxyear
        return

    def fromString(self, date):
        return parseDateTime(date, self.__formatString, self.__maxyear, self.__logger)
        
    def toHDF5(self, dateTimeObj):
        # store it as a timestamp, seconds from epoch in HDF5.
        # assumes time is in UTC...
        if dateTimeObj == None:
            return 0

        return calendar.timegm(dateTimeObj.timetuple())

    def fromHDF5(self, timestamp):
        return datetime.utcfromtimestamp()

    def toMySQL(self, inputStr):
        if date == None:
            return '\'NULL\''

        return '\'%s\'' % (date.isoformat())
        
    def getMySQLFormatString(self):
        return '%s'

    def getValueList(self, inputDateTime):
        if inputDateTime == None:
            return ['NULL']

        return [inputDateTime.isoformat()]

    def mySQLType(self):
        return 'DATETIME'

    def pyTablesType(self):
        return Time64Col()

# Data Types

class TableDescription:
    # orderedColumns is a LIST of mappings the column names to the respective classes defined above. (i.e a list of tuples like ('column_name', TypeClass())
    def __init__(self, orderedColumns, indexedCols = [], csIndexedCols = []):
        # pre generate these as they are commonly used.
        self.__orderedColumns = orderedColumns 
        self.__columnsDict = dict(orderedColumns)

        self.__indexedCols = indexedCols
        self.__csIndexedCols = csIndexedCols

        self.__logger = logging.getLogger(__name__)

        return

    def getOrderedColumns(self):
        return self.__orderedColumns

    def getColumnsDict(self):
        return self.__columnsDict

    def getPyTablesDescription(self):
        # not pregenerated because the general use case isnt necessarily using HDF5/PyTables all the time
        return {columnName : colType.pyTablesType() for (columnName, colType) in self.__orderedColumns}

    def indexPyTable(self, pytable):
        for column in self.__indexedCols:
            pytable.cols[column].createIndex()

        for column in self.__csIndexedCols:
            pytable.colinstances[column].createCSIndex()

        return

    def generateMySQLTableSchema(self, tableName):
        columns = ''

        isFirst = True

        for colName, colType in self.__orderedColumns:
            if not isFirst:
                columns = columns + ', '
            else:
                isFirst = False

            columns = columns + ' ' + colType.mySQLType() 

        schemaLine = 'CREATE TABLE %s (%s);' % (tableName, columns)

        self.__logger.debug('Generated MySQL CREATE line: %s', schemaLine)
        
        return schemaLine

    def generateMySQLInsertFormatString(self, tableName):
        columns = '(' + ','.join(colName for (colName, colType) in self.__orderedColumns) + ')'

        valueFormatString = '(' + ','.join(
            colType.getMySQLFormatString() for (colName, colType) in self.__orderedColumns) + ')'

        return ('INSERT INTO %s ' % (tableName)) + columns + ' VALUES' + valueFormatString + ';'

# Maps a dictionary that maps column names to values (i.e the standard expression of a 'row' in this module) 
# into a tuple that can be used with the mysqlUtils module and the fmt string generated by 
# TableDescription.generateMySQLInsertFormatString() to insert
class MySQLInsertTuple:
    def __init__(self, tableDesc):
        self.__orderedCols = [colName for colName, colType in tableDesc.getOrderedColumns()] 

        self.__colDict = tableDesc.getColumnsDict()

        self.__logger = logging.getLogger('MySQLInsertTuple')

        return

    def __call__(self, row):

        columnInfo = iterUtils.EnumerateStarMaps([self.__colDict, row], keys=self.__orderedCols)

        result = []

        for colName, colType, colValue in columnInfo:
            try:
                for value in colType.getValueList(colValue):
                    result.append(value)
            except Exception as error:
                self.__logger.error('Error while trying to handle value for column \'%s\', value: %s, error: %s', colName, str(colValue), error)

        return tuple(result) 

# Batches up rows into MySQL Inserts
# 'rows' is a collection of dictionaries with keys matching the colnames in tableDesc
# NOTE: I haven't really used this yet, I'm not really sure its a very good idea to use..
class MySQLInsertCollectionAdapter:
    def __init__(self, tableDesc, tableName, rows, batchSize=1):
        self.__orderedColumns = tableDesc.getOrderedColumns()
        self.__tableName = tableName
        self.__batchSize = batchSize
        self.__rows = rows

        self.__cols = ''
        self.__values = ''
        self.__currentBatchCount = 0
        
        self.__isFinished = False

        self.__logger = logging.getLogger(__name__)

        isFirst = True

        self.__insertFmt = 'INSERT INTO %s %s VALUES%s;'

        self.__cols = ','.join(colName for colName, colType in self.__orderedColumns)

        return

    def __getNextLine(self):
        insertString = self.__insertFmt % (self.__tableName, self.__cols, self.__values)

        self.__values = ''
        self.__currentBatchCount = 0
        self.__logger.debug('Generated INSERT line: %s', insertString)
        return insertString

    def __iter__(self):
        return self

    def next(self):
        if self.__isFinished:
            raise StopIteration

        # go through the rows of the underlying collection..
        for row in self.__rows:
            if self.__currentBatchSize != 0:
                self.__values = self.__values + ','

            # start a new tuple of values...
            self.__values = self.__values + '('

            isFirstCol = True
            for colName, colType in self.__orderedColumns:
                if not isFirstCol:
                    self.__values = self.__values + ',' 
                else:
                    isFirstCol = False

                # convert them all into valid mysql input, 
                # string and date types make sure the resultant string is enclosed in
                # single quotes to make them literals to avoid an injection attack
                self.__values = self.__values + colType.toMySQL(row[colName])

            self.__values = self.__values + ')'
            self.__currentBatchSize += 1

            if self.__currentBatchSize == self.__batchSize:
                return self.__getNextLine()

        self.__isFinished = True

        if self.__currentBatchSize == 0:
            raise StopIteration
            
        return self.__getNextLine()

# writes a row to an HDF5 table..
class HDF5TableRowWriter:
    def __init__(self, h5Table, columnsDict):
        self.__h5Table = h5Table
        self.__columnsDict = columnsDict

        self.__logger = logging.getLogger('HDF5TableRowWriter')

        return

    def __call__(self, row):
        h5Row = self.__h5Table.row
        for colName, colValue in row.iteritems():
            hdf5Representation = self.__columnsDict[colName].toHDF5(colValue)
            try:
                h5Row[colName] = hdf5Representation
            except TypeError as typeError:
                errorMsg = 'TypeError while writing HDF5 Table, column \'%s\', value \'%s\': %s' % (
                    colName, str(hdf5Representation), typeError.args[0])
                self.__logger.error(errorMsg)
                raise HDF5Error(errorMsg)

        h5Row.append()
        return


    def close(self):
        self.flush()
        return

    def flush(self):
        self.__h5Table.flush()
        return
