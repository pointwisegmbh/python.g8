import logging
#regex
import re

from contextlib import contextmanager, closing
from optparse import OptionParser, OptionGroup

import MySQLdb as mdb

# Exceptions
class MySQLError(Exception):
    def __init__(self, errNumber, errorString):
        self.errorStr = errorString
        self.errNo = errNumber
        self.args = [errNumber, errorString]

        return
#end Exceptions

def makeMySQLOpts(parser):
    mysqlOpts = OptionGroup(parser, "Mysql Database options")

    mysqlOpts.add_option('--mysqlhost', type='string', dest='mysqlhost', default='localhost',
        help='Hostname for the MySQL server [default=%default]',
        metavar='HOSTNAME')

    mysqlOpts.add_option('--mysqlport', type='int', dest='mysqlport', default='3306',
        help='Port the MySQL server is listening to [default=%default]',
        metavar='PORT')

    mysqlOpts.add_option('-u', '--mysqluser', type='string', dest='mysqluser', default='root',
        help='The user to sign into the mysql server as. [default=%default]',
        metavar='USERNAME')

    mysqlOpts.add_option('-p', '--mysqlpasswd', type='string', dest='mysqlpasswd', default='',
        help='Password to sign into the MySQL server with [default=\'%default\']',
        metavar='PASSWD')

    mysqlOpts.add_option('--mysqldb', type='string', dest='mysqldb', default='geoServer',
        help='The database to use for this application [default=%default]',
        metavar='DATABASE')

    return mysqlOpts

class MySQLOptions:
    def __init__(self, options):
        self.host = options.mysqlhost
        self.port = options.mysqlport
        self.user = options.mysqluser
        self.passwd = options.mysqlpasswd
        self.dbName = options.mysqldb
        return

class ConnectionPool:
    def __init__(
        self,
        host,
        port,
        user,
        passwd,
        dbName):

        self.__host = host
        self.__port = port
        self.__user = user
        self.__passwd = passwd
        self.__dbName = dbName

        self.__connections = []

        self.__logger = logging.getLogger(__name__)

        return

    def getConnection(self):
        if len(self.__connections) == 0:
            # create a new connection:
            
            self.__logger.info('No pooled connections available, making new connection')

            return PooledMySQLConnection(
                mdb.connect(
                    self.__host,
                    self.__user,
                    self.__passwd,
                    db=self.__dbName,
                    port=self.__port),
                self)
        else:
            # pop the pooled connection off the end and return that.
            self.__logger.debug('Pooled connection available, %d remaining', len(self.__connections) - 1)
            return PooledMySQLConnection(self.__connections.pop(), self)

    def returnConnection(self, connection):
        self.__connections.append(connection)

        self.__logger.info('Connection returned to pool, %d connections now available', len(self.__connections))

        return

    def closeConnections(self):
        for conn in self.__connections:
            conn.close()

        self.__logger.info('Closed %d connections', len(self.__connections))

        return

class PooledMySQLConnection:
    def __init__(self, connection, pool):
        self.__connection = connection
        self.__pool = pool
        return

    def cursor(self, *args):
        return self.__connection.cursor(*args)

    def commit(self):
        self.__connection.commit()
        return

    def close(self):
        self.__pool.returnConnection(self.__connection)
        return

# right now this simply creates connections whenever necessary but its all encapsulated here so that
# more sophisticated pooling can happen later.
class MySQLConnector:
    def __init__(self, mysqlOptions):
        self.host = mysqlOptions.host
        self.port = mysqlOptions.port
        self.user = mysqlOptions.user
        self.passwd = mysqlOptions.passwd
        self.dbName = mysqlOptions.dbName

        self.__connections = ConnectionPool(self.host, self.port, self.user, self.passwd, self.dbName)

        self.logger = logging.getLogger('MySQLConnector[%s@%s:%d/%s]' % (self.user, self.host, self.port, self.dbName))

        return

    def getDBName(self):
        return self.dbName

    def onDBError(self, dbError):
        errorMsg = 'MySQL Error [%d]: %s' % (dbError.args[0], dbError.args[1])
        self.logger.error(errorMsg)
        raise MySQLError(dbError.args[0], errorMsg)

    def connect(self, charSet=None, dbase=None):
        try:
            if charSet == None and dbase == None:
                return self.__connections.getConnection()

            if dbase == None:
                dbase = self.dbName

            # bit ugly but mysqldb docs dont say what the default value for charset is,
            # then i could just set the default val for charSet 
            if charSet != None:
                con = mdb.connect(
                    self.host,
                    self.user,
                    self.passwd,
                    db=dbase,
                    port=self.port,
                    charset=charSet)
            else:
                con = mdb.connect(
                    self.host,
                    self.user,
                    self.passwd,
                    db=dbase,
                    port=self.port)    

            return con

        except mdb.Error as dbError:
            self.onDBError(dbError)

    # checks to see if the db exists:
    def checkDB(self):
        try:
            with closing(self.connect(dbase='')) as connection:
                with closing(connection.cursor()) as cur:
                    cur.execute("SHOW DATABASES;")

                    rows = cur.fetchall()

                    for line in rows:
                        self.logger.debug('Database returned by query: %s', line[0])
                        if line[0] == self.dbName:
                            self.logger.debug('DB %s exists' % (self.dbName))
                            return True 

                    self.logger.info('Database %s was not found' % (self.dbName))
                    return False

        except mdb.Error as dbError:
            self.onDBError(dbError)

    def checkAndCreateDB(self):
        result = self.checkDB()

        # if the DB doesnt exist, create it:
        if not result:
            self.logger.info('Database %s does not exist, attempting to create it..', self.dbName)
            self.createDB()

            # verify that it worked...
            result = self.checkDB()

            if result:
                self.logger.info('created database %s successfully', self.dbName)
    
            else:
                raise MySQLError(0, 'MySQL Error, Could not create database \'%s\'for unknown reason' % (self.dbName))

        return


    # checks to see if a specific table exists..
    def checkTable(self, tableName):
        try:
            with closing(self.connect()) as connection:
                with closing(connection.cursor()) as cur:
                    cur.execute("SHOW TABLES;")

                    rows = cur.fetchall()
                    for line in rows:
                        if line[0] == tableName:
                            self.logger.debug('Table %s exists' % (tableName))
                            return True

                    self.logger.info('Table %s was not found' % (tableName))
                    return False
        except mdb.Error as dbError:
            self.onDBError(dbError)

    def createDB(self):
        try:
            with closing(self.connect(dbase='')) as con:
                with closing(con.cursor()) as cur:
                    cur.execute('CREATE DATABASE %s;' % (self.dbName))
                    con.commit()

        except mdb.Error as dbError:
            self.onDBError(dbError)

        return

    def executeSQL(self, formatString, args=(), charSet=None):
        result = None

        try:
            with closing(self.connect(charSet=charSet)) as conn:
                with closing(conn.cursor()) as cur:
                    self.logger.debug('Executing: %s' % (formatString % args))
                    cur.execute(formatString, args)
                    conn.commit()
                    result = cur.fetchall()

        except mdb.Error as dbError:
            self.onDBError(dbError)

        return result

    def executeSQLDict(self, formatString, args=(), charSet=None):
        result = None

        try:
            with closing(self.connect(charSet=charSet)) as conn:
                with closing(conn.cursor(mdb.cursors.DictCursor)) as cur:
                    self.logger.debug('Executing: %s' % (formatString % args))
                    cur.execute(formatString, args)
                    conn.commit()
                    result = cur.fetchall()

        except mdb.Error as dbError:
            self.onDBError(dbError)

        return result

    def executeSchema(self, schemaFilename):
        try:
            with closing(self.connect()) as conn:
                with closing(conn.cursor()) as cur:
                    self.logger.debug('loading schema from %s' % (schemaFilename))
                    with open(schemaFilename, 'r') as schemaFile:
                        for line in schemaFile:
                            self.logger.debug('Executing SQL from schema: %s' % (line))
                            cur.execute(line)
                        conn.commit()
                    
        except mdb.Error as dbError:
            self.onDBError(dbError)

        return

    def cleanUp(self):
        self.__connections.closeConnections()
        return

    def close(self):
        self.cleanUp()
        return
