import logging
import psycopg2
import mysql.connector
from mysql.connector import Error

logger = logging.getLogger(__name__)


class PSQL_Connection():

    def __init__(self, host, user, password, dbname=None):

        self._host = host
        self._user = user
        self._password = password
        self._connection = None
        self._current_db = dbname
        self._last_table = None
        self._last_updated = ''

        if self._current_db is None:
            self.create_server_connection()
        else:
            self.create_db_connection(self._current_db)

    def get_connection(self):

        return self._connection

    def get_currentdb(self):

        return self._current_db

    def get_current_table(self):

        return self._last_table

    def get_last_updated(self):

        return self._last_updated

    def set_last_updated(self):

        self.last_updated(self._last_table)


    def create_server_connection(self):
        """
        Connect to SQL server on given host with given credentials
        :return:
        """

        try:
            self._connection = psycopg2.connect(host = self._host, database = self.currentdb, user = self._user, password = self._password)
        except Exception as err:
            self.close_connection()
            raise err


    def create_db_connection(self, dbname):
        """
        Connect to DB on server
        :param dbname: name of db
        :return:
        """

        try:
            self._connection = psycopg2.connect(host = self._host, database = dbname, user = self._user, password = self._password)
            self._current_db = dbname
        except Exception as err:
            self.close_connection()
            raise err


    def close_connection(self):
        """
        Close server connection
        :return:
        """

        if self._connection is not None:
            self._connection.close()


    def create_database(self, dbname):
        """
        Create a DB on server
        :param dbname: name of DB
        :return:
        """

        query = f"CREATE DATABASE {dbname}"
        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            cursor.close()
        except Exception as err:
            cursor.close()
            raise err


    def execute_query(self, query):
        """
        Execute a SQL query
        :param query: SQL query
        :return:
        """

        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            self._connection.commit()
            cursor.close()
        except Exception as err:
            cursor.close()
            raise err


    def execute_query_multi(self, sql, val):
        """
        Execute query with multiple values
        :param sql: sql command to execute
        :param val: values to execute on
        :return:
        """

        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, val)
            self._connection.commit()
            cursor.close()
        except Exception as err:
            cursor.close()
            raise err


    def create_table(self, tablename, columns):
        """
        Create table in DB
        :param tablename: name of table
        :param columns: name of columns in table, data type and default value
        :return:
        """

        query = f"CREATE TABLE {tablename} ({columns})"
        self.execute_query(query)


    def table_insert(self, tablename, columns, values):
        """
        Insert values into table columns
        :param tablename: table name
        :param columns: column names
        :param values: values to insert
        :return:
        """

        query = f"INSERT INTO {tablename}({columns}) VALUES({values})"
        self.execute_query(query)


    def table_insert_ignore(self, tablename, columns, values):
        """
        Insert values into table without creating duplicate values.

        REQUIRES PRIMARY KEY SET ON TABLE

        :param tablename: name of table
        :param columns: column names
        :param values: values to insert
        :return:
        """

        query = f"INSERT INTO {tablename}({columns}) VALUES({values}) ON CONFLICT DO NOTHING"
        self.execute_query(query)


    def table_insert_multi(self, tablename, columns, values):
        """
        Insert multiple values into table, escapes characters using %s preventing SQL injection
        :param tablename: name of table
        :param columns: columns in table
        :param values: values to insert into given columns
        :return:
        """

        ns = ', '.join(['%s'] * len(values))
        sql = f"INSERT INTO {tablename}({columns}) VALUES {ns}"
        self.execute_query_multi(sql, values)


    def table_insert_multi_unique(self, tablename, columns, values):
        """
        Insert multiple values into table without duplicates, escapes characters using %s preventing SQL injection
        :param tablename: name of table
        :param columns: column names
        :param values: values to insert into given columns
        :return:
        """

        ns = ', '.join(['%s'] * len(values))
        sql = f"INSERT INTO {tablename}({columns}) VALUES {ns} ON CONFLICT DO NOTHING"
        self.execute_query_multi(sql, values)


    def read_query(self, query):
        """
        Read data from table
        :param query: query to execute
        :return: result of query
        """

        cursor = self._connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
        except Exception as err:
            cursor.execute("ROLLBACK")
            cursor.close()
            raise err
        return result


    def drop_table(self, tablename):
        """Drop table from schema

        Args:
            tablename (str): name of table to drop
        """

        query = f"DROP TABLE IF EXISTS {tablename};"
        self.execute_query(query)


    def update_record(self, tablename, column1, val, wherecolumn, whereval):
        """Update a record where condition met with a new value

        Args:
            tablename (str): Table name
            column1 (str): Column to update value at
            val (str): New value
            wherecolumn (str): Column for where condition
            whereval (str): Value of where condition, value in wherecolumn
        """

        sql = f"UPDATE {tablename} SET {column1} = %s WHERE {wherecolumn} = %s"
        val = (val, whereval)
        print(val)
        self.execute_query_multi(sql, val)


    def delete_record(self, tablename, column, value):
        """Delete records from table

        Args:
            tablename (str): Table name
            column (str): column name where value of row is to delete
            value (str): value in column to delete row
        """

        sql = f"DELETE FROM {tablename} WHERE {column} = {value}"
        self.execute_query(sql)

    
    def upsert_records(self, tablename, pk, columns, values):
        """
        Update insert records

        Args:
            tablename (string): table name
            columns (string): columns in table
            values (string): values to upsert
        """

        excluded = ', '.join([f'EXCLUDED.{x}' for x in columns.replace(',', '').split()])
        sql = f"INSERT INTO {tablename}({columns}) VALUES({values}) ON CONFLICT ({pk}) DO UPDATE SET ({columns}) = ({excluded})"
        self.execute_query(sql)


    def last_updated(self, table):

        sql = f"SELECT UPDATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA = '{self._current_db}' AND TABLE_NAME = '{table}'"
        return self.execute_query(sql)


    currentdb = property(get_currentdb, )

    currenttable = property(get_current_table, )

    current_connection = property(get_connection, )

    lastupdated = property(get_last_updated, )


class SQL_Connection():

    def __init__(self, host, user, password, dbname=None):

        self._host = host
        self._user = user
        self._password = password
        self._connection = None
        self._current_db = dbname
        self._last_table = None
        self._last_updated = ''

        if self._current_db is None:
            self.create_server_connection()
        else:
            self.create_db_connection(self._current_db)

    def get_connection(self):

        return self._connection

    def get_currentdb(self):

        return self._current_db

    def get_current_table(self):

        return self._last_table

    def get_last_updated(self):

        return self._last_updated

    def set_last_updated(self):

        self.last_updated(self._last_table)


    def create_server_connection(self):
        """
        Connect to SQL server on given host with given credentials
        :return:
        """

        try:
            self._connection = mysql.connector.connect(host=self._host,
                                                 user=self._user,
                                                 passwd=self._password)
        except Error as err:
            self.close_connection()
            raise err


    def create_db_connection(self, dbname):
        """
        Connect to DB on server
        :param dbname: name of db
        :return:
        """

        try:
            self._connection = mysql.connector.connect(host=self._host,
                                                 user=self._user,
                                                 passwd=self._password,
                                                 database=dbname
            )
            self._current_db = dbname
        except Error as err:
            self.close_connection()
            raise err


    def close_connection(self):
        """
        Close server connection
        :return:
        """

        if self._connection is not None:
            self._connection.close()


    def create_database(self, dbname):
        """
        Create a DB on server
        :param dbname: name of DB
        :return:
        """

        query = f'CREATE DATABASE {dbname};'
        cursor = self._connection.cursor(query)
        try:
            cursor.execute(query)
            cursor.close()
        except Error as err:
            cursor.close()
            raise err


    def select_db(self, dbname):

        query = f'''USE {dbname}'''
        self.execute_query(query)
        self._current_db = dbname


    def execute_query(self, query):
        """
        Execute a SQL query
        :param query: Query command
        :return:
        """

        cursor = self._connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
        except Exception as err:
            cursor.close()
            raise err

        return result


    def execute_query_multi(self, sql, val):
        """
        Execute query with multiple values, escapes characters using %s preventing SQL injection
        :param sql: sql command to execute
        :param val: values to execute on
        :return:
        """

        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, val)
            self._connection.commit()
            cursor.close()
        except Exception as err:
            cursor.close()
            raise err


    def create_table(self, tablename, columns):
        """
        Create table in DB
        :param tablename: name of table
        :param columns: name of columns in table, data type and default value
        :return:
        """

        query = f'''CREATE TABLE {tablename} ({columns});'''
        self.execute_query(query)


    def table_insert(self, tablename, values):
        """
        Insert values into table columns
        :param tablename: table name
        :param values: values to insert
        :return:
        """

        query = f'''INSERT INTO {tablename} VALUES {values};'''
        self.execute_query(query)


    def table_insert_unique(self, tablename, values):
        """
        Insert values into table without creating duplicate values.

        REQUIRES PRIMARY KEY SET ON TABLE

        :param tablename: name of table
        :param values: values to insert
        :return:
        """

        query = f'''INSERT IGNORE INTO {tablename} VALUES {values};'''
        self.execute_query(query)


    def table_insert_multi(self, tablename, columns, values):
        """
        Insert multiple values into table
        :param tablename: name of table
        :param columns: columns in table
        :param values: values to insert into given columns
        :return:
        """

        ns = []
        for i in range(len(values)):
            ns.append('%s')
        ns = ', '.join(ns)
        sql = f'''INSERT INTO {tablename} {columns} VALUES ({ns})'''
        self.execute_query_multi(sql, values)


    def table_insert_multi_unique(self, tablename, columns, values):
        """
        Insert multiple values into table without duplicates
        :param tablename: name of table
        :param columns: columns in table
        :param values: values to insert into given columns
        :return:
        """

        ns = []
        for i in range(len(values)):
            ns.append('%s')
        ns = ', '.join(ns)
        sql = f'''INSERT IGNORE INTO {tablename} {columns} VALUES ({ns})'''
        self.execute_query_multi(sql, values)


    def read_query(self, query):
        """
        Read data from table
        :param query: query to execute
        :return: result of query
        """

        cursor = self._connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
        except Error as err:
            cursor.close()
            raise err

        return result


    def create_user(self, user, password):

        query = f'''CREATE USER '{user}'@'{self._host}' IDENTIFIED BY '{password}'''
        self.execute_query(query)


    def print_table_columns(self, tablename):

        query = f'''SELECT * FROM INFORMATION_SCHEMA.COLUMNS'''
        self.execute_query(query)


    def rename_column(self, tablename, oldcol, newcol):

        query = f'''ALTER TABLE {tablename} RENAME COLUMN {oldcol} TO {newcol};'''
        self.execute_query(query)


    def add_unique_index(self, tablename, column):

        query = f'''ALTER TABLE {tablename} ADD UNIQUE INDEX({column});'''
        self.execute_query(query)


    def add_primary_key(self, tablename, columns):

        keycols = ', '.join(columns)
        query = f'''ALTER TABLE {tablename} ADD PRIMARY KEY ({keycols})'''
        self.execute_query(query)


    def drop_table(self, tablename):

        query = f'''DROP TABLE IF EXISTS {tablename};'''
        self.execute_query(query)


    def update_record(self, tablename, column, oldval, newval):

        sql = f'''UPDATE {tablename} SET {column} = %s WHERE {column} = %s '''
        val = (oldval, newval)
        self.execute_query_multi(sql, val)


    def delete_record(self, tablename, column, value):

        sql = f'''DELETE FROM {tablename} WHERE {column} = %s'''
        val = (value, )
        self.execute_query_multi(sql, val)


    def last_updated(self, table):

        sql = f'''SELECT UPDATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA = '{self._current_db}' AND TABLE_NAME = '{table}';'''
        return self.execute_query(sql)


    currentdb = property(get_currentdb, )

    currenttable = property(get_current_table, )

    current_connection = property(get_connection, )

    lastupdated = property(get_last_updated, )