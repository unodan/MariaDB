########################################################################################################################
#    File: db-maria.py
# Purpose: Class to make working with MariaDB/MySQL a lot easier.
#  Author: Dan Huckson, https://github.com/unodan
########################################################################################################################
import os
import time
import pymysql
import logging as lg

version = '0.1'


class MariaDB:
    def __init__(self, log_file=None):
        self.host = None
        self.conn = None
        self.cursor = None
        self.db_name = None
        self.db_user = None
        self.db_password = None
        self.class_name = self.__class__.__name__

        if not log_file:
            log_file = self.class_name + '.log'

        lg.basicConfig(filename=log_file, format='%(levelname)s:%(name)s:%(asctime)s:%(message)s',
                       datefmt='%Y/%m/%d %I:%M:%S', level=lg.DEBUG)
        lg.info('__init__:Object created')

    def use(self, database, **kwargs):
        database = kwargs.get('database', database)

        sql = f'USE {database}'
        try:
            self.cursor.execute(sql)
            self.db_name = database
            self.set_autocommit(autocommit=kwargs.get('autocommit', True))
            lg.info(f'use:{sql}')
            return True
        except Exception as err:
            lg.error(f'use:{str(err)}:{sql}')

    def dump(self, database):
        try:
            t = time.strftime('%Y-%m-%d_%H:%M:%S')
            os.popen('mysqldump -u %s -p%s -h %s -e --opt -c %s | gzip -c > %s.gz' % (
                self.db_user, self.db_password, self.host, database, database + '_' + t))
            lg.info('dump:' + database + '_' + t + '.gz')
            return True
        except Exception as err:
            lg.error('dump:' + str(err))

    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
            self.conn = None
            self.cursor = None
            lg.info(f'close:Closed database ({self.db_name})')
            self.db_name = None
            return True
        except Exception as err:
            lg.error('close:' + str(err))

    def commit(self):
        try:
            self.conn.commit()
            lg.info('commit')
            return True
        except Exception as err:
            lg.error(f'commit:{str(err)}')

    def connect(self, database=None, **kwargs):
        info = kwargs.get('connection')
        if not info:
            return

        database = self.db_name = kwargs.get('database', database)
        try:
            self.conn = pymysql.connect(
                host=info['host'], port=info['port'], user=info['user'],
                passwd=info['password'], charset=info['charset'])

            self.db_user = info['user']
            self.db_password = info['password']
            self.host = info['host']
            self.cursor = self.conn.cursor()

            if database:
                if not self.database_exist(database):
                    self.create_database(database)

                self.use(database)

            lg.info(f'connect:Connection authenticated:('
                    f'host={info["host"]}, '
                    f'port={info["port"]}, '
                    f'user={info["user"]}, '
                    f'database={database} '
                    f'charset={info["charset"]})')

            return True

        except Exception as err:
            lg.error(f'connect:{str(err)}')

    def execute(self, sql, args=None):
        try:
            if not args:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, args)
            lg.info(f'execute:{sql}')
            return True

        except Exception as err:
            lg.error(f'execute:{str(err)}:{sql}')

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        finally:
            pass

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        finally:
            pass

    def drop_table(self, table):
        sql = f'DROP TABLE {table}'
        try:
            self.cursor.execute(sql)
            lg.info(f'drop_table:{sql}')
            return True
        except Exception as err:
            lg.error(f'drop_table:{str(err)}:{sql}')

    def drop_index(self, table, index):
        sql = f'DROP INDEX {index} ON {table};'
        try:
            self.cursor.execute(sql)
            lg.info(f'drop_index:{sql}')
            return True
        except Exception as err:
            lg.error(f'drop_index:{str(err)}:{sql}')

    def drop_database(self, database, **kwargs):
        database = kwargs.get('database', database)

        sql = f'DROP DATABASE {database};'
        try:
            self.cursor.execute(sql)
            lg.info(f'drop_database:{sql}')
            return True
        except Exception as err:
            lg.error(f'drop_database:{str(err)}:{sql}')

    def create_table(self, table, sql, **kwargs):
        database = kwargs.get('database', self.db_name)
        sql = f'CREATE TABLE {table} ({sql}) ENGINE=%s'

        database_engine = kwargs.get('database_engine', 'InnoDB')

        try:
            if database == self.db_name:
                self.cursor.execute(sql, (database_engine,))
            else:
                db = self.db_name
                self.use(database)
                self.cursor.execute(sql)
                self.use(db)
            lg.info('create_table:' + sql)
            return True
        except Exception as err:
            lg.error(f'create_table:{str(err)}:{sql}')

    def create_index(self, table, column, index):
        sql = f'CREATE INDEX {index} ON {table}({column});'
        try:
            self.cursor.execute(sql)
            lg.info(f'create_index:{sql}')
            return True
        except Exception as err:
            lg.error(f'create_index:{str(err)}:{sql}')

    def create_database(self, database, **kwargs):
        database = kwargs.get('database', database)

        sql = f'CREATE DATABASE {database};'
        try:
            self.cursor.execute(sql)
            lg.info(f'create_database:{sql}')
            return True
        except Exception as err:
            lg.error(f'create_database:{str(err)}:{sql}')

    def row_exist(self, table, _id):
        sql = f'SELECT id FROM {table} WHERE id=%s;'
        if self.execute(sql, (_id,)):
            if self.fetchone():
                return True

    def table_exist(self, table, **kwargs):
        database = kwargs.get('database', self.db_name)
        sql = 'SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_name=%s;'

        try:
            self.cursor.execute(sql, (database, table))
            if self.cursor.fetchone():
                return True
        except Exception as err:
            lg.error(f'table_exist:{str(err)}')

    def index_exist(self, table, index, **kwargs):
        result = False
        database = kwargs.get('database', self.db_name)

        if database:
            sql = f'SELECT 1 FROM information_schema.statistics WHERE table_schema="{database}" AND ' \
                  f'table_name="{table}" AND index_name="{index}";'

            try:
                if database == self.db_name:
                    if self.cursor.execute(sql) and self.cursor.fetchone():
                        result = True
                else:
                    db_name = self.db_name
                    self.use(database)
                    if self.cursor.execute(sql) and self.cursor.fetchone():
                        result = True
                    self.use(db_name)
            except Exception as err:
                lg.error(f'index_exist:{str(err)}')

        return result

    def database_exist(self, database, **kwargs):
        database = kwargs.get('database', database)

        sql = 'SHOW DATABASES;'
        try:
            self.cursor.execute(sql)
            for r in self.cursor.fetchall():
                if database in r:
                    return True
        except Exception as err:
            lg.error(f'database_exist:{str(err)}')

    def insert_row(self, table, row):
        column_names = []
        for name in self.get_columns_metadata(table):
            column_names.append(name[3])

        sql = f"INSERT INTO {table} ({','.join(column_names[1:])}) VALUES ({('%s,' * len(row)).rstrip(',')});"
        try:
            self.cursor.execute(sql, row)
            lg.info(f'insert_row:{sql}')
            return self.cursor.lastrowid
        except Exception as err:
            lg.error(f'insert_row:{str(err)}:{sql}')

    def update_row(self, table, row, _id):
        parts = ''
        sql = f'UPDATE {table} SET '
        for f in row:
            parts += (f + '=%s,')
        sql = sql + parts[:-1] + ' WHERE id=%s;'

        try:
            self.cursor.execute(sql, row)
            lg.info(f'update_row:{sql}')
            return True
        except Exception as err:
            lg.error(f'update_row:{str(err)}:{sql}')

    def delete_row(self, table, _id):
        sql = f'DELETE FROM {table} WHERE id = %s;'
        try:
            self.cursor.execute(sql, (_id,))
            lg.info(f'delete_row:{sql}')
            return True
        except Exception as err:
            lg.error(f'delete_row:{str(err)}:{sql}')

    def get_databases(self):
        sql = 'SHOW DATABASES;'
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            if rows:
                return tuple([i[0] for i in rows])
        except Exception as err:
            lg.error(f'get_databases:{str(err)}:{sql}')

    def get_tables(self, database=None, **kwargs):
        if not database:
            database = self.db_name

        sql = "SHOW TABLES;"
        database = kwargs.get('database', database)

        try:
            if database == self.db_name:
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
            else:
                db = self.db_name
                self.use(database)
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                self.use(db)

            if rows:
                return tuple([i[0] for i in rows])
        except Exception as err:
            lg.error(f'get_tables:{str(err)}:{sql}')

    def get_column_metadata(self, table, column, **kwargs):
        database = kwargs.get('database', self.db_name)

        sql = f"SELECT * FROM information_schema.COLUMNS WHERE " \
              f"TABLE_SCHEMA='{database}' AND TABLE_NAME='{table}' AND COLUMN_NAME='{column}';"
        try:
            if database == self.db_name:
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
            else:
                db = self.db_name
                self.use(database)
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                self.use(db)

            if rows:
                return tuple(rows)
        except Exception as err:
            lg.error(f'get_databases:{str(err)}:{sql}')

    def get_columns_metadata(self, table, **kwargs):
        database = kwargs.get('database', self.db_name)

        sql = f"SELECT * FROM information_schema.COLUMNS WHERE " \
              f"TABLE_SCHEMA='{database}' AND " \
              f"TABLE_NAME = '{table}';"
        try:
            if database == self.db_name:
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
            else:
                db = self.db_name
                self.use(database)
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                self.use(db)
            if rows:
                return tuple(rows)
        except Exception as err:
            lg.error(f'get_columns_metadata:{str(err)}:{sql}')

    #############################################

    def set_autocommit(self, **kwargs):
        sql = f'SET AUTOCOMMIT = {kwargs.get("autocommit", True)};'

        try:
            self.cursor.execute(sql)
            lg.info(f'set_autocommit:{sql}')
            return True
        except Exception as err:
            lg.error(f'set_autocommit:{str(err)}:{sql}')

    def get_table_status(self, table=None, **kwargs):
        sql = "SHOW TABLE STATUS"
        database = kwargs.get('database', self.db_name)

        if table:
            sql += f" WHERE Name='{table}'"

        if database or table:
            try:
                if database == self.db_name:
                    self.cursor.execute(sql)
                    rows = self.cursor.fetchall()
                else:
                    db = self.db_name
                    self.use(database)
                    self.cursor.execute(sql)
                    rows = self.cursor.fetchall()
                    self.use(db)
                if rows:
                    return rows
            except Exception as err:
                lg.error(f'get_table_status:{str(err)}:{sql}')
