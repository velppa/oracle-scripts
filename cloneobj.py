#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    cloneobj.py

    A module for clone objects from one Oracle Database to another.

    history:
    0.1.0 (2013-08-01): Initial version
    0.1.1 (2013-08-01): + Added ability to create object if it doesn't exists
                          on target DB
    0.1.2 (2013-08-02): + Added 'select' attribute to Cloner class
                          which used as select statement if set.
                          If not set then usual SELECT * FROM is used.
                        + Cloner logs total number of inserted rows instead
                          number of insert rows on current step
                        ~ str() replaced to {!s} in string formatting
    0.1.3 (2013-08-12): ~ Added parameter names to format strings
                        + Passwords are now hidden in __repr__
                        + Added `insert` function to Cloner.clone to provide
                          regular insert if bulk insert fails with TypeError
    0.1.4 (2013-08-12): ~ Minor format strings improvements
    0.2.0 (2013-08-14): + Added Cloner.columns and Cloner.where attributes
                        + INSERT in Cloner is now column-aware of cursor it
                          takes to insert -- ability to insert into some columns
    0.2.1 (2013-11-25): ~ Changes in module header, follow PEP-257
                        + Added logging instead of print
    0.2.2 (2014-02-11): ~ Python3 compatibility
    0.3   (2014-02-19): ~ Fixed bug with TypeError
                        + Connection now connects on execute if not active

"""

import re
import datetime
import logging
import cx_Oracle


__version__ = '0.3'
__author__ = 'Pavel Popov'
__email__ = 'pavelpopov@outlook.com'
__date__ = '2014-02-19'
__license__ = 'GPLv3'


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def setup_logger():
    FORMAT = ''
    formatter = logging.Formatter(fmt=FORMAT)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


setup_logger()


class Connection:
    """Provides connection to Oracle DB and corresponding methods."""

    def __init__(self, connection_string):
        cs = connection_string
        self.__connection_string = cs
        self.connection_string = '{user}@{db}'.format(user=cs[0:cs.index('/')], db=cs[cs.rindex('@') + 1:])
        self.conn = None
        self.cursor = None
        self.active = False

    def __repr__(self):
        return "{status} connection to '{conn}'".format(status=self.status(), conn=self.connection_string)

    def connect(self):
        """Establishes connection."""
        if not self.active:
            self.conn = cx_Oracle.connect(self.__connection_string)
            self.cursor = self.conn.cursor()
            self.active = True

    def close(self):
        """Closes connection."""
        if self.active:
            self.cursor.close()
            self.conn.close()
            self.active = False

    def status(self):
        return 'Active' if self.active else 'Not active'

    def commit(self):
        """Commits transaction on connection level."""
        self.conn.commit()
        # todo: add logger instead of print
        print('Commit complete.')

    def object_exists(self, obj):
        q = """SELECT 1
                 FROM all_objects
                WHERE owner = upper(:owner)
                  AND object_name = upper(:name)
                  AND object_type = upper(:type)
        """
        params = {'owner': obj.owner, 'name': obj.name, 'type': obj.type}
        self.cursor.execute(q, params)
        return len(self.cursor.fetchall()) == 1

    def ddl(self, obj):
        q = """SELECT dbms_metadata.get_ddl(upper(:type), upper(:name), upper(:owner)) FROM dual"""
        params = {'owner': obj.owner, 'name': obj.name, 'type': obj.type}
        self.execute(q, params)
        return self.cursor.fetchone()[0].read()

    def ddl_target(self, ddl, from_obj, to_obj):
        # removing schema name from DDL
        ddl = ddl.replace(' {type} "{owner}"."{name}"'.format(type=from_obj.type, owner=from_obj.owner.upper(),
                                                              name=from_obj.name.upper()),
                          ' {type} "{name}"'.format(type=to_obj.type, name=to_obj.name.upper()))

        # remapping tablespace
        r = re.compile('TABLESPACE ".*"')
        tablespace = to_obj.opts['tablespace']
        ddl = r.sub('TABLESPACE "{name}"'.format(name=tablespace) if tablespace is not None else '', ddl)

        return ddl

    def log(self, query, params=None):
        if params is None:
            print("ISSUING '{query}' ON '{db}'".format(query=query, db=self.connection_string))
        else:
            print("ISSUING '{query}' WITH PARAMS {params!s} ON '{db}'".format(query=query, params=params,
                                                                              db=self.connection_string))

    def execute(self, query, params=None, print_output=False):
        """
        Execute statement at the connection.
        If connection is not active tries to connect first.

        Arguments:
        query -- statement to be executed
        params -- dictionary with bind variables
        print_output -- boolean flag to print output to stdout

        """

        if not self.active:
            self.connect()

        if isinstance(params, dict):
            self.log(query, params)
            self.cursor.execute(query, params)
        else:
            self.log(query)
            self.cursor.execute(query)

        if print_output:
            for row in self.cursor:
                # todo: tab-separated print instead of built-one
                print(row)


class DBObject:
    """Describe Oracle Database object"""
    def __init__(self, name=None, owner=None, type='TABLE', opts=None):
        # todo: maybe combine owner and object_name together?
        self.owner = owner.lower() if owner is not None else None
        self.name = name.lower() if name is not None else None
        self.type = type
        self.opts = {'tablespace': None, 'truncate': False, 'create_if_not_exists': False}
        if isinstance(opts, dict):
            self.opts.update(opts)

    def __repr__(self):
        return '{type} {owner}.{name}'.format(type=self.type.lower(), owner=self.owner, name=self.name)


class Cloner:
    """Copies content of one object to another"""

    BULK_ROWS = 25000
    # BULK_ROWS = 100000

    def __init__(self, from_db, from_obj, to_db, to_obj):
        self.from_db = from_db
        self.to_db = to_db
        self.from_obj = from_obj
        self.to_obj = to_obj
        self.select = None
        self.columns = None
        self.where = None

        if not (self.from_obj.type == 'TABLE' and self.to_obj.type == 'TABLE'):
            raise Exception('Currently only tables are supported')

        if self.from_obj.name is None:
            raise Exception('Specify object name for source object')

        if self.to_obj.name is None:
            self.to_obj.name = self.from_obj.name

        if (self.from_db.connection_string == self.to_db.connection_string and
                self.from_obj.name == self.to_obj.name and
                self.from_obj.owner == self.to_obj.owner and
                self.from_obj.type == self.to_obj.type):
            raise Exception('Objects are equal')

    def connect(self):
        self.from_db.connect()
        self.set_owner(self.from_obj, self.from_db)
        self.to_db.connect()
        self.set_owner(self.to_obj, self.to_db)

    def close(self):
        """Close connections to Databases"""
        self.from_db.close()
        self.to_db.close()

    @staticmethod
    def set_owner(obj, conn):
        if obj.owner is None:
            conn.execute('SELECT LOWER(user) FROM dual')
            obj.owner = conn.cursor.fetchone()[0]

    def clone(self):
        """Clone object from_obj to to_obj"""
        # todo: measure time spent on transfer
        self.connect()

        if not self.to_db.object_exists(self.to_obj):
            if self.to_obj.opts['create_if_not_exists']:
                self.from_obj.opts['tablespace'] = self.to_obj.opts['tablespace']
                from_ddl = self.from_db.ddl(self.from_obj)
                to_ddl = self.to_db.ddl_target(from_ddl, self.from_obj, self.to_obj)
                # todo: create target objects on cursor basis instead of object basis
                self.to_db.execute(to_ddl)
            else:
                raise Exception('First, create {obj} at {db}'.format(obj=self.to_obj, db=self.to_db.connection_string))

        if self.select is None:
            where = '1=1' if self.where is None else self.where
            columns = '*' if self.columns is None else self.columns
            self.select = '''SELECT {columns}
                               FROM {owner}.{name}
                              WHERE 1=1
                                AND {where}
                          '''.format(owner=self.from_obj.owner, name=self.from_obj.name,
                                     columns=columns, where=where)

        if self.to_obj.opts['truncate'] and self.to_obj.type == 'TABLE':
            self.to_db.execute('TRUNCATE TABLE {owner}.{name}'.format(owner=self.to_obj.owner, name=self.to_obj.name))

        self.from_db.execute(self.select)

        desc = self.from_db.cursor.description
        columns = ', '.join([x[0] for x in desc]).lower()
        placeholders = ', '.join([':{!s}'.format(x) for x in range(len(desc))])

        insert = 'INSERT INTO {owner}.{table}({columns}) VALUES({placeholders})'
        insert = insert.format(owner=self.to_obj.owner, table=self.to_obj.name,
                               columns=columns, placeholders=placeholders)

        self.to_db.log(insert)
        self.to_db.cursor.prepare(insert)

        def insert(rows):
            rowcount = 0
            for row in rows:
                try:
                    self.to_db.cursor.execute(None, row)
                except TypeError as e:
                    print('TypeError on row occurred: {}'.format(e))
                    print(row)
                rowcount += 1
            return rowcount

        def bulk_insert(rows, total_rows=0):
            if rows:
                try:
                    self.to_db.cursor.executemany(None, rows)
                    rowcount = self.to_db.cursor.rowcount
                except TypeError as e:
                    print('TypeError occurred: {}'.format(e))
                    print('Trying to insert row-by-row')
                    rowcount = insert(rows)
                print('{time}: {x} rows processed'.format(time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                          x=total_rows + rowcount))
                return rowcount
            else:
                print('Empty set - nothing to insert')
                return 0

        total_rows = 0
        i = 0
        rows = []
        for row in self.from_db.cursor:
            i += 1
            rows.append(row)
            if i == Cloner.BULK_ROWS:
                total_rows += bulk_insert(rows, total_rows)
                i = 0
                rows = []

        bulk_insert(rows, total_rows)
        self.to_db.commit()

    def __repr__(self):
        return 'Cloner from {from_obj} at {from_db} to '\
               '{to_obj} at {to_db}'.format(from_obj=self.from_obj, from_db=self.from_db.connection_string,
                                            to_obj=self.to_obj, to_db=self.to_db.connection_string)


def example():
    # todo: accept command line params
    from_db = Connection('user/pass@qwer')
    from_obj = DBObject(owner='SCHEME', name='SOME_TABLE')
    to_db = Connection('user2/pass2@qwer2')
    to_obj = DBObject(name='SOME_TABLE2',
                      opts={'truncate': True, 'create_if_not_exists': False})
    cloner = Cloner(from_db=from_db, from_obj=from_obj,
                    to_db=to_db, to_obj=to_obj)
    cloner.select = """SELECT table_name , owner
                         FROM all_tables p
                        WHERE 1=1
                          AND rownum < 20"""
    cloner.connect()
    print(cloner)
    cloner.clone()
    cloner.close()

if __name__ == '__main__':
    example()
