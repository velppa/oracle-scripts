# -*- coding: utf-8 -*-
"""
    cloneobj
    ~~~~~~~~

    A tool for cloning objects from one Oracle Database to another.

    :copyright: (c) 2013 by Pavel Popov
    :license: GPLv3
"""

__version__ = '0.1-01-AUG-2013'
__author__ = 'Pavel Popov, pavelpopov@outlook.com'

import cx_Oracle
import re

BULK_ROWS = 1000

class Connection:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.conn = None
        self.cursor = None
        self.active = False

    def __repr__(self):
        return "{} connection to '{}'".format(self.status(), self.connection_string)

    def connect(self):
        if not self.active:
            self.conn = cx_Oracle.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            self.active = True

    def close(self):
        if self.active:
            self.cursor.close()
            self.conn.close()
            self.active = False

    def status(self):
        return 'Active' if self.active else 'Not active'

    def commit(self):
        self.conn.commit()
        print('Commited')

    def object_exists(self, obj):
        q = """SELECT 1
                 FROM all_objects
                WHERE owner = upper(:owner)
                  AND object_name = upper(:name)
                  AND object_type = upper(:type)"""
        params = {'owner': obj.owner, 'name': obj.name, 'type': obj.type}
        self.cursor.execute(q, params)
        return len(self.cursor.fetchall()) == 1

    def ddl(self, obj):
        q = """SELECT dbms_metadata.get_ddl(:type, upper(:name), upper(:owner))
                 FROM dual"""
        params = {'owner': obj.owner, 'name': obj.name, 'type': obj.type}
        self.execute(q, params)
        return self.cursor.fetchone()[0].read()

    def ddl_target(self, ddl, from_obj, to_obj):
        # removing schema name from DDL
        ddl = ddl.replace(' {} "{}"."{}"'.format(from_obj.type, from_obj.owner.upper(), from_obj.name.upper()),
                          ' {} "{}"'.format(to_obj.type, to_obj.name.upper()))

        # remapping tablespace
        r = re.compile('TABLESPACE ".*"')
        tablespace = to_obj.opts['tablespace']
        ddl = r.sub('TABLESPACE "{}"'.format(tablespace) if tablespace is not None else '', ddl)

        return ddl

    def log(self, query, params=None):
        if params is None:
            print "ISSUING '{}' ON '{}'".format(query, self.connection_string)
        else:
            print "ISSUING '{}' WITH PARAMS {} ON '{}'".format(query, str(params), self.connection_string)

    def execute(self, query, params=None, print_output=False):
        if isinstance(params, dict):
            self.log(query, params)
            self.cursor.execute(query, params)
        else:
            self.log(query)
            self.cursor.execute(query)

        if print_output:
            for row in self.cursor:
                print(row)


class DBObject:
    def __init__(self, name=None, owner=None, type='TABLE', opts=None):
        self.owner = owner.lower() if owner is not None else None
        self.name = name.lower() if name is not None else None
        self.type = type
        self.opts = {'tablespace': None, 'truncate': False, 'create_if_not_exists': False}
        if isinstance(opts, dict):
            self.opts = dict(self.opts.items() + opts.items())

    def __repr__(self):
        return '{} {}.{}'.format(self.type, self.owner, self.name)


class Cloner:
    def __init__(self, from_db, to_db, from_obj, to_obj):
        self.from_db = from_db
        self.to_db = to_db
        self.from_obj = from_obj
        self.to_obj = to_obj

        if self.from_obj.name is None:
            raise Exception('Specify object name for source object')

        if self.to_obj.name is None:
            self.to_obj.name = self.from_obj.name

        if self.from_db.connection_string == self.to_db.connection_string and \
                        self.from_obj.name == self.to_obj.name and \
                        self.from_obj.owner == self.to_obj.owner and \
                        self.from_obj.type == self.to_obj.type:
            raise Exception('Objects are the same')

    def connect(self):
        self.from_db.connect()
        self.set_owner(self.from_obj, self.from_db)
        self.to_db.connect()
        self.set_owner(self.to_obj, self.to_db)

    def close(self):
        self.from_db.close()
        self.to_db.close()


    def set_owner(self, obj, conn):
        if obj.owner is None:
            conn.execute('select lower(user) from dual')
            owner = conn.cursor.fetchone()[0]
            obj.owner = owner

    def clone(self):
        self.connect()

        if not self.to_db.object_exists(self.to_obj):
            if self.to_obj.opts['create_if_not_exists']:
                self.from_obj.opts['tablespace'] = self.to_obj.opts['tablespace']
                from_ddl = self.from_db.ddl(self.from_obj)
                to_ddl = self.to_db.ddl_target(from_ddl, self.from_obj, self.to_obj)
                self.to_db.execute(to_ddl)
            else:
                raise Exception('Create object {} at {}'.format(self.to_obj, self.to_db.connection_string))

        if self.to_obj.opts['truncate']:
            self.to_db.execute('TRUNCATE {} {}.{}'.format(self.to_obj.type, self.to_obj.owner, self.to_obj.name))

        select = 'SELECT * FROM {}.{}'.format(self.from_obj.owner, self.from_obj.name)
        self.from_db.execute(select)

        desc = self.from_db.cursor.description
        insert = 'INSERT INTO {}.{} VALUES({})'.format(self.to_obj.owner,
                                                       self.to_obj.name,
                                                       ', '.join([':'+str(x) for x in range(len(desc))]))
        self.to_db.log(insert)
        self.to_db.cursor.prepare(insert)

        def bulk_insert(rows):
            if rows:
                self.to_db.cursor.executemany(None, rows)
                rowcount = self.to_db.cursor.rowcount
                print('Inserted {} rows'.format(rowcount))
                return rowcount
            else:
                print('Empty set - nothing to insert')
                return 0

        i = 0
        rows = []
        for row in self.from_db.cursor:
            rows.append(row)
            i += 1
            if i == BULK_ROWS:
                bulk_insert(rows)
                i = 0
                rows = []

        bulk_insert(rows)
        self.to_db.commit()

    def __repr__(self):
        return 'Cloner from {} at {} to {} at {}'.format(self.from_obj, self.from_db.connection_string,
                                                         self.to_obj, self.to_db.connection_string)


def main():

    from_db = Connection('user/pass@qwer')

    from_obj = DBObject(owner='SCHEME', name='SOME_TABLE')

    to_db = Connection('user2/pass2@qwer2')

    to_obj = DBObject(opts={'truncate':True, 'create_if_not_exists':True})

    cloner = Cloner(from_db=from_db, from_obj=from_obj,
                    to_db=to_db, to_obj=to_obj)

    cloner.connect()
    print(cloner)

    cloner.clone()

    cloner.close()

if __name__ == '__main__':
    main()
