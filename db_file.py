# -*- coding: utf-8 -*-
from header import *
from table_reader import Table_RdbPages, Table_Rdb_Database, Table_Rdb_Relations
from consts import charset_map


class DBReader(object):
    def __init__(self, filename):
        self.db_file = DbFile(filename)
        self.db_header = self.read_page(0)
        self.db_file.pagesize = self.db_header.hdr_page_size
        self._pages_table = None
        self._db_charset = None
        self._relations = None

    def read_page(self, number):
        header = PageHeader(self.db_file, number)
        if header.pag_type == 1:
            payload = Headerpage(self.db_file, number)
        elif header.pag_type == 4:
            payload = PointerPage(self.db_file, number)
        elif header.pag_type == 5:
            payload = DataPage(self.db_file, number)
        else:
            class A(object):
                pass
            payload = A()
            # raise Exception('Unsupported page type {}'.format(header.pag_type))
        payload.header = header
        return payload

    @property
    def pages_table(self):
        if not self._pages_table:
            self._pages_table = list(Table_RdbPages(self).get_rows())
        return self._pages_table

    @property
    def db_charset(self):
        if not self._db_charset:
            reader = Table_Rdb_Database(self)
            rows = list(reader.get_rows())
            assert len(rows) == 1
            db_charset = rows[0].p_character_set_name
            self._db_charset = charset_map[db_charset]
        return self._db_charset

    @property
    def relations(self):
        if not self._relations:
            self._relations = list(Table_Rdb_Relations(self).get_rows())
        return self._relations


class DbFile(object):
    def __init__(self, filename):
        self.db_file = open(filename, 'rb')
        self.pagesize = -1

    def close(self):
        self.db_file.close()

    def get_page_data(self, num):
        if num != 0:
            assert self.pagesize != -1
        self.db_file.seek(num * self.pagesize)
        return self.db_file.read(10240)