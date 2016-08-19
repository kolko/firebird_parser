# -*- coding: utf-8 -*-
from header import *


class DBReader(object):
    def __init__(self, filename):
        self.db_file = DbFile(filename)
        self.db_header = self.read_page(0)
        self.db_file.pagesize = self.db_header.hdr_page_size

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