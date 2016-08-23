# -*- coding: utf-8 -*-
import struct
from db_file import DBReader
from table_reader import *


def main():
    db_reader = DBReader('test_db.gdb')

    print(list(Table_Rdb_Formats(db_reader).get_rows()))
    print(list(Table_Rdb_Fields(db_reader).get_rows()))
    print(list(Table_Rdb_Relation_Fields(db_reader).get_rows()))


if __name__ == '__main__':
    main()