# -*- coding: utf-8 -*-
import struct
from db_file import DBReader
from table_reader import *


def get_db_codepage(db_reader, pages_table):
    reader = Table_Rdb_Database(db_reader, pages_table)
    rows = list(reader.get_rows())
    assert len(rows) == 1
    return rows[0].p_character_set_name

def main():
    db_reader = DBReader('test_db.gdb')
    pages_table = list(Table_RdbPages(db_reader).get_rows())
    db_charset = get_db_codepage(db_reader, pages_table)
    db_reader.charset = db_charset

    relations = list(Table_Rdb_Relations(db_reader, pages_table).get_rows())

    # print(list(Table_Rdb_Formats(db_reader, pages_table, relations).get_rows()))
    # print(list(Table_Rdb_Fields(db_reader, pages_table, relations).get_rows()))

if __name__ == '__main__':
    main()