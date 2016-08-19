# -*- coding: utf-8 -*-
import struct
from db_file import DBReader


def read_pages_table(db_reader):
    class PagesTableRow(object):
        def __init__(self, p_page_number, p_relation_id, p_page_seq, p_page_type):
            self.p_page_number = p_page_number
            self.p_relation_id = p_relation_id
            self.p_page_seq = p_page_seq
            self.p_page_type = p_page_type
    result = []
    pages_address = db_reader.db_header.hdr_PAGES
    pages_table_page_list = []
    pages_table_page = db_reader.read_page(pages_address)
    pages_table_page_list.append(pages_table_page)
    while pages_table_page.ppg_next != 0:
        # have more pages
        pages_table_page = db_reader.read_page(pages_table_page.ppg_next)
        pages_table_page_list.append(pages_table_page)

    for pages_table_page in pages_table_page_list:
        for page in pages_table_page.ppg_page:
            if page['address'] == 0:
                # Free page
                continue
            data_page = db_reader.read_page(page['address'])
            assert data_page.header.pag_type == 5, 'RDB$PAGES must links to Data Pages'
            # Check bit parser in bitmap
            if data_page.header.pag_flags & 0b10:
                assert page['full'] == 1
            else:
                assert page['full'] == 0
            if data_page.header.pag_flags & 0b100:
                assert page['has_large_obj'] == 1
            else:
                assert page['has_large_obj'] == 0

            for pages_row in data_page.dpg_rpt:
                # RDB$PAGE_NUMBER RDB$RELATION_ID RDB$PAGE_SEQUENCE RDB$PAGE_TYPE
                data = pages_row.data_uncompressed
                # print(data)
                p_unknown, p_page_number, p_relation_id, p_page_seq, p_page_type= struct.unpack_from('<IIIIH', data, offset=0)
                # print(p_page_number, p_relation_id, p_page_seq, p_page_type)
                assert p_unknown == 0xf0, 'If assert, then here is sompthing interesting...'
                result.append(PagesTableRow(p_page_number, p_relation_id, p_page_seq, p_page_type))
            # exit(0)
    return result

def test_read_pages_table(db_reader):
    pages_table = read_pages_table(db_reader)
    for page_row in pages_table:
        assert db_reader.read_page(page_row.p_page_number).header.pag_type == page_row.p_page_type

def main():
    db_reader = DBReader('test_db.gdb')
    test_read_pages_table(db_reader)
    pages_table = read_pages_table(db_reader)
    print([x.__dict__ for x in pages_table])


if __name__ == '__main__':
    main()