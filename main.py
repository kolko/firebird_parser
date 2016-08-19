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

def get_relationid_6(db_reader, pages_table):
    #RDB$RELATIONS
    pages = list(filter(lambda x: x.p_relation_id == 6, pages_table))
    pages_pointer = list(filter(lambda x: x.p_page_type == 4, pages))
    assert len(list(pages_pointer)) == 1
    pointer_page = db_reader.read_page(pages_pointer[0].p_page_number)
    for page in pointer_page.ppg_page:
        if page['address'] == 0:
            # Free page
            continue
        data_page = db_reader.read_page(page['address'])
        assert data_page.header.pag_type == 5, '???? must links to Data Pages'
        # Check bit parser in bitmap
        # TODO: FIX
        # if data_page.header.pag_flags & 0b10:
        #     assert page['full'] == 1
        # else:
        #     assert page['full'] == 0
        # if data_page.header.pag_flags & 0b100:
        #     assert page['has_large_obj'] == 1
        # else:
        #     assert page['has_large_obj'] == 0
        for pages_row in data_page.dpg_rpt:
            # RDB$VIEW_BLR                    <null>
            # RDB$VIEW_SOURCE                 <null>
            # RDB$DESCRIPTION                 <null>
            # RDB$RELATION_ID                 6
            # RDB$SYSTEM_FLAG                 1
            # RDB$DBKEY_LENGTH                8
            # RDB$FORMAT                      0
            # RDB$FIELD_ID                    17
            # RDB$RELATION_NAME               RDB$RELATIONS
            # RDB$SECURITY_CLASS              <null>
            # RDB$EXTERNAL_FILE               <null>
            # RDB$RUNTIME                     6:32
            # BLOB display set to subtype 1. This BLOB: subtype = 5
            # RDB$EXTERNAL_DESCRIPTION        <null>
            # RDB$OWNER_NAME                  SYSDBA
            # RDB$DEFAULT_CLASS               <null>
            # RDB$FLAGS                       <null>
            # RDB$RELATION_TYPE               0

            data = pages_row.data_uncompressed
            print(data)
def main():
    db_reader = DBReader('test_db.gdb')
    test_read_pages_table(db_reader)
    pages_table = read_pages_table(db_reader)
    get_relationid_6(db_reader, pages_table)


# const int REQ_relation_id		= 2;

if __name__ == '__main__':
    main()