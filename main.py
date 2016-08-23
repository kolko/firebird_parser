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
    class RelationsTableRow(object):
        def __init__(self, p_relation_id, p_system_flag, p_dbkey_length, p_format, p_field_id,
                     p_relation_name, p_security_class):
            self.p_relation_id = p_relation_id
            self.p_system_flag = p_system_flag
            self.p_dbkey_length = p_dbkey_length
            self.p_format = p_format
            self.p_field_id = p_field_id
            self.p_relation_name = p_relation_name
            self.p_security_class = p_security_class
    result = []
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
            data = pages_row.data_uncompressed
            #TODO: remove hack
            if len(data) < 100:
                continue
            p_relation_id, p_system_flag, p_dbkey_length, p_format, p_field_id = struct.unpack_from('<HHHHH', data, offset=28)
            p_relation_name = ''.join(x.decode(db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=38)).strip()
            p_security_class = ''.join(x.decode(db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=69)).strip()

            # TODO: other fields
            result.append(RelationsTableRow(p_relation_id, p_system_flag, p_dbkey_length, p_format, p_field_id, p_relation_name, p_security_class))
    return result

def get_database_table_relid_1(db_reader, pages_table):
    # RDB$DESCRIPTION                 (RDB$DESCRIPTION) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$RELATION_ID                 (RDB$RELATION_ID) SMALLINT Nullable
    # RDB$SECURITY_CLASS              (RDB$SECURITY_CLASS) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$CHARACTER_SET_NAME          (RDB$CHARACTER_SET_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    pages = list(filter(lambda x: x.p_relation_id == 1, pages_table))
    pages_pointer = list(filter(lambda x: x.p_page_type == 4, pages))
    assert len(list(pages_pointer)) == 1
    pointer_page = db_reader.read_page(pages_pointer[0].p_page_number)
    for page in pointer_page.ppg_page:
        if page['address'] == 0:
            # Free page
            continue
        data_page = db_reader.read_page(page['address'])
        assert data_page.header.pag_type == 5, '???? must links to Data Pages'
        for pages_row in data_page.dpg_rpt:
            data = pages_row.data_uncompressed
            # TODO parse blob with description
            p_relation_id, = struct.unpack_from('<H', data, offset=12)
            p_security_class = ''.join(x.decode('utf8') for x in struct.unpack_from('<'+('c'*31), data, offset=14)).strip()
            p_character_set_name = ''.join(x.decode('utf8') for x in struct.unpack_from('<'+('c'*31), data, offset=45)).strip()

            if p_character_set_name.lower() == 'win1251':
                p_character_set_name = 'cp1251'
            return p_character_set_name.lower()


def formats_table(db_reader, pages_table, relations):
    # RDB$RELATION_ID                 (RDB$RELATION_ID) SMALLINT Nullable
    # RDB$FORMAT                      (RDB$FORMAT) SMALLINT Nullable
    # RDB$DESCRIPTOR                  (RDB$DESCRIPTOR) BLOB segment 80, subtype FORMAT CHARACTER SET NONE Nullable
    relation = list(filter(lambda x: x.p_relation_name == 'RDB$FORMATS', relations))
    assert len(relation) == 1
    pages = list(filter(lambda x: x.p_relation_id == relation[0].p_relation_id, pages_table))
    pages_pointer = list(filter(lambda x: x.p_page_type == 4, pages))
    assert len(list(pages_pointer)) == 1
    pointer_page = db_reader.read_page(pages_pointer[0].p_page_number)
    for page in pointer_page.ppg_page:
        if page['address'] == 0:
            # Free page
            continue
        data_page = db_reader.read_page(page['address'])
        assert data_page.header.pag_type == 5, '???? must links to Data Pages'
        for pages_row in data_page.dpg_rpt:
            data = pages_row.data_uncompressed
            # TODO: remove hack
            if not data:
                continue
            p_relation_id, p_format = struct.unpack_from('<HH', data, offset=5)
            print(p_relation_id, p_format)
            print(data[9:])


def fields_table(db_reader, pages_table, relations):
    # RDB$FIELD_NAME                  (RDB$FIELD_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$QUERY_NAME                  (RDB$FIELD_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$VALIDATION_BLR              (RDB$VALIDATION_BLR) BLOB segment 80, subtype BLR CHARACTER SET NONE Nullable
    # RDB$VALIDATION_SOURCE           (RDB$SOURCE) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$COMPUTED_BLR                (RDB$VALUE) BLOB segment 80, subtype BLR CHARACTER SET NONE Nullable
    # RDB$COMPUTED_SOURCE             (RDB$SOURCE) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$DEFAULT_VALUE               (RDB$VALUE) BLOB segment 80, subtype BLR CHARACTER SET NONE Nullable
    # RDB$DEFAULT_SOURCE              (RDB$SOURCE) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$FIELD_LENGTH                (RDB$FIELD_LENGTH) SMALLINT Nullable
    # RDB$FIELD_SCALE                 (RDB$FIELD_SCALE) SMALLINT Nullable
    # RDB$FIELD_TYPE                  (RDB$FIELD_TYPE) SMALLINT Nullable
    # RDB$FIELD_SUB_TYPE              (RDB$FIELD_SUB_TYPE) SMALLINT Nullable
    # RDB$MISSING_VALUE               (RDB$VALUE) BLOB segment 80, subtype BLR CHARACTER SET NONE Nullable
    # RDB$MISSING_SOURCE              (RDB$SOURCE) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$DESCRIPTION                 (RDB$DESCRIPTION) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$SYSTEM_FLAG                 (RDB$SYSTEM_FLAG) SMALLINT Nullable
    # RDB$QUERY_HEADER                (RDB$QUERY_HEADER) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$SEGMENT_LENGTH              (RDB$SEGMENT_LENGTH) SMALLINT Nullable
    # RDB$EDIT_STRING                 (RDB$EDIT_STRING) VARCHAR(125) CHARACTER SET NONE Nullable
    # RDB$EXTERNAL_LENGTH             (RDB$FIELD_LENGTH) SMALLINT Nullable
    # RDB$EXTERNAL_SCALE              (RDB$FIELD_SCALE) SMALLINT Nullable
    # RDB$EXTERNAL_TYPE               (RDB$FIELD_TYPE) SMALLINT Nullable
    # RDB$DIMENSIONS                  (RDB$DIMENSIONS) SMALLINT Nullable
    # RDB$NULL_FLAG                   (RDB$NULL_FLAG) SMALLINT Nullable
    # RDB$CHARACTER_LENGTH            (RDB$FIELD_LENGTH) SMALLINT Nullable
    # RDB$COLLATION_ID                (RDB$COLLATION_ID) SMALLINT Nullable
    # RDB$CHARACTER_SET_ID            (RDB$CHARACTER_SET_ID) SMALLINT Nullable
    # RDB$FIELD_PRECISION             (RDB$FIELD_PRECISION) SMALLINT Nullable
    relation = list(filter(lambda x: x.p_relation_name == 'RDB$FIELDS', relations))
    assert len(relation) == 1
    pages = list(filter(lambda x: x.p_relation_id == relation[0].p_relation_id, pages_table))
    pages_pointer = list(filter(lambda x: x.p_page_type == 4, pages))
    assert len(list(pages_pointer)) == 1
    pointer_page = db_reader.read_page(pages_pointer[0].p_page_number)
    for page in pointer_page.ppg_page:
        if page['address'] == 0:
            # Free page
            continue
        data_page = db_reader.read_page(page['address'])
        assert data_page.header.pag_type == 5, '???? must links to Data Pages'
        for pages_row in data_page.dpg_rpt:
            data = pages_row.data_uncompressed
            # TODO: remove hack
            if not data:
                continue
            # TODO: parse all
            p_field_name = ''.join(x.decode(db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=4)).strip()
            p_query_name = ''.join(x.decode(db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=35)).strip()
            print(p_field_name, p_query_name)
            # print(data)


def main():
    db_reader = DBReader('test_db.gdb')
    test_read_pages_table(db_reader)
    pages_table = read_pages_table(db_reader)
    db_charset = get_database_table_relid_1(db_reader, pages_table)
    db_reader.charset = db_charset
    relations = get_relationid_6(db_reader, pages_table)
    # formats_table(db_reader, pages_table, relations)
    fields_table(db_reader, pages_table, relations)
    # print(relations)


if __name__ == '__main__':
    main()