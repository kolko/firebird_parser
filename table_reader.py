# -*- coding: utf-8 -*-
import struct
from collections import namedtuple


class TableReader(object):
    '''Abstract class for talbe read
    Need to define:
        TABLE_NAME
        or RELATION_ID if table RDB$RELATION not red yet
        or PAGE_NUMBER to read RDB$PAGES
    '''
    TABLE_NAME = None
    RELATION_ID = None
    PAGE_NUMBER = None

    def __init__(self, db_reader, pages_table=None, relations=None):
        assert self.TABLE_NAME or self.RELATION_ID or self.PAGE_NUMBER
        self.db_reader = db_reader
        self.pages_table = pages_table
        self.relations = relations
        if self.TABLE_NAME:
            assert pages_table, 'Forget pages_table'
            assert relations, 'Forget relations'
        if self.RELATION_ID:
            assert pages_table, 'Forget pages_table'

    def _find_relation(self):
        relation_list = list(filter(lambda x: x.p_relation_name == self.TABLE_NAME, self.relations))
        assert len(relation_list) == 1
        return relation_list[0]

    def _find_pointer_page(self, relation_id):
        pages = list(filter(lambda x: x.p_relation_id == relation_id, self.pages_table))
        pages_pointer = list(filter(lambda x: x.p_page_type == 4, pages))
        assert len(list(pages_pointer)) == 1
        return pages_pointer[0]

    def get_rows(self):
        if self.TABLE_NAME:
            relation = self._find_relation()
            pointer_page = self._find_pointer_page(relation.p_relation_id)
            ppage = self.db_reader.read_page(pointer_page.p_page_number)

        if self.RELATION_ID:
            pointer_page = self._find_pointer_page(self.RELATION_ID)
            ppage = self.db_reader.read_page(pointer_page.p_page_number)

        if self.PAGE_NUMBER:
            ppage = self.db_reader.read_page(self.PAGE_NUMBER)

        page_list = [ppage]
        while ppage.ppg_next != 0:
            ppage = self.db_reader.read_page(ppage.ppg_next)
            page_list.append(ppage)

        for ppage in page_list:
            for page in ppage.ppg_page:
                if page['address'] == 0:
                    # Free page
                    continue
                data_page = self.db_reader.read_page(page['address'])
                assert data_page.header.pag_type == 5, 'must links to Data Pages'

                # Check bit parser in bitmap
                # TODO DEBUG
                # if data_page.header.pag_flags & 0b10:
                #     assert page['full'] == 1
                # else:
                #     assert page['full'] == 0
                if data_page.header.pag_flags & 0b100:
                    assert page['has_large_obj'] == 1
                else:
                    assert page['has_large_obj'] == 0

                for pages_row in data_page.dpg_rpt:
                    if pages_row.rhd_blob or \
                        pages_row.rhd_stream_blob_or_rhd_delta:
                            continue

                    if pages_row.rhd_incomplete:
                        continue

                    data = pages_row.data_uncompressed
                    if pages_row.rhd_fragment:
                        continue

                    # while pages_row.rhd_fragment:
                    #     _p = self.db_reader.read_page(pages_row.rhdf_f_page)
                    #     print(pages_row.rhdf_f_page, pages_row.rhdf_f_line)
                    #     pages_row = _p.dpg_rpt[pages_row.rhdf_f_line]
                    #     print(data)
                    #     data += pages_row.data_uncompressed
                    #     print(data)
                    #     print()
                    #     # exit(0)

                    # TODO: know why data is empty
                    if not data:
                        continue
                    # HACK, becose sometimes we cant parse row and return Null
                    row = self.parse_row_data(data)
                    if row:
                        yield row

    def parse_row_data(self, data):
        raise NotImplementedError()


class Table_RdbPages(TableReader):
    '''List all pages in database file. page_number of first page can get from db_header'''
    def __init__(self, db_reader, **kwargs):
        self.PAGE_NUMBER = db_reader.db_header.hdr_PAGES
        super().__init__(db_reader, **kwargs)

    def parse_row_data(self, data):
        p_unknown, p_page_number, p_relation_id, p_page_seq, p_page_type = struct.unpack_from('<IIIIH', data, offset=0)
        assert p_unknown == 0xf0, 'If assert, then here is sompthing interesting...'

        # Test
        # self.db_reader.read_page(p_page_number).header.pag_type == p_page_type

        PagesTableRow = namedtuple('RDB_PAGES', 'p_page_number, p_relation_id, p_page_seq, p_page_type')
        return PagesTableRow(p_page_number, p_relation_id, p_page_seq, p_page_type)


class Table_Rdb_Database(TableReader):
    '''Main information about database. First, what we get - codepage'''
    RELATION_ID = 1

    def parse_row_data(self, data):
        # TODO parse blob with description
        p_relation_id, = struct.unpack_from('<H', data, offset=12)
        p_security_class = ''.join(x.decode('utf8') for x in struct.unpack_from('<'+('c'*31), data, offset=14)).rstrip()
        p_character_set_name = ''.join(x.decode('utf8') for x in struct.unpack_from('<'+('c'*31), data, offset=45)).rstrip()

        DatabaseTableRow = namedtuple('RDB_PAGES', 'p_relation_id, p_security_class, p_character_set_name')
        return DatabaseTableRow(p_relation_id, p_security_class, p_character_set_name)


class Table_Rdb_Relations(TableReader):
    '''Help find relation_id by table_name'''
    RELATION_ID = 6

    def parse_row_data(self, data):
        p_relation_id, p_system_flag, p_dbkey_length, p_format, p_field_id = struct.unpack_from('<HHHHH', data, offset=28)
        p_relation_name = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=38)).rstrip()
        p_security_class = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=69)).rstrip()

        # TODO: other fields
        RelationsTableRow = namedtuple('PDB_RELATIONS', 'p_relation_id, p_system_flag, p_dbkey_length, p_format, p_field_id, p_relation_name, p_security_class')
        return RelationsTableRow(p_relation_id, p_system_flag, p_dbkey_length, p_format, p_field_id, p_relation_name, p_security_class)


class Table_Rdb_Formats(TableReader):
    TABLE_NAME = 'RDB$FORMATS'

    def parse_row_data(self, data):
        p_relation_id, p_format = struct.unpack_from('<HH', data, offset=5)
        # TODO parse blob
        blob = data[9:]
        FormatsTableRow = namedtuple('RDB_FORMATS', 'p_relation_id, p_format, p_descriptor')
        return FormatsTableRow(p_relation_id, p_format, blob)


class Table_Rdb_Fields(TableReader):
    TABLE_NAME = 'RDB$FIELDS'

    def parse_row_data(self, data):
        # TODO: parse all
        p_field_name = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=4)).rstrip()
        p_query_name = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=35)).rstrip()
        FieldsTableRow = namedtuple('RDB_FIELDS', 'p_field_name, p_query_name')
        return FieldsTableRow(p_field_name, p_query_name)


class Table_Rdb_Relation_Fields(TableReader):
    '''Table with list columns of each table. Required for parsing rows, i think'''
    TABLE_NAME = 'RDB$RELATION_FIELDS'

    # RDB$FIELD_NAME                  (RDB$FIELD_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$RELATION_NAME               (RDB$RELATION_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$FIELD_SOURCE                (RDB$FIELD_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$QUERY_NAME                  (RDB$FIELD_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$BASE_FIELD                  (RDB$FIELD_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$EDIT_STRING                 (RDB$EDIT_STRING) VARCHAR(125) CHARACTER SET NONE Nullable
    # RDB$FIELD_POSITION              (RDB$FIELD_POSITION) SMALLINT Nullable
    # RDB$QUERY_HEADER                (RDB$QUERY_HEADER) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$UPDATE_FLAG                 (RDB$SYSTEM_FLAG) SMALLINT Nullable
    # RDB$FIELD_ID                    (RDB$FIELD_ID) SMALLINT Nullable
    # RDB$VIEW_CONTEXT                (RDB$VIEW_CONTEXT) SMALLINT Nullable
    # RDB$DESCRIPTION                 (RDB$DESCRIPTION) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$DEFAULT_VALUE               (RDB$VALUE) BLOB segment 80, subtype BLR CHARACTER SET NONE Nullable
    # RDB$SYSTEM_FLAG                 (RDB$SYSTEM_FLAG) SMALLINT Nullable
    # RDB$SECURITY_CLASS              (RDB$SECURITY_CLASS) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$COMPLEX_NAME                (RDB$FIELD_NAME) CHAR(31) CHARACTER SET UNICODE_FSS Nullable
    # RDB$NULL_FLAG                   (RDB$NULL_FLAG) SMALLINT Nullable
    # RDB$DEFAULT_SOURCE              (RDB$SOURCE) BLOB segment 80, subtype TEXT CHARACTER SET UNICODE_FSS Nullable
    # RDB$COLLATION_ID                (RDB$COLLATION_ID) SMALLINT Nullable

    def parse_row_data(self, data):
        p_field_name = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=4)).rstrip()
        p_relation_name = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=35)).rstrip()
        p_field_source = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=66)).rstrip()
        p_query_name = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=97)).rstrip()
        p_base_field = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*31), data, offset=128)).rstrip()
        p_edit_string = ''.join(x.decode(self.db_reader.charset) for x in struct.unpack_from('<'+('c'*125), data, offset=159)).rstrip()
        p_field_position, = struct.unpack_from('<h', data, offset=288) # TODO: Why 125 + 159 != 288
        # TODO: parse all fields

        RelationFieldsTableRow = namedtuple('RDB_RELATION_FIELDS', 'p_field_name, p_relation_name, p_field_source, p_query_name, p_base_field, p_edit_string, p_field_position')
        return RelationFieldsTableRow(p_field_name, p_relation_name, p_field_source, p_query_name, p_base_field, p_edit_string, p_field_position)
