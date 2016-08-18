# -*- coding: utf-8 -*-
from db_file import DBReader


def read_pages_table(db_reader):
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
                page_number = pages_row.maybe_data[1]
                print(page_number)
                _page = db_reader.read_page(page_number)
                print(_page)


def main():
    db_reader = DBReader('test_db.gdb')
    read_pages_table(db_reader)


if __name__ == '__main__':
    main()