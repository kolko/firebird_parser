# -*- coding: utf-8 -*-
from db_file import DBReader


def read_pages_table(db_reader):
    pages_address = db_reader.db_header.hdr_PAGES
    print('RDB$PAGES address: {}'.format(pages_address))
    pages_table_page_list = []
    pages_table_page = db_reader.read_page(pages_address)
    pages_table_page_list.append(pages_table_page)
    while pages_table_page.ppg_next != 0:
        # have more pages
        pages_table_page = db_reader.read_page(pages_table_page.ppg_next)
        pages_table_page_list.append(pages_table_page)
    print('First page of RDB$PAGES: {}'.format(pages_table_page_list))

    for pages_table_page in pages_table_page_list:
        for page in pages_table_page.ppg_page:
            if page['address'] == 0:
                # Free page
                continue
            data_page = db_reader.read_page(page['address'])
            assert data_page.header.pag_type == 5, 'RDB$PAGES must links to Data Pages'


def main():
    db_reader = DBReader('test_db.gdb')
    read_pages_table(db_reader)
    # print(db_reader.read_page(819))


if __name__ == '__main__':
    main()