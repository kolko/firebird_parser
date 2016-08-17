# -*- coding: utf-8 -*-
import struct
import binascii


class PageHeader(object):
    # struct pag
    # {
    #     SCHAR pag_type;
    #     UCHAR pag_flags;
    #     USHORT pag_checksum; #not used, always 12345
    #     ULONG pag_generation;
    #     ULONG pag_scn;
    #     ULONG reserved;
    # };
    PAG_STRUCT = '<bBHLLL'
    PAG_SIZE = struct.calcsize(PAG_STRUCT)

    def __init__(self, db_file, pagenum=0):
        data = db_file.get_page_data(pagenum)
        self.pag_type, self.pag_flags, _, self.pag_generation, self.pag_scn, _ = \
            struct.unpack_from(self.PAG_STRUCT, data)#, offset=offset)


class Headerpage(object):
    # struct header_page
    # {
    #     pag hdr_header;
    #     USHORT hdr_page_size;
    #     USHORT hdr_ods_version;
    #     SLONG hdr_PAGES;
    #     ULONG hdr_next_page;
    #     SLONG hdr_oldest_transaction;
    #     SLONG hdr_oldest_active;
    #     SLONG hdr_next_transaction;
    #     USHORT hdr_sequence;
    #     USHORT hdr_flags;
    #     SLONG hdr_creation_date[2];
    #     SLONG hdr_attachment_id;
    #     SLONG hdr_shadow_count;
    #     SSHORT hdr_implementation;
    #     USHORT hdr_ods_minor;
    #     USHORT hdr_ods_minor_original;
    #     USHORT hdr_end;
    #     ULONG hdr_page_buffers;
    #     SLONG hdr_bumped_transaction;
    #     SLONG hdr_oldest_snapshot;
    #     SLONG hdr_backup_pages;
    #     SLONG hdr_misc[3];
    #     UCHAR hdr_data[1];
    # };
    HEADER_PAGE_STRUCT = '<HHlLlllHHllllhHHHLllllll'
    HEADER_PAGE_SIZE = struct.calcsize(HEADER_PAGE_STRUCT)

    def __init__(self, db_file, pagenum=0):
        data = db_file.get_page_data(pagenum)
        (self.hdr_page_size, self.hdr_ods_version, self.hdr_PAGES, self.hdr_next_page, self.hdr_oldest_transaction,
            self.hdr_oldest_active, self.hdr_next_transaction, self.hdr_sequence, self.hdr_flags, self.hdr_creation_date1,
            self.hdr_creation_date1, self.hdr_attachment_id, self.hdr_shadow_count, self.hdr_implementation,
            self.hdr_ods_minor, self.hdr_ods_minor_original, self.hdr_end, self.hdr_page_buffers, self.hdr_bumped_transaction,
            self.hdr_oldest_snapshot, self.hdr_backup_pages, self.hdr_misc1, self.hdr_misc2, self.hdr_misc3) = \
                struct.unpack_from(self.HEADER_PAGE_STRUCT, data, offset=PageHeader.PAG_SIZE)
        assert self.hdr_next_page == 0, u'Это multi-file database, со всеми вытекающими доработками...'

        offset = self.HEADER_PAGE_SIZE
        hdr_data = {}
        while True:
            (type,) = struct.unpack_from('B', data, offset=offset)
            offset += 1
            if type == 0x00:
                break
            elif type == 0x01:
                #Original name of the root file for this database
                offset, hdr_data['HDR_root_file_name'] = _parse_header_data(data, offset)
            elif type == 0x02:
                #Name of the journal server
                offset, hdr_data['HDR_journal_server'] = _parse_header_data(data, offset)
            elif type == 0x03:
                #Secondary file name
                offset, hdr_data['HDR_file'] = _parse_header_data(data, offset)
            elif type == 0x04:
                #Last logical page of the current file
                offset, hdr_data['HDR_last_page'] = _parse_header_data(data, offset)
            elif type == 0x05:
                #Count of unlicensed activity. No longer used
                offset, hdr_data['HDR_unlicemsed'] = _parse_header_data(data, offset)
            elif type == 0x06:
                #Number of transactions between sweep
                offset, hdr_data['HDR_sewwp_interval'] = _parse_header_data(data, offset)
            elif type == 0x07:
                #Replay log name
                offset, hdr_data['HDR_log_name'] = _parse_header_data(data, offset)
            elif type == 0x08:
                #Intermediate journal filename
                offset, hdr_data['HDR_journal_file'] = _parse_header_data(data, offset)
            elif type == 0x09:
                #Key to compare with the password database
                offset, hdr_data['HDR_password_file_key'] = _parse_header_data(data, offset)
            elif type == 0x0a:
                #Write Ahead Log (WAL) backup information. No longer used
                offset, hdr_data['HDR_backup_info'] = _parse_header_data(data, offset)
            elif type == 0x0b:
                #Shared cache file. No longer used
                offset, hdr_data['HDR_cache_file'] = _parse_header_data(data, offset)
            elif type == 0x0c:
                #Diff file used during the times when the database is in backup mode
                offset, hdr_data['HDR_difference_file'] = _parse_header_data(data, offset)
            elif type == 0x0d:
                #UID generated when database is in backup mode. Overwritten on subsequent backups.
                offset, hdr_data['HDR_backup_guid'] = _parse_header_data(data, offset)
        self.hdr_data = hdr_data

def _parse_header_data(data, offset):
    (size,) = struct.unpack_from('B', data, offset=offset)
    offset += 1
    res = data[offset:offset+size]
    offset += size
    return offset, res


class PointerPage(object):
    # struct pointer_page
    # {
    #     pag ppg_header;
    #     SLONG ppg_sequence;
    #     SLONG ppg_next;
    #     USHORT ppg_count;
    #     USHORT ppg_relation;
    #     USHORT ppg_min_space;
    #     USHORT ppg_max_space; # not used
    #     SLONG ppg_page[1];
    # };
    #It is not a fill level. It have two bits per data page slot and one bit means
    # data page is full while second bit means that data page occupied by large
    # object (blob or huge record fragment). This bits is set or cleared when
    # dpg_full or dpg_large bits of corresponding data page is changed. See also
    # DPM\mark_full() function.
    # https://sourceforge.net/p/firebird/mailman/message/24080463/
    PP_PAGE_STRUCT = '<llHHHH'
    PP_PAGE_SIZE = struct.calcsize(PP_PAGE_STRUCT)

    def __init__(self, db_file, pagenum=0):
        data = db_file.get_page_data(pagenum)
        self.ppg_sequence, self.ppg_next, self.ppg_count, self.ppg_relation, self.ppg_min_space, _ = \
            struct.unpack_from(self.PP_PAGE_STRUCT, data, offset=PageHeader.PAG_SIZE)

        self.ppg_page = []
        # Count of ppg_page = free_space on page (in bits (*8)) devide by len of LONG (in bits) plus 2 bits of bitmap
        dbb_dp_per_pp = (db_file.pagesize - (self.PP_PAGE_SIZE + PageHeader.PAG_SIZE)) * 8 / (32 + 2)
        dbb_dp_per_pp = int(dbb_dp_per_pp)

        start_pos_of_fill_bitmap = self.PP_PAGE_SIZE + PageHeader.PAG_SIZE + dbb_dp_per_pp * 4
        if db_file.pagesize == 4096:
            assert start_pos_of_fill_bitmap == 0x0f10

        hex_string_bitmap = ''.join("{:08b}".format(f) for f in struct.unpack_from('b'*dbb_dp_per_pp, data, offset=start_pos_of_fill_bitmap))

        # Read from end of PP_PAGE_STRUCT to end of page minus "Page fill bitmaps"
        n = 0
        for offset in range(self.PP_PAGE_SIZE + PageHeader.PAG_SIZE, db_file.pagesize, 4):
            if offset >= start_pos_of_fill_bitmap:
                break
            ppg_page = {'address': struct.unpack_from('h', data, offset=offset)[0]}
            _bits = hex_string_bitmap[n*2:][:2]
            n += 1
            assert _bits[0] in '01'
            assert _bits[1] in '01'
            ppg_page['full'] = int(_bits[1])
            ppg_page['has_large_obj'] = int(_bits[0])

            self.ppg_page.append(ppg_page)

        assert len(self.ppg_page) == dbb_dp_per_pp
        # Count - count of used pages
        assert self.ppg_count == len([p for p in self.ppg_page if p['address'] != 0])
        # Count - address of first not full page
        assert self.ppg_page[self.ppg_min_space]['full'] == 0
        for p in self.ppg_page[:self.ppg_min_space]:
            assert p['full'] == 1


class DataPage(object):
    #     struct data_page
    # {
    #     pag dpg_header;
    #     SLONG dpg_sequence;
    #     USHORT dpg_relation;
    #     USHORT dpg_count;
    #     struct dpg_repeat {
    #         USHORT dpg_offset;
    #         USHORT dpg_length;
    #     } dpg_rpt[1];
    # }
    PP_PAGE_STRUCT = '<llHHHH'
    PP_PAGE_SIZE = struct.calcsize(PP_PAGE_STRUCT)

    def __init__(self, db_file, pagenum=0):
        return
        data = db_file.get_page_data(pagenum)
        self.ppg_sequence, self.ppg_next, self.ppg_count, self.ppg_relation, self.ppg_min_space, _ = \
            struct.unpack_from(self.PP_PAGE_STRUCT, data, offset=PageHeader.PAG_SIZE)