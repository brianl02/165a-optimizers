from lstore import page
from lstore.index import Index
from time import time
from dataclasses import dataclass
from typing import List

from lstore.page import Page

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
# TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 2

NUM_RECORDS_PER_RANGE = 1024

@dataclass
class PageCoord:
    page_number: int
    offset: int

@dataclass
class PageDirectoryEntry:
    page_range_number: int
    is_base: bool
    data_locations: List[PageCoord] 

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.page_range_directory = {}

    def add_page_range(self, page_range_number):
        if page_range_number in self.page_range_directory:
            return False
        self.page_range_directory[page_range_number] = PageRange(self, page_range_number)
        return True
    
    def add_record(self, page_range_number, is_base, *all_columns, record):
        page_range = self.page_range_directory[page_range_number]
        last_record = page_range.get_last_record(is_base)
        last_record_info = None
        last_record_data_locations = None

        if last_record is None:
            last_record_info = PageDirectoryEntry(page_range_number, is_base, [PageCoord(0, 0) for _ in range(self.num_columns + 3)])
        else:
            last_record_rid = last_record.rid
            last_record_info = self.page_directory[last_record_rid]

        last_record_data_locations = last_record_info.data_locations
        
        if is_base:
            pages = page_range.base_pages
        else:
            pages = page_range.tail_pages

        new_record_data_locations = []

        for column_number in range(self.num_columns + 3):

            if all_columns[column_number] is None:
                new_record_data_locations[column_number] = None
                continue

            last_page = pages[column_number][-1]
            last_record_page_number = pages[column_number].__len__() - 1
            last_record_offset = pages[column_number][-1].current_offset

            if last_page.has_capacity():
                new_page_coord = PageCoord(last_record_page_number, last_record_offset + page.COLUMN_ENTRY_SIZE)

            else:
                page_range.add_page(is_base, column_number)
                new_page_number = last_record_page_number + 1
                new_page_coord = PageCoord(new_page_number, 0)

            new_record_data_locations[column_number] = new_page_coord
            page_to_write = pages[column_number][new_page_coord.page_number]
            page_to_write.write(all_columns[column_number])

        new_page_directory_entry = PageDirectoryEntry(page_range_number, is_base, new_record_data_locations)
        self.page_directory[record.rid] = new_page_directory_entry
        page_range.add_record(is_base, record)
        # TODO update index, waiting for code from index.py
        return
    
    def get_all_record_versions(self):
        # make list of PageDirectoryEntries
        # add PDE for base record to list
        # extract RID of base record
        # extract RID of most recent tail record from indirection pointer of base record
        # while loop that loops till RID of record you are examining is base record
            # get PDE of current record
            # add PDE to list
            # extract RID of next tail record from indirection pointer of current record
        # return list of PDEs
        pass

    def get_relative_version(self):
        pass

    def __merge(self):
        print("merge is happening")
        pass

class PageRange:

    def __init__(self, table, page_range_number):
        self.table = table
        self.page_range_number = page_range_number
        self.base_pages = [[Page()] for _ in range(table.num_columns + 3)]
        self.tail_pages = [[Page()] for _ in range(table.num_columns + 3)]
        self.base_records = []
        self.tail_records = []
        self.num_records = 0

    def get_last_record(self, is_base):
        if is_base:
            if len(self.base_records) == 0:
                return None
            return self.base_records[-1]
        else:
            if len(self.tail_records) == 0:
                return None
            return self.tail_records[-1]
        
    def add_record(self, is_base, record):
        if is_base:
            self.base_records.append(record)
            self.num_records += 1
        else:
            self.tail_records.append(record)

    def add_page(self, is_base, column_number):
        if is_base:
            self.base_pages[column_number].append(Page())
        else:
            self.tail_pages[column_number].append(Page())

 
