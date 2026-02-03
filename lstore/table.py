from lstore.index import Index
from time import time
from dataclasses import dataclass
from typing import List, Tuple

from lstore.page import Page

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
# TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 2

NUM_RECORDS_PER_BASE_TAIL = Page.MAX_RECORDS_PER_PAGE
NUM_BASE_PER_RANGE = 16
NUM_RECORDS_PER_RANGE = NUM_BASE_PER_RANGE * NUM_RECORDS_PER_BASE_TAIL


# @dataclass
# class PageDirectoryEntry:
#     page_range_number: int
#     in_base_page: bool
#     base_tail_page_number: int
#     data_locations: List[Tuple[int, int]] 

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
        self.page_ranges = []
        self.num_page_ranges = 1 
        self.add_page_range()

    def add_page_range(self):
        self.page_ranges.append(PageRange(self, self.num_page_ranges))
        self.num_page_ranges += 1 

    def get_last_page_range(self):
        return self.page_ranges[-1]

    def get_next_record_location(self, is_base): 
        if is_base:
            pass
        else:
            pass

    def __merge(self):
        print("merge is happening")
        pass
 

class PageRange:

    def __init__(self, table, page_range_number):
        self.table = table
        self.page_range_number = page_range_number
        self.base_pages = []
        self.tail_pages = []
        self.num_base_pages = 0
        self.num_tail_pages = 0
        self.add_base_tail_page(True)
        self.add_base_tail_page(False)
    
    def add_base_tail_page(self, is_base):
        if is_base and not self.has_capacity():
            return False 
        if is_base:
            self.base_pages.append(BaseTailPage(True, self.num_base_pages, self, self.table))
            self.num_base_pages += 1
        else:
            self.tail_pages.append(BaseTailPage(False, self.num_tail_pages, self, self.table))
            self.num_tail_pages += 1
        return True
    
    def get_last_base_page(self):
        return self.base_pages[-1]
    
    def has_capacity(self):
        return self.num_base_pages < NUM_BASE_PER_RANGE
        

class BaseTailPage:

    def __init__(self, is_base, page_number, page_range, table):
        self.is_base = is_base
        self.page_number = page_number
        self.page_range = page_range
        self.table = table
        self.pages = [Page() for _ in range(table.num_columns + 3)]
        self.num_records = 0

    def has_capacity(self):
        return self.num_records < NUM_RECORDS_PER_BASE_TAIL



