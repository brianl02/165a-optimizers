from lstore.index import Index
from time import time

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
# TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 2

PAGE_RANGE_SIZE = 64000

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
        self.page_range_directory[page_range_number] = PageRange(page_range_number, self)

    def __merge(self):
        print("merge is happening")
        pass


class PageRange:

    def __init__(self, page_range_number, table):
        self.table = table
        self.start_key = page_range_number * PAGE_RANGE_SIZE
        self.end_key = (page_range_number + 1) * PAGE_RANGE_SIZE 
        self.base_records = []
        self.tail_records = []
        self.base_records_count = 0
        self.tail_records_count = 0
        self.base_pages = [[] for _ in range(table.num_columns + 3)]
        self.tail_pages = [[] for _ in range(table.num_columns + 3)]
        self.base_page_count = 0
        self.tail_page_count = 0

    def add_record(self, record, is_base):
        if is_base:
            self.base_records.append(record)
            self.base_records_count += 1
        else:
            self.tail_records.append(record)
            self.tail_records_count += 1

    def add_page(self, is_base, column_index, page):
        if is_base:
            self.base_pages[column_index].append(page)
            self.base_page_count += 1
        else:
            self.tail_pages[column_index].append(page)
            self.tail_page_count += 1

    


