from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# variable for size of page range that can be changed for optimization


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
        # variable for list of page ranges
        # create first page range
        pass

    def __merge(self):
        print("merge is happening")
        pass

    # class Table should have functions for adding to PageRange, reading PageRange, etc.
 
 # class PageRange (each holds base pages for specific range of rows and their corresponding tail pages)
 
    # init function (accepts number of columns and range of records it holds)
    # set range variable
    # create 2d array for base pages, init with one base page per column
    # create 2d array for tail pages, init empty
    # num records tracker

    # has_capacity function


