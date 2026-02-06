from lstore import table
from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # use index to get RID of base record
        
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids: 
                return False
                base_rid = rids[0]
        # call update with all columns set to None to insert tail record of all nulls
        # remove primary key from index, and any mapping from the old column values to RID in other indices
        # remove RID of base record from page directory

        entry = self.table.page_directory[base_rid] # type: ignore
        pr = self.table.page.range_directory[entry.page_range_number]
        if base_rid in page.range.base_records:
            del page.range.base_records[base_rid]

        if base_rid in self.table.page_directory:
            del self.table.page_directory[base_rid]

            return True
        

    
      return False


        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
    
        # check if col if is correct number
        if len(column) != self.table.num_columns:
            return False

        # use the key to find a pageRange
        primary_key = column[self.table.key]
        # calculate range number using primary key
        page_range_number = primary_key // PAGE_RANGE_SIZE

        # if there isn't page number then we build a new page
        if page_range_number not in self.table.page_range_directory:
            self.table.add_page_range(page_range_number)

        page_range = self.table.page_range_directory[page_range_number]

        # allocate  RID
        if len(self.table.page_directory)  == 0:
            rid = 0
        else:
            rid = max(self.table.page_directory.keys()) +1

        
        # schema for base record
        schema_encoding = '0' * self.table.num_columns
                    
        # create new record object with new RID
        # call add record in table class
        record = Record(rid, primary_key, list(columns))
        page_range.add_record(record, is_base = True)

        # construct variable that holds all columns including metadata
        # track locations
        total_cols = self.table.num_columns + 3
        data_locations = [None] * total_cols

        # hidding col : RID（record ID, '0' base record）
        # /Indirection(point to tail record, '1' tail record)
        #/Schema(which col have been updated?)
        data_locations[RID_COLUMN] = self._append_value(page_range, RID_COLUMN,rid,is_base=True)
        data_locations[INDIRECTION_COLUMN] = self._append_value(page_range,INDIRECTION_COLUMN, 0, is_base = True)
        data_locations[SCHEMA_ENCODING_COLUMN] = self._append_value(page_range,SCHEMA_ENCODING_COLUMN,schema_encoding, is_base = True)

        # starting from index 3
        # 0 - RID
        # 1 - Indirection point to tail record
        # 2 - Schema encoding (check which one have been updated)
        # 3,4,5 - User data
        for i, val in enumerate(columns):
            base_col_idx = i + 3
            data_locations[base_col_idx] = self._append_value(page_range, base_col_idx,val, is_base = True)

        # updating page directory
        self.table.page_directory[rid] = PageDirectoryEntry(
            page_range_number = page_range_number,
            data_locations = data_locations
        )

        return True

    
    """
    # Read matching record with specified search key

    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        rid_list = self.table.index.locate(search_key_index, search_key)
        record_list = []
        for rid in rid_list:
            columns = self.table.construct_full_record(rid)
            primary_key = self.table.get_primary_key(rid)
            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    continue
                new_columns.append(columns[i])
            record_list.append(Record(rid, primary_key, new_columns))
        return record_list
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rid_list = self.table.index.locate(search_key_index, search_key)
        record_list = []
        for rid in rid_list:
            columns = self.table.construct_full_record(rid, relative_version * -1)
            primary_key = self.table.get_primary_key(rid)
            new_columns = []
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    continue
                new_columns.append(columns[i])
            record_list.append(Record(rid, primary_key, new_columns))
        return record_list
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):

    
        rids = self.table.index.locate(self.table.key,primary_key)
        if not rids:
            return False
        base_rid = rids[0]


        base_entry = self.table.page_directory[base-rid]
        pr_num = base_entry.page_range_number

    #schema encoding

schema = ""
for v in columns:
        if v is None:
            schema += "0"
        else:
            schema += "1"

            #new tail rid 
            new_tail_rid = max(self.table.page_directory.keys()) + 1

            values = [new_tail_rid, base_rid, schema]
            for v in columns:
                values.append(v)

        rec = record(new_tail_rid, primary_key, list(columns))
        self.table.add_record(pr_num, False, *values, record=rec)

        indir_loc = base_entry.data_location[1]
        base_indir_page +self.table.page_range_directory[pr_num].base_pages[1][indir_loc.page_number]
        base_indir_page.write(new_tail_rid, indir_loc.offset)


        #


        # IMPORTANT: must check if columns are all set to null, if so then you are doing a delete operation and SE should be all 0's


        # use index to get RID of base record
        # use page directory to get data locations of base record
        # create new tail record object
        # construct variable that holds all columns including metadata 
        # update indirection pointer and schema encoding of base record 
        # call add record in table class 
        # (note: if record is being updated for the first time, must add copy of base record as tail record)
        pass
        
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        has_records = False
        for key in range(start_range, end_range + 1):
            rid = self.table.index.locate(0, key)
            if rid is None:
                continue
            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index)
            if column_value is None:
                column_value = 0
            sum += column_value
        if not has_records:
            return False
        return sum
    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        sum = 0
        has_records = False
        for key in range(start_range, end_range + 1):
            rid = self.table.index.locate(0, key)
            if rid is None:
                continue
            has_records = True
            column_value = self.table.get_column_value(rid, aggregate_column_index, relative_version * -1)
            if column_value is None:
                column_value = 0
            sum += column_value
        if not has_records:
            return False
        return sum

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
=