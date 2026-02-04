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
        # call update with all columns set to None to insert tail record of all nulls
        # remove primary key from index, and any mapping from the old column values to RID in other indices
        # remove RID of base record from page directory
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        # check index to see if key already taken
        # calculate range number using primary key
        # check if range exists, if not create it
        # create new record object with new RID
        # construct variable that holds all columns including metadata
        # call add record in table class
        pass

    
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
