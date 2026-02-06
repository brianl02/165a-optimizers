"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    # every index is a dictionary that maps a column value to a set of RIDs 
    def __init__(self, table):
        self.indices = [None] *  table.num_columns
        self.table = table
        # Key column should be indexed by default (commonly column 0).
        # If your table defines key column differently, change 0 to that column index.
        self.create_index(0)
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        idx = self.indices[column]
        if idx is not None:
            return list(idx.get(value, []))

        # No index -> full scan
        result = []

        records = getattr(self.table, "records", None)
        if records is None:
            return result  # or raise error if you prefer

        # records as dict: rid -> record
        if isinstance(records, dict):
            items = records.items()
        else:
            # records as list: record objects
            items = enumerate(records)

        for rid_key, record in items:
            rid = getattr(record, "rid", rid_key)

            # --- get row values (edit here if your record stores differently) ---
            if hasattr(record, "columns"):
                row = record.columns
            elif isinstance(record, (list, tuple)):
                row = record
            else:
                row = getattr(record, "values", None)
            # ---------------------------------------------------------------

            if row is None:
                continue

            if row[column] == value:
                result.append(rid)

        return result
    def add_to_index(self, column, value):
        # check if index exists, if not create it
        # given key, get old value
        # update old value by adding new RID
        # put old value back into dictionary
        pass

    def remove_from_index(self, column, value):
        # if index doesn't exist, do nothing
        # get old value with key
        # remove RID from set of RIDs
        # put old value back into dictionary
        pass

    def locate(self, column, value):
        # check if index exists for that column
        # get correct index for that column
        idx = self.indices[column]
        # input value as key for that index 
        rids = idx[value]
        # return list of RIDs that you get
        return list(rids)

          
          if idx is not None:
            return list(idx.get(value, []))
          result = []
          records = getattr(self.table, "records", None)
          if records is None:
            return result
          if isinstance(records, dict):
             items = records.items()
          else:
            items = enumerate(records)
          for rid_key, record in items:
            rid = getattr(record, "rid", rid_key)
            if hasattr(record, "columns"):
               row = record.columns
            elif isinstance(record, (list, tuple)):
               row = record
            else:
               row = getattr(record, "values", None)
            if row is None:
               continue

            if row[column] == value:
               result.append(rid)

          return result



    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        all_rids = ()
        for i in range(begin, end + 1):
            all_rids = all_rids.union(self.locate(column, i))
        return list(all_rids)


        idx = self.indices[column]
        if idx is not None:
            result = []
            for v, rids in idx.items():
                if begin <= v <= end:
                    result.extend(list(rids))
            return result

        # No index -> full scan
        result = []

        records = getattr(self.table, "records", None)
        if records is None:
            return result

        if isinstance(records, dict):
            items = records.items()
        else:
            items = enumerate(records)

        for rid_key, record in items:
            rid = getattr(record, "rid", rid_key)

            # --- get row values (edit here if your record stores differently) ---
            if hasattr(record, "columns"):
                row = record.columns
            elif isinstance(record, (list, tuple)):
                row = record
            else:
                row = getattr(record, "values", None)
            # ---------------------------------------------------------------

            if row is None:
                continue

            v = row[column]
            if begin <= v <= end:
                result.append(rid)

        return result
        # return result
        result = []

        records = getattr(self.table, "records", None)
    if records is None:
        return result

        if isinstance(records, dict):
        items = records.items()
        else:
        items = enumerate(records)

        for rid_key, record in items:
        rid = getattr(record, "rid", rid_key)

        if hasattr(record, "columns"):
            row = record.columns
        elif isinstance(record, (list, tuple)):
                row = record
        else:
                row = getattr(record, "values", None)
        
        if row is None:
            continue

        v = row[column]
        if begin <= v <= end:
            result.append(rid)

    return result


    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        idx = {}

        records = getattr(self.table, "records", None)
        if records is None:
            self.indices[column_number] = idx
            return

        if isinstance(records, dict):
            items = records.items()
        else:
            items = enumerate(records)

        for rid_key, record in items:
            rid = getattr(record, "rid", rid_key)

            # --- get row values (edit here if your record stores differently) ---
            if hasattr(record, "columns"):
                row = record.columns
            elif isinstance(record, (list, tuple)):
                row = record
            else:
                row = getattr(record, "values", None)
            # ---------------------------------------------------------------

            if row is None:
                continue

            v = row[column_number]
            if v not in idx:
                idx[v] = set()
            idx[v].add(rid)

        self.indices[column_number] = idx

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
