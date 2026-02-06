"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        self.indices = [None] *  table.num_columns
        self.table = table
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def add_to_index():
        pass

    def remove_from_index():
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
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
