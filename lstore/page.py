PAGE_SIZE = 4096
COLUMN_ENTRY_SIZE = 8

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        max_records = PAGE_SIZE // COLUMN_ENTRY_SIZE
        return self.num_records < max_records

    def write(self, value):
        if not self.has_capacity():
            return False
        offset = self.num_records * COLUMN_ENTRY_SIZE
        value_in_bytes = value.to_bytes(8, byteorder='little')
        self.data[offset : offset + 8] = value_in_bytes
        self.num_records += 1
        return True
    
    def read(self, index):
        if index >= self.num_records:
            return None
        start_offset = index * COLUMN_ENTRY_SIZE
        end_offset = start_offset + COLUMN_ENTRY_SIZE
        value_in_bytes = self.data[start_offset:end_offset]
        return int.from_bytes(value_in_bytes, byteorder='little')

