
class Page:

    # variable for page size so it can be changed for optimization
    # each column entry is 8 bytes, keep another constant variable for that

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # given page size, calculate with record size max number of records
        # check num_records
        # return true/false
        pass

    def write(self, value):
        self.num_records += 1
        # calculate the offset needed to append to end of page
        # write value into data at offset
        pass

