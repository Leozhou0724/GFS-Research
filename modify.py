
import os
from tempfile import TemporaryFile


def modify(modify_data, file, left_pos, right_pos):
    with open(file, 'r+') as f:
        f.seek(right_pos)
        tmp_data = f.read()
        # tmp_file = TemporaryFile('w+t')
        # tmp_file = TemporaryFile('w+b')
        # tmp_file.write(tmp_data)
        f.truncate(left_pos)  # remove the right part
        f.seek(0, 2)  # move the pointer to the end of file
        f.write(modify_data)
        f.write(tmp_data)
        # tmp_file.close()


#modify('abc', 'source_file.txt', 3, 8)
modify10 = './client/modifyfiles10/'
modify50 = './client/modifyfiles50/'
filename = modify10 + 'modify_file_' + str(0) + '.txt'
sourcefile_name = 'source_file_' + str(0) + '.txt'

with open(filename, 'rb') as f:
    data = f.read()
    size1 = len(data)
print(size1)

left = 0
right = size1
print(size1)
