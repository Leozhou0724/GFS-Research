'''
CS 519 GFS project
Yuhang Zhou yz853
Yi Qi yq97
12/21/2019
'''
import rpyc
import sys
import time
import os
import numpy as np
path = './client/sourcefiles/'

# Three Basic Functions:
# Download
# Upload
# Delete
# change ip of master and chunkserver
master_ip = '128.6.4.131'
chunk_ip = '128.6.13.131'
# change the master_ip to localhost if you want to deploy them in localhost
#master_ip = 'localhost'
#chunk_ip = 'localhost'


class client:
    client_root = os.path.expanduser("~")
    client_root += "./gfs_root/client/"
    if not os.access(client_root, os.W_OK):
        os.makedirs(client_root)
        print("create client root")
    con_master = rpyc.connect(master_ip, port=18861)
    master_sever = con_master.root
    master = master_sever

    chunkservers = {}
    con_chunk = rpyc.connect(chunk_ip, port=8888)
    chunkservers[0] = con_chunk.root

    def __init__(self):
        # connect to the master
        return

    def write(self, filename, data):
        if self.exists(filename):
            self.delete(filename)
            print('file already exists')
        else:
            print('this is a new data')
        with open(path + str(data), 'rb') as f:
            data = f.read()
            # num_chunks = self.num_chunks(len(data))
            num_chunks = 1
            # ---
            print("num_chunks", num_chunks)
            # ---
            chunkuuids = self.master.alloc_file(filename, num_chunks)
            print("chunk_id ", chunkuuids)

            self.write_chunks(chunkuuids, data)

    def write_chunks(self, chunkuuids, data):
        # ----------------------------
        # chunks = [ data[x:x+self.master.chunksize] \
        #     for x in range(0, len(data), self.master.chunksize) ]
        # ------------------------------
        chunks = {}
        chunks[0] = data
        chunkservers = self.chunkservers
        print("num_chunkuids", len(chunkuuids))
        for i in range(0, len(chunkuuids)):  # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            print("chunk_location", chunkloc)
            chunkservers[chunkloc].write(chunkuuid, chunks[i])

    def num_chunks(self, size):
        # return (size // self.master.chunksize) \
        #     + (1 if size % self.master.chunksize > 0 else 0)
        return 1

    def exists(self, filename):
        return self.master.exists(filename)

    def read(self, filename):  # get metadata, then read chunks direct
        if not self.exists(filename):
            raise Exception("read error, file does not exist: "
                            + filename)
        chunks = []
        chunkuuids = self.master.get_chunkuuids(filename)
        chunkservers = self.chunkservers
        for chunkuuid in chunkuuids:
            chunkloc = self.master.get_chunkloc(chunkuuid)
            chunk = chunkservers[chunkloc].read(chunkuuid)
            chunks.append(chunk)
        data = chunk
        # data = reduce(lambda x, y: x + y, chunks) # reassemble in order
        with open('./' + str(filename), 'wb') as f:
            f.write(data)

    def delete(self, filename):
        self.master.delete(filename)

    def show_file(self):
        filelist = self.master.filelist

    def modify(self, filename, modify_file, left_pos, right_pos):
        if not self.exists(filename):
            print('Source file does not exist')
        chunkuuids = self.master.get_chunkuuids(filename)
        print("chunk_id ", chunkuuids)
        with open(str(modify_file), 'rb') as mf:
            modify_data = mf.read()
            # just modify in the first chunk
            chunkservers = self.chunkservers
            chunkuuid = chunkuuids[0]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            print("chunk_location", chunkloc)
            chunkservers[chunkloc].modify(
                modify_data, chunkuuid, left_pos, right_pos)


client1 = client()
print("client root is ", client1.client_root)
print("download root is local root")
print("chunk root is ./gfs_root/chunks/0")
print("masterserver IP is ", master_ip)
print("chunkserver IP is ", chunk_ip)


def upload_test():
    t1 = time.time()
    client1 = client()
    sourcefile = 'source_file_'
    for i in range(10):
        start_time = time.time()
        source = sourcefile + str(i)+'.txt'
        print(source)
        client1.write(source, source)
        end_time = time.time()
        print('write time', i+1, ':', (end_time-start_time))

    client1.master.dump_metadata()
    t2 = time.time()
    print('Runtime = ', t2 - t1)


def modify_test():
    size = [5000, 10000, 20000, 30000, 50000,
            80000, 100000, 150000, 250000, 500000]
    modify10 = './client/modifyfiles10/'
    modify50 = './client/modifyfiles50/'
    runtime = []
    for i in range(4, 10):
        single_time = []
        for j in range(10):
            filename = modify10 + 'modify_file_' + str(j) + '.txt'
            sourcefile_name = 'source_file_' + str(i) + '.txt'

            with open(filename, 'rb') as f:
                data = f.read()
                size1 = len(data)
            print(size1)
            with open('./client/sourcefiles/'+sourcefile_name, 'rb') as f:
                data = f.read()
                size2 = len(data)
            print(size2)
            left = int(size2 - size1)
            right = int(size2)

            left = 0
            right = int(size1)

            print(filename, sourcefile_name)
            start_time = time.time()
            client1.modify(sourcefile_name, filename, left, right)
            end_time = time.time()
            print(i, end_time - start_time)
            single_time.append(end_time - start_time)
        runtime.append(single_time)
    print(np.array(runtime))


upload_test()
# modify_test()
