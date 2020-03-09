import rpyc
import sys
import os
import time
path = './client/sourcefiles/'

master_ip = '128.6.4.131'
chunk_ip = '128.6.13.131'
# Three Basic Functions:
# Download
# Upload
# Delete


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
            # print("num_chunks",num_chunks)
            # ---
            chunkuuids = self.master.alloc_file(filename, num_chunks)
            print("chunk_id ", chunkuuids)

            self.write_chunks(chunkuuids, data)

    def write_chunks(self, chunkuuids, data):

        chunks = {}
        chunks[0] = data
        chunkservers = self.chunkservers
        # print("num_chunkuids",len(chunkuuids))
        for i in range(0, len(chunkuuids)):  # write to each chunkserver
            chunkuuid = chunkuuids[i]
            chunkloc = self.master.get_chunkloc(chunkuuid)
            # print("chunk_location",chunkloc)
            chunkservers[chunkloc].write(chunkuuid, chunks[i])

    def num_chunks(self, size):

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

        with open('./' + str(filename), 'wb') as f:
            f.write(data)

    def delete(self, filename):
        self.master.delete(filename)

    def show_file(self):
        filelist = self.master.filelist


t1 = time.time()
client1 = client()
print("masterserver IP is ", master_ip)
print("chunkserver IP is ", chunk_ip)
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
# client1.read('pic.jpg')
# finish_time = time.time()
# print('read time: ',(finish_time-end_time))
# print('end')
