'''
CS 519 GFS project
Yuhang Zhou yz853
Yi Qi yq97
12/21/2019
'''
import rpyc
from rpyc.utils.server import ThreadedServer
import os
path = './files/chunk/'


class chunk_service(rpyc.Service):
    file_table = {}

    def on_connect(self, conn):
        print('Connected to the chunk server!')

    def on_disconnect(self, conn):
        print('Disconnected with the chunk server.')

    ############################################################
    # 199 start here
    ############################################################
    # lock/leasing
    lock = 1

    # --------------------
    # initialize
    # --------------------
    chunkloc = 0
    chunktable = {}
    # local_filesystem_root = "/gfs/chunks/" + repr(chunkloc)
    # local_filesystem_root = os.path.expanduser("~")
    local_filesystem_root = "./gfs_root/chunks/"+repr(chunkloc)
    if not os.access(local_filesystem_root, os.W_OK):
        os.makedirs(local_filesystem_root)
        print("create chunk root")

    # def __init__(self, chunkloc):
    #     self.chunkloc = chunkloc
    #     self.chunktable = {}

    #     self.local_filesystem_root = "/tmp/gfs/chunks/" + repr(chunkloc)
    #     if not os.access(self.local_filesystem_root, os.W_OK):
    #         os.makedirs(self.local_filesystem_root)

    def exposed_write(self, chunkuuid, chunk):
        # check the lock
        while (self.lock != 1):
            tmp = 8
        self.lock = 0
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "wb") as f:
            f.write(chunk)
        self.chunktable[chunkuuid] = local_filename
        self.lock = 1

    def exposed_read(self, chunkuuid):
        data = None
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, "rb") as f:
            data = f.read()
            return data

    def chunk_filename(self, chunkuuid):
        local_filename = self.local_filesystem_root + \
            "/" + str(chunkuuid) + ".gfs"
        return local_filename

    def exposed_modify(self, modify_data, chunkuuid, left_pos, right_pos):
        local_filename = self.chunk_filename(chunkuuid)
        with open(local_filename, 'r+') as f:
            f.seek(right_pos)
            tmp_data = f.read()
            f.truncate(left_pos)  # remove the right part
            f.seek(0, 2)  # move the pointer to the end of file
            f.write(modify_data)
            f.write(tmp_data)


if __name__ == "__main__":
    t = ThreadedServer(chunk_service, port=8888)
    t.start()
