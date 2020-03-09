'''
CS 519 GFS project
Yuhang Zhou yz853
Yi Qi yq97
12/21/2019
'''
import rpyc
from rpyc.utils.server import ThreadedServer
import time
import uuid


class master_service(rpyc.Service):
    def on_connect(self, conn):
        print('Connected to the master server!')

    def on_disconnect(self, conn):
        print('Disconnected with the master server.')

    num_chunkservers = 1
    max_chunkservers = 1
    max_chunksperfile = 1

    #
    chunksize = 100*1024*1024
    chunkrobin = 0
    filetable = {}
    chunktable = {}
    # For now only one chunk server
    chunkservers = {}

    def exposed_get_chunkservers(self):
        return self.chunkservers

    # Allocate a new file with filename and number of chunks
    def exposed_alloc_file(self, fname, num_chunks):  # return ordered chunkuuid list
        chunkuuids = self.alloc_chunks(num_chunks)
        self.filetable[fname] = chunkuuids
        return chunkuuids

    # allocate chunks

    def alloc_chunks(self, num_chunks):
        chunkuuids = []
        for i in range(0, num_chunks):
            chunkuuid = uuid.uuid1()
            chunkuuid = str(chunkuuid)
            chunkloc = self.chunkrobin
            self.chunktable[chunkuuid] = chunkloc
            chunkuuids.append(chunkuuid)
            self.chunkrobin = (self.chunkrobin + 1) % self.num_chunkservers
        return chunkuuids

    # append some chunks to a file
    def exposed_append(self, fname, num_append_chunks):  # append chunks
        chunkuuids = self.filetable[fname]
        append_chunkuuids = self.alloc_chunks(num_append_chunks)
        chunkuuids.extend(append_chunkuuids)
        return append_chunkuuids

    def exposed_get_chunkloc(self, chunkuuid):
        return self.chunktable[chunkuuid]

    def exposed_get_chunkuuids(self, fname):
        return self.filetable[fname]

    def exposed_exists(self, fname):
        return True if fname in self.filetable.keys() else False
    # delete the file and rename it for garbge collection

    def exposed_delete(self, fname):
        chunkuuids = self.filetable[fname]
        del self.filetable[fname]
        # #garbge collection
        # timestamp = repr(time.time())
        # deleted_filename = "/hidden/deleted/" + timestamp + fname
        # self.filetable[deleted_filename] = chunkuuids
        # print ("deleted file: " + fname + " renamed to " + \
        #      deleted_filename + " ready for gc")

    def exposed_dump_metadata(self):
        print("Filetable:")
        for filename, chunkuuids in self.filetable.items():
            print(filename, "with", len(chunkuuids), "chunks")
        # print ("Chunkservers: ", len(self.chunkservers))
        # print ("Chunkserver Data:")
        # for chunkuuid, chunkloc in sorted(self.chunktable.iteritems(), key=operator.itemgetter(1)):
        #     chunk = self.chunkservers[chunkloc].read(chunkuuid)
        #     print (chunkloc, chunkuuid, chunk)

    def exposed_filelist(self):
        return self.filetable


if __name__ == "__main__":
    t = ThreadedServer(master_service, port=18861)
    t.start()
