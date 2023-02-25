from ..bgpparser.parse_Process import ParseProcess
from ..bgpparser.read_Process import ReadProcess
from multiprocessing import JoinableQueue, cpu_count, Manager
from ..bgpparser.init import PeerIndexTable,Mrt
import time
import bz2
from ..bgpparser.params import *
import gzip

def init_peer_index(filename):
    GZIP_MAGIC = b'\x1f\x8b'
    BZ2_MAGIC = b'\x42\x5a\x68'
    f=open(filename,'rb')
    thdr = f.read(max(len(BZ2_MAGIC), len(GZIP_MAGIC)))
    f.close()
    if thdr.startswith(BZ2_MAGIC):
        f = bz2.BZ2File(filename, 'rb')
    elif thdr.startswith(GZIP_MAGIC):
        f = gzip.GzipFile(filename, 'rb')
    else:
        f = open(filename, 'rb')
    hdr=f.read(12)
    m=Mrt(hdr)
    m.unpack()
    length=m.data['length']
    msg=f.read(length)
    first_entry_pos=length+12
    peer_table=[]
    is_add_path=False
    f.close()
    if m.data['type'][0]==MRT_T['TABLE_DUMP_V2']:
        if m.data['subtype'][0] == TD_V2_ST['RIB_IPV4_UNICAST_ADDPATH'] \
            or m.data['subtype'][0] \
            == TD_V2_ST['RIB_IPV4_MULTICAST_ADDPATH'] \
            or m.data['subtype'][0] == TD_V2_ST['RIB_IPV6_UNICAST_ADDPATH'] \
            or m.data['subtype'][0] == TD_V2_ST['RIB_IPV6_MULTICAST_ADDPATH']:
            is_add_path=True
        peer = PeerIndexTable(msg)
        peer.is_add_path=is_add_path
        peer.unpack()
        for i in peer.data['peer_entries']:
            peer_table.append(i)
        return peer_table,first_entry_pos
    else:
        return [],0
    

def parse_multiprocessing(filename,path_to_write,worker_num=int(cpu_count()/3)):
    '''
    调用入口
    '''
    print(f'{worker_num=}')
    #init_peer_index(filename)
    byteq=JoinableQueue()
    peer_Table,first_entry_pos=init_peer_index(filename)
    producer=ReadProcess(filename, byteq, worker_num, first_entry_pos)
    worker=[]
    for i in range(worker_num):
        worker.append(ParseProcess(filename, path_to_write, byteq, peer_Table))

    stime=time.time()
    producer.start()
    for i in range(worker_num):
        worker[i].start()

    producer.join()
    for i in range(worker_num):
        worker[i].join()

    etime=time.time()
    print(etime-stime)




