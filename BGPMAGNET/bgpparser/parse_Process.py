import collections
import signal
from datetime import datetime
from ..bgpparser.dump_form import BgpDump
from ..bgpparser.params import *
from ..bgpparser.base import *
from multiprocessing import Process,Manager
from ..bgpparser.init import TableDump,Mrt,Bgp4Mp,AfiSpecRib,RibGeneric,PeerIndexTable
import time
try:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
    pass

__version__ = '2.0.2-dev'

class ParseProcess(Process):
    '''
    parser for MRT format data.
    '''
    __slots__ = ['data','f', 'err', 'err_msg','q','resq','usedt']

    def __init__(self, arg, path_to_write, q, peer_table):
        super(ParseProcess,self).__init__()
        self.data = collections.OrderedDict()
        self.q=q
        filename=arg+'.txt'
        self.f=BgpDump(path_to_write,peer_table)
        self.usedt=0

    def close(self):
        '''
        Close file object and return.
        '''
        self.f.close()
        return
        
    def run(self):
        while True:           
            res=self.q.get()
            
            self.f.clear()
            if res==None:
                self.q.task_done()
                self.close()
                break
     
            self.parse_and_write(res)
            self.q.task_done()           
        return
    
    def parse_and_write(self,buf):
        '''
        parse given buf and write
        '''
        hdr=buf[0:12]
        msg=buf[12:]
        mrt=Mrt(hdr)
        mrt.unpack()
        self.data=mrt.data
        try:
            self.unpack_msg(mrt,msg)
        except MrtFormatError as e:
            self.err = MRT_ERR_C['MRT Data Error']
            self.err_msg = e.msg
            self.buf = mrt.buf
    
    def unpack_msg(self, mrt, msg):
        '''
        Decoder for MRT message.
        '''
        buf = msg
        mrt.buf += buf
        if len(buf) < mrt.data['length']:
            raise MrtFormatError(
                'Invalid MRT data length %d < %d byte'
                % (len(buf), mrt.data['length'])
            )

        if mrt.data['subtype'][0] == 'Unknown':
            raise MrtFormatError(
                'Unsupported type %d(%s) subtype %d(%s)'
                % tuple(mrt.data['type'] + mrt.data['subtype'])
            )

        if mrt.data['type'][0] == MRT_T['TABLE_DUMP_V2']:
            self.unpack_td_v2(buf, mrt)
            self.f.td_v2(self.data)
        elif mrt.data['type'][0] == MRT_T['BGP4MP'] \
            or mrt.data['type'][0] == MRT_T['BGP4MP_ET']:
            if mrt.data['subtype'][0] == MRT_T['BGP4MP_ENTRY'] \
                or mrt.data['subtype'][0] == MRT_T['BGP4MP_SNAPSHOT']:
                self.p += mrt.data['length']
                raise MrtFormatError(
                    'Unsupported type %d(%s) subtype %d(%s)'
                    % tuple(mrt.data['type'] + mrt.data['subtype'])
                )
            else:
                if mrt.data['type'][0] == MRT_T['BGP4MP_ET']:
                    mrt.data['microsecond_timestamp'] = mrt.val_num(4)
                    buf = buf[4:]
                bgp = Bgp4Mp(buf)
                bgp.unpack(mrt.data['subtype'][0])
                self.data.update(bgp.data)
                self.f.bgp4mp(self.data)

        elif mrt.data['type'][0] == MRT_T['TABLE_DUMP']:
            td = TableDump(buf)
            td.unpack(mrt.data['subtype'][0])
            self.data.update(td.data)   
            self.f.td(self.data)


        else:
            self.p += mrt.data['length']
            raise MrtFormatError(
                'Unsupported type %d(%s) subtype %d(%s)'
                % tuple(mrt.data['type'] + mrt.data['subtype'])
            )

    
    def unpack_td_v2(self, data, mrt):
        '''
        Decoder for Table_Dump_V2 format.
        '''
        is_add_path=False
        af_num_afi=0
        if mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV4_UNICAST_ADDPATH'] \
            or mrt.data['subtype'][0] \
            == TD_V2_ST['RIB_IPV4_MULTICAST_ADDPATH'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV6_UNICAST_ADDPATH'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV6_MULTICAST_ADDPATH']:
            is_add_path=True

        if mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV4_UNICAST'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV4_MULTICAST'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV4_UNICAST_ADDPATH'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV4_MULTICAST_ADDPATH']:
            af_num_afi = AFI_T['IPv4']
            rib = AfiSpecRib(data)
            rib.is_add_path=is_add_path
            rib.af_num_afi=af_num_afi
            rib.unpack()
            self.data.update(rib.data)
        elif mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV6_UNICAST'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV6_MULTICAST'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV6_UNICAST_ADDPATH'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_IPV6_MULTICAST_ADDPATH']:
            af_num_afi = AFI_T['IPv6']
            rib = AfiSpecRib(data)
            rib.is_add_path=is_add_path
            rib.af_num_afi=af_num_afi
            rib.unpack()
            self.data.update(rib.data)
        elif mrt.data['subtype'][0] == TD_V2_ST['PEER_INDEX_TABLE']:
            peer = PeerIndexTable(data)
            peer.is_add_path=is_add_path
            peer.unpack()
            self.data.update(peer.data)
        elif mrt.data['subtype'][0] == TD_V2_ST['RIB_GENERIC'] \
            or mrt.data['subtype'][0] == TD_V2_ST['RIB_GENERIC_ADDPATH']:
            rib = RibGeneric(data)
            rib.is_add_path=is_add_path
            rib.unpack()
            self.data.update(rib.data)
        else:
            self.p += self.mrt.len
