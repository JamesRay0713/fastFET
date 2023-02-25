import multiprocessing
import sys
import gzip
import bz2
import signal
from ..bgpparser.params import *
from multiprocessing import Process,JoinableQueue,cpu_count
from ..bgpparser.base import *

try:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
    pass

__version__ = '2.0.2-dev'

# Magic Number
GZIP_MAGIC = b'\x1f\x8b'
BZ2_MAGIC = b'\x42\x5a\x68'

class ReadProcess(Process):
    '''
    Reader to get bytes into queue
    '''
    __slots__ = ['f', 'err', 'err_msg','q', 'cpu', 'fep']

    def __init__(self, arg, q:JoinableQueue, consumer_num, first_entry_pos):
        super(ReadProcess,self).__init__()
        self.q=q
        self.cpu=consumer_num
        self.fep=first_entry_pos
        # file instance
        if hasattr(arg, 'read'):
            self.f = arg
        # file path
        elif isinstance(arg, str):
            f = open(arg, 'rb')
            hdr = f.read(max(len(BZ2_MAGIC), len(GZIP_MAGIC)))
            f.close()

            if hdr.startswith(BZ2_MAGIC):
                self.f = bz2.BZ2File(arg, 'rb')
            elif hdr.startswith(GZIP_MAGIC):
                self.f = gzip.GzipFile(arg, 'rb')
            else:
                self.f = open(arg, 'rb')
        else:
            sys.stderr.write("Error: Unsupported instance type\n")

    def close(self):
        '''
        Close file object and return.
        '''
        self.f.close()
        return
        
    def run(self):
        if self.fep!=0:
            self.f.read(self.fep)
        buf=self.f.read(12)
        while (len(buf) != 0):
            try:
                if len(buf) < 12:
                    raise MrtFormatError(
                        'Invalid MRT header length %d < 12 byte' % len(buf)
                    )
                length=0
                for i in buf[8:12]:
                    length = (length << 8) + i
                msg=self.f.read(length)
                complete_msg=buf+msg
                self.q.put(complete_msg)
            except MrtFormatError as e:
                self.err = MRT_ERR_C['MRT Header Error']
                self.err_msg = e.msg
                self.buf = buf
            finally:
                buf=self.f.read(12)
        self.f.close()
        # while (buf := self.f.read(12)):
        #     try:

        #         if len(buf) == 0:
        #             self.f.close()
        #             return
        #         elif len(buf) < 12:
        #             raise MrtFormatError(
        #                 'Invalid MRT header length %d < 12 byte' % len(buf)
        #             )
        #         length=0
        #         for i in buf[8:12]:
        #             length = (length << 8) + i
        #         msg=self.f.read(length)
        #         complete_msg=buf+msg
        #         self.q.put(complete_msg)
        #     except MrtFormatError as e:
        #         self.err = MRT_ERR_C['MRT Header Error']
        #         self.err_msg = e.msg
        #         self.buf = buf
        for i in range(self.cpu):
            self.q.put(None)
        
        self.q.join()
