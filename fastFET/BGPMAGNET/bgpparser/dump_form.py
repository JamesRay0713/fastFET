import sys
from datetime import *
import time
from time import sleep
from ..bgpparser.params import *
import configparser
import traceback
import copy, os

class BgpDump:
    __slots__ = [
        'verbose', 'output', 'ts_format', 'pkt_num', 'type', 'num', 'ts',
        'org_time', 'flag', 'peer_ip', 'peer_as', 'nlri', 'withdrawn',
        'as_path', 'origin', 'next_hop', 'local_pref', 'med', 'comm',
        'atomic_aggr', 'aggr', 'as4_path', 'as4_aggr', 'old_state', 'new_state','peer',
        'as_path_only','prefix_and_origin','utime'
    ]

    def __init__(self, output, peer_table):
        cp = configparser.ConfigParser()
        path= os.path.dirname(os.path.dirname(__file__))+ '/config/parseMRT.ini'
        cp.read(path)
        verbose = cp.get('init','verbose')
        if verbose=='0':
            self.verbose = False
        else:
            self.verbose = True
        self.output = sys.stdout
        f_handler=open(output,"a")
        self.output=f_handler
        # self.output=sys.stdout
        self.utime=0
        ts_format = cp.get('init','ts_format')
        if ts_format==0:
            self.ts_format = 'dump'
        else:
            self.ts_format = 'change'

        self.pkt_num = False
        self.type = ''
        self.peer=copy.copy(peer_table)
        self.num = 0
        self.ts = 0
        self.org_time = 0
        self.flag = ''
        self.peer_ip = ''
        self.peer_as = 0
        self.nlri = []
        self.withdrawn = []
        self.as_path = []
        self.origin = ''
        self.next_hop = []
        self.local_pref = 0
        self.med = 0
        self.comm = ''
        self.atomic_aggr = 'NAG'
        self.aggr = ''
        self.as4_path = []
        self.as4_aggr = ''
        self.old_state = 0
        self.new_state = 0
        self.as_path_only=cp.get('init','AS_PATH_ONLY')
        self.prefix_and_origin = cp.get('init','PREFIX_AND_ORIGIN')

    def close(self):
        self.output.close()
    
    def clear(self):
        self.type = ''
        self.num = 0
        self.ts = 0
        self.org_time = 0
        self.flag = ''
        self.peer_ip = ''
        self.peer_as = 0
        self.nlri = []
        self.withdrawn = []
        self.as_path = []
        self.origin = ''
        self.next_hop = []
        self.local_pref = 0
        self.med = 0
        self.comm = ''
        self.atomic_aggr = 'NAG'
        self.aggr = ''
        self.as4_path = []
        self.as4_aggr = ''
        self.old_state = 0
        self.new_state = 0

    def print_line(self, prefix, next_hop): 
        if self.ts_format == 'dump':
            d = self.ts
        else:
            d = self.org_time

        if self.verbose:
            d = str(d)
        else:
            d = datetime.utcfromtimestamp(d).strftime('%m/%d/%y %H:%M:%S')
        res=''
        if self.flag == 'B' or self.flag == 'A':
            if self.as_path_only == '0':
                if self.prefix_and_origin == '0':
                    if self.verbose == False:
                        res='%s|%s|%s|%s|%s|%s|%s\n' % (
                                self.type, d, self.flag, self.peer_ip, self.peer_as, prefix,
                                self.merge_as_path()
                            )
                        
                    else:
                        res='%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%s|%s|%s|\n' % (
                                self.type, d, self.flag, self.peer_ip, self.peer_as, prefix,
                                self.merge_as_path(), self.origin,next_hop, self.local_pref, self.med, self.comm,
                                self.atomic_aggr, self.merge_aggr()
                            )
                else:
                    origin_as = self.merge_as_path().split(' ')[-1]
                    res='%s|%s\n' %(prefix, origin_as)
            else:
                res='%s|%s\n'%(prefix,self.merge_as_path())
        elif self.flag == 'W':
            if self.as_path_only=='0' and self.prefix_and_origin=='0':
                if self.verbose==True:
                    res='%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%s|%s|%s|\n' % (
                                self.type, d, self.flag, self.peer_ip, self.peer_as, prefix,
                                self.merge_as_path(), self.origin,next_hop, self.local_pref, self.med, self.comm,
                                self.atomic_aggr, self.merge_aggr()
                            )
                else:
                    res='%s|%s|%s|%s|%s|%s|%s\n' % (
                            self.type, d, self.flag, self.peer_ip, self.peer_as,
                            prefix,self.merge_as_path()
                        )
            else:
                pass
        elif self.flag == 'STATE':
            if self.as_path_only=='0' and self.prefix_and_origin=='0':
                res='%s|%s|%s|%s|%s|%d|%d\n' % (
                        self.type, d, self.flag, self.peer_ip, self.peer_as,
                        self.old_state, self.new_state
                    )
            else:
                pass
        
        self.output.write(res)
        

    def print_routes(self):
        for withdrawn in self.withdrawn:
            if self.type == 'BGP4MP':
                self.flag = 'W'
            self.print_line(withdrawn, '')
            
        for nlri in self.nlri:
            if self.type == 'BGP4MP':
                self.flag = 'A'
            for next_hop in self.next_hop:            
                self.print_line(nlri, next_hop)
               
    
    def td(self, m):
        self.type = 'TABLE_DUMP'
        self.flag = 'B'
        self.ts = m['timestamp'][0]
        self.org_time = m['originated_time'][0]
        self.peer_ip = m['peer_ip']
        self.peer_as = m['peer_as']
        self.nlri.append('%s/%d' % (m['prefix'], m['prefix_length']))
        for attr in m['path_attributes']:
            self.bgp_attr(attr)
        self.print_routes()
        

    def td_v2(self, m):
        self.type = 'TABLE_DUMP2'
        self.flag = 'B'
        self.ts = m['timestamp'][0]
       
        if m['subtype'][0] == TD_V2_ST['PEER_INDEX_TABLE']:
            pass
            # for i in m['peer_entries']:
            #     self.peer.append(i)
        elif (m['subtype'][0] == TD_V2_ST['RIB_IPV4_UNICAST']
            or m['subtype'][0] == TD_V2_ST['RIB_IPV4_MULTICAST']
            or m['subtype'][0] == TD_V2_ST['RIB_IPV6_UNICAST']
            or m['subtype'][0] == TD_V2_ST['RIB_IPV6_MULTICAST']):
            
            self.num = m['sequence_number']
            self.nlri.append('%s/%d' % (m['prefix'], m['prefix_length']))
            for entry in m['rib_entries']:
                self.org_time = entry['originated_time'][0]
                self.peer_ip = self.peer[entry['peer_index']]['peer_ip']
                self.peer_as = self.peer[entry['peer_index']]['peer_as']
                self.as_path = []
                self.origin = ''
                self.next_hop = []
                self.local_pref = 0
                self.med = 0
                self.comm = ''
                self.atomic_aggr = 'NAG'
                self.aggr = ''
                self.as4_path = []
                self.as4_aggr = ''
                
                for attr in entry['path_attributes']:
                    self.bgp_attr(attr)
                
                self.print_routes()
                

    def bgp4mp(self, m):
        self.type = 'BGP4MP'
        self.ts = m['timestamp'][0]
        self.org_time = m['timestamp'][0]
        self.peer_ip = m['peer_ip']
        self.peer_as = m['peer_as']
        if (m['subtype'][0] == BGP4MP_ST['BGP4MP_STATE_CHANGE']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_STATE_CHANGE_AS4']):
            self.flag = 'STATE'
            self.old_state = m['old_state'][0]
            self.new_state = m['new_state'][0]
            self.print_line([], '')
        elif (m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_AS4']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']):
            if m['bgp_message']['type'][0] != BGP_MSG_T['UPDATE']:
                return
            for attr in m['bgp_message']['path_attributes']:
                self.bgp_attr(attr)
            for withdrawn in m['bgp_message']['withdrawn_routes']:
                self.withdrawn.append(
                    '%s/%d' % (
                        withdrawn['prefix'], withdrawn['prefix_length']
                    )
                )
            for nlri in m['bgp_message']['nlri']:
                self.nlri.append(
                    '%s/%d' % (
                        nlri['prefix'], nlri['prefix_length']
                    )
                )
            
            self.print_routes()
            

    def bgp_attr(self, attr):
        if attr['type'][0] == BGP_ATTR_T['NEXT_HOP']:
            self.next_hop.append(attr['value'])
        elif attr['type'][0] == BGP_ATTR_T['ORIGIN']:
            self.origin = ORIGIN_T[attr['value']]
        elif attr['type'][0] == BGP_ATTR_T['AS_PATH']:
            self.as_path = []
            for seg in attr['value']:
                if seg['type'][0] == AS_PATH_SEG_T['AS_SET']:
                    self.as_path.append('{%s}' % ','.join(seg['value']))
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as_path.append('(' + seg['value'][0])
                    self.as_path += seg['value'][1:-1]
                    self.as_path.append(seg['value'][-1] + ')')
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as_path.append('[%s]' % ','.join(seg['value']))
                else:
                    self.as_path += seg['value']
        elif attr['type'][0] == BGP_ATTR_T['MP_REACH_NLRI']:
            self.next_hop = attr['value']['next_hop']
            if self.type != 'BGP4MP':
                return
            for nlri in attr['value']['nlri']:
                self.nlri.append(
                    '%s/%d' % (
                        nlri['prefix'], nlri['prefix_length']
                    )
                )
        elif attr['type'][0] == BGP_ATTR_T['MP_UNREACH_NLRI']:
            if self.type != 'BGP4MP':
                return
            for withdrawn in attr['value']['withdrawn_routes']:
                self.withdrawn.append(
                    '%s/%d' % (
                        withdrawn['prefix'], withdrawn['prefix_length']
                    )
                )
        elif attr['type'][0] == BGP_ATTR_T['AS4_PATH']:
            self.as4_path = []
            for seg in attr['value']:
                if seg['type'][0] == AS_PATH_SEG_T['AS_SET']:
                    self.as4_path.append('{%s}' % ','.join(seg['value']))
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as4_path.append('(' + seg['value'][0])
                    self.as4_path += seg['value'][1:-1]
                    self.as4_path.append(seg['value'][-1] + ')')
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as4_path.append('[%s]' % ','.join(seg['value']))
                else:
                    self.as4_path += seg['value']
        elif self.verbose:
            if attr['type'][0] == BGP_ATTR_T['AS4_AGGREGATOR']:
                self.as4_aggr = '%s %s' % (
                    attr['value']['as'], attr['value']['id']
                )           
            elif attr['type'][0] == BGP_ATTR_T['MULTI_EXIT_DISC']:
                self.med = attr['value']
            elif attr['type'][0] == BGP_ATTR_T['LOCAL_PREF']:
                self.local_pref = attr['value']
            elif attr['type'][0] == BGP_ATTR_T['ATOMIC_AGGREGATE']:
                self.atomic_aggr = 'AG'
            elif attr['type'][0] == BGP_ATTR_T['AGGREGATOR']:
                self.aggr = '%s %s' % (attr['value']['as'], attr['value']['id'])

            elif attr['type'][0] == BGP_ATTR_T['COMMUNITY']:
                self.comm = ' '.join(attr['value'])
        


    def merge_as_path(self):
        if len(self.as4_path):
            n = len(self.as_path) - len(self.as4_path)
            return ' '.join(self.as_path[:n] + self.as4_path)
        else:
            return ' '.join(self.as_path)

    def merge_aggr(self):
        if len(self.as4_aggr):
            return self.as4_aggr
        else:
            return self.aggr
