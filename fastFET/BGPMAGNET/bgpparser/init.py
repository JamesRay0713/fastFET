import collections
import signal
from datetime import datetime
from ..bgpparser.params import *
from ..bgpparser.base import *

try:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
    pass

__version__ = '2.0.2-dev'

class Mrt(Base):
    '''
    Class for MRT header.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for MRT header.
        '''
        self.data['timestamp'] = [self.val_num(4)]
        self.data['timestamp'].append(
            str(datetime.fromtimestamp(self.data['timestamp'][0]))
        )
        self.data['type'] = [self.val_num(2)]
        self.data['type'].append(MRT_T[self.data['type'][0]])
        self.data['subtype'] = [self.val_num(2)]
        self.data['subtype'].append(
            MRT_ST[self.data['type'][0]][self.data['subtype'][0]]
        )
        self.data['length'] = self.val_num(4)

        return self.p

class TableDump(Base):
    '''
    Class for Table_Dump format.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self, subtype):
        '''
        Decoder for Table_Dump format.
        '''
        self.data['view_number'] = self.val_num(2)
        self.data['sequence_number'] = self.val_num(2)
        self.data['prefix'] = self.val_addr(subtype)
        self.data['prefix_length'] = self.val_num(1)
        self.data['status'] = self.val_num(1)
        self.data['originated_time'] = [self.val_num(4)]
        self.data['originated_time'].append(
            str(datetime.fromtimestamp(self.data['originated_time'][0]))
        )

        # Considering the IPv4 peers advertising IPv6 Prefixes, first,
        # the Peer IP Address field is decoded as an IPv4 address.
        self.data['peer_ip'] = self.val_addr(AFI_T['IPv4'])
        if subtype == AFI_T['IPv6'] and self.val_num(12):
            self.p -= 16
            self.data['peer_ip'] = self.val_addr(subtype)
        self.as_len=2
        self.data['peer_as'] = self.val_as(self.as_len)
        self.data['path_attribute_length'] = attr_len = self.val_num(2)
        self.data['path_attributes'] = []
        while attr_len > 0:
            attr = BgpAttr(self.buf[self.p:])
            attr.as_len=2
            self.p += attr.unpack()
            self.data['path_attributes'].append(attr.data)
            attr_len -= attr.p
        return self.p

class PeerIndexTable(Base):
    '''
    Class for PEER_INDEX_TABLE format.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for PEER_INDEX_TABLE format.
        '''
        self.data['collector_bgp_id'] = self.val_addr(AFI_T['IPv4'])
        self.data['view_name_length'] = self.val_num(2)
        self.data['view_name'] = self.val_str(self.data['view_name_length'])
        self.data['peer_count'] = self.val_num(2)
        self.data['peer_entries'] = []
        for _ in range(self.data['peer_count']):
            entry = PeerEntries(self.buf[self.p:])
            entry.is_add_path=self.is_add_path
            self.p += entry.unpack()
            self.data['peer_entries'].append(entry.data)
        return self.p

class PeerEntries(Base):
    '''
    Class for Peer Entries.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for Peer Entries.
        '''
        self.data['peer_type'] = self.val_num(1)
        self.data['peer_bgp_id'] = self.val_addr(AFI_T['IPv4'])
        if self.data['peer_type'] & 0x01:
            self.af_num_afi = AFI_T['IPv6']
        else:
            self.af_num_afi = AFI_T['IPv4']
        self.data['peer_ip'] = self.val_addr(self.af_num_afi)
        self.data['peer_as'] = self.val_as(
            4 if self.data['peer_type'] & (0x01 << 1) else 2
        )
        return self.p

class RibGeneric(Base):
    '''
    Class for RIB_GENERIC format.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for RIB_GENERIC format.
        '''
        self.data['sequence_number'] = self.val_num(4)
        self.af_num_afi = self.val_num(3)
        self.data['afi'] = [self.af_num_afi, AFI_T[self.af_num_afi]]
        self.af_num_safi = self.val_num(1)
        self.data['safi'] = [self.af_num_safi, SAFI_T[self.af_num_safi]]
        n = self.val_num(1)
        self.p -= 1
        self.data['nlri'] \
            = self.val_nlri(self.p+(n+7)//8, self.af_num_afi, self.af_num_safi)
        self.data['entry_count'] = self.val_num(2)
        self.data['rib_entries'] = []
        for _ in range(self.data['entry_count']):
            entry = RibEntries(self.buf[self.p:])
            entry.is_add_path=self.is_add_path
            self.p += entry.unpack()
            self.data['rib_entries'].append(entry.data)
        return self.p

class AfiSpecRib(Base):
    '''
    Class for AFI/SAFI-Specific RIB format.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for AFI/SAFI-Specific RIB format.
        '''
        self.data['sequence_number'] = self.val_num(4)
        self.data['prefix_length'] = self.val_num(1)
        self.data['prefix'] \
            = self.val_addr(self.af_num_afi, self.data['prefix_length'])
        self.data['entry_count'] = self.val_num(2)
        self.data['rib_entries'] = []
        for _ in range(self.data['entry_count']):
            entry = RibEntries(self.buf[self.p:])
            entry.is_add_path=self.is_add_path
            entry.af_num_afi=self.af_num_afi
            self.p += entry.unpack()
            self.data['rib_entries'].append(entry.data)
        return self.p

class RibEntries(Base):
    '''
    Class for Rib Entries format.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for Rib Entries format.
        '''
        self.data['peer_index'] = self.val_num(2)
        self.data['originated_time'] = [self.val_num(4)]
        self.data['originated_time'].append(
            str(datetime.fromtimestamp(self.data['originated_time'][0]))
        )
        if self.is_add_path:
            self.data['path_id'] = self.val_num(4)
        attr_len = self.data['path_attribute_length'] = self.val_num(2)
        self.data['path_attributes'] = []
        while attr_len > 0:
            attr = BgpAttr(self.buf[self.p:])
            attr.is_add_path=self.is_add_path
            attr.af_num_afi=self.af_num_afi
            self.p += attr.unpack()
            self.data['path_attributes'].append(attr.data)
            attr_len -= attr.p
        return self.p

class Bgp4Mp(Base):
    '''
    Class for BGP4MP format.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self, subtype):
        '''
        Decoder for BGP4MP format.
        '''
        if subtype == BGP4MP_ST['BGP4MP_STATE_CHANGE'] \
            or subtype == BGP4MP_ST['BGP4MP_MESSAGE'] \
            or subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL'] \
            or subtype == BGP4MP_ST['BGP4MP_MESSAGE_ADDPATH'] \
            or subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL_ADDPATH']:
            self.as_len=2

        if subtype == BGP4MP_ST['BGP4MP_MESSAGE_ADDPATH'] \
            or subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_ADDPATH'] \
            or subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL_ADDPATH'] \
            or subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL_ADDPATH']:
            self.is_add_path=True

        self.data['peer_as'] = self.val_as(self.as_len)
        self.data['local_as'] = self.val_as(self.as_len)
        self.data['ifindex'] = self.val_num(2)
        self.data['afi'] = [self.val_num(2)]
        self.data['afi'].append(AFI_T[self.data['afi'][0]])
        self.data['peer_ip'] = self.val_addr(self.data['afi'][0])
        self.data['local_ip'] = self.val_addr(self.data['afi'][0])

        if subtype == BGP4MP_ST['BGP4MP_STATE_CHANGE'] \
            or subtype == BGP4MP_ST['BGP4MP_STATE_CHANGE_AS4']:
            self.data['old_state'] = [self.val_num(2)]
            self.data['old_state'].append(BGP_FSM[self.data['old_state'][0]])
            self.data['new_state'] = [self.val_num(2)]
            self.data['new_state'].append(BGP_FSM[self.data['new_state'][0]])
        else:
            bgp_msg = BgpMessage(self.buf[self.p:])
            bgp_msg.as_len=self.as_len
            bgp_msg.is_add_path=self.is_add_path
            self.p += bgp_msg.unpack()
            self.data['bgp_message'] = bgp_msg.data
        return self.p

class BgpMessage(Base):
    '''
    Class for BGP Message.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for BGP Message.
        '''
        self.data['marker'] = self.val_bytes(16)
        self.data['length'] = self.val_num(2)
        self.data['type'] = [self.val_num(1)]
        self.data['type'].append(BGP_MSG_T[self.data['type'][0]])

        if self.data['type'][0] == BGP_MSG_T['OPEN']:
            self.unpack_open()
        elif self.data['type'][0] == BGP_MSG_T['UPDATE']:
            self.unpack_update()
        elif self.data['type'][0] == BGP_MSG_T['NOTIFICATION']:
            self.unpack_notification()
        elif self.data['type'][0] == BGP_MSG_T['ROUTE-REFRESH']:
            self.unpack_route_refresh()

        self.p += self.data['length'] - self.p
        return self.p

    def unpack_open(self):
        '''
        Decoder for BGP OPEN Message.
        '''
        self.data['version'] = self.val_num(1)
        self.data['local_as'] = self.val_num(2)
        self.data['holdtime'] = self.val_num(2)
        self.data['bgp_id'] = self.val_addr(AFI_T['IPv4'])
        opt_len = self.data['length'] = self.val_num(1)
        self.data['optional_parameters'] = []
        while opt_len > 0:
            opt_params = OptParams(self.buf[self.p:])
            opt_params.as_len=self.as_len
            opt_params.is_add_path=self.is_add_path
            self.p += opt_params.unpack()
            self.data['optional_parameters'].append(opt_params.data)
            opt_len -= opt_params.p

    def unpack_update(self):
        '''
        Decoder for BGP UPDATE Message.
        '''
        self.data['withdrawn_routes_length'] = self.val_num(2)
        self.data['withdrawn_routes'] = self.val_nlri(
            self.p + self.data['withdrawn_routes_length'], AFI_T['IPv4']
        )
        self.data['path_attribute_length'] = self.val_num(2)
        attr_len = self.p + self.data['path_attribute_length']
        self.data['path_attributes'] = []
        while self.p < attr_len:
            attr = BgpAttr(self.buf[self.p:])
            attr.as_len=self.as_len
            attr.is_add_path=self.is_add_path
            self.p += attr.unpack()
            self.data['path_attributes'].append(attr.data)
        self.data['nlri'] = self.val_nlri(self.data['length'], AFI_T['IPv4'])

    def unpack_notification(self):
        '''
        Decoder for BGP NOTIFICATION Message.
        '''
        self.data['error_code'] = [self.val_num(1)]
        self.data['error_code'].append(BGP_ERR_C[self.data['error_code'][0]])
        self.data['error_subcode'] = [self.val_num(1)]
        self.data['error_subcode'].append(
            BGP_ERR_SC[self.data['error_code'][0]]\
                [self.data['error_subcode'][0]]
        )
        self.data['data'] = self.val_bytes(self.data['length'] - self.p)

    def unpack_route_refresh(self):
        '''
        Decoder for BGP ROUTE-REFRESH Message.
        '''
        self.data['afi'] = [self.val_num(2)]
        self.data['afi'].append(AFI_T[self.data['afi'][0]])
        self.data['reserved'] = self.val_num(1)
        self.data['safi'] = [self.val_num(1)]
        self.data['safi'].append(SAFI_T[self.data['safi'][0]])

class OptParams(Base):
    '''
    Class for BGP OPEN Optional Parameters.
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for BGP OPEN Optional Parameters.
        '''
        self.data['type'] = [self.val_num(1)]
        self.data['type'].append(BGP_OPT_PARAMS_T[self.data['type'][0]])
        self.data['length'] = self.val_num(1)
        if self.data['type'][0] == BGP_OPT_PARAMS_T['Capabilities']:
            self.unpack_capabilities()
        else:
            self.p += self.data['length']
        return self.p

    def unpack_capabilities(self):
        '''
        Decoder for BGP Capabilities.
        '''
        self.data['type'] = [self.val_num(1)]
        self.data['type'].append(BGP_CAP_C[self.data['type'][0]])
        self.data['length'] = self.val_num(1)

        if self.data['type'][0] \
            == BGP_CAP_C['Multiprotocol Extensions for BGP-4']:
            self.unpack_multi_ext()
        elif self.data['type'][0] \
            == BGP_CAP_C['Route Refresh Capability for BGP-4']:
            self.p += self.data['length'] - 2
        elif self.data['type'][0] \
            == BGP_CAP_C['Outbound Route Filtering Capability']:
            self.unpack_orf()
        elif self.data['type'][0] == BGP_CAP_C['Graceful Restart Capability']:
            self.unpack_graceful_restart()
        elif self.data['type'][0] \
            == BGP_CAP_C['Support for 4-octet AS number capability']:
            self.unpack_support_as4()
        elif self.data['type'][0] == BGP_CAP_C['ADD-PATH Capability']:
            self.unpack_add_path()
        else:
            self.p += self.data['length'] - 2

    def unpack_multi_ext(self):
        '''
        Decoder for Multiprotocol Extensions for BGP-4.
        '''
        self.data['value'] = collections.OrderedDict()
        self.data['value']['afi'] = [self.val_num(2)]
        self.data['value']['afi'].append(AFI_T[self.data['value']['afi'][0]])
        self.data['value']['reserved'] = self.val_num(1)
        self.data['value']['safi'] = [self.val_num(1)]
        self.data['value']['safi'].append(SAFI_T[self.data['value']['safi'][0]])

    def unpack_orf(self):
        '''
        Decoder for Outbound Route Filtering Capability.
        '''
        self.data['value'] = collections.OrderedDict()
        self.data['value']['afi'] = [self.val_num(2)]
        self.data['value']['afi'].append(AFI_T[self.data['value']['afi'][0]])
        self.data['value']['reserved'] = self.val_num(1)
        self.data['value']['safi'] = [self.val_num(1)]
        self.data['value']['safi'].append(SAFI_T[self.data['value']['safi'][0]])
        self.data['value']['number'] = self.val_num(1)
        self.data['value']['entries'] = []
        for _ in range(self.data['value']['number']):
            entry = collections.OrderedDict()
            entry['type'] = [self.val_num(1)]
            entry['type'].append(ORF_T[entry['type'][0]])
            entry['send_receive'] = [self.val_num(1)]
            entry['send_receive'].append(ORF_T[entry['send_receive'][0]])
            self.data['entries'].append(entry)

    def unpack_graceful_restart(self):
        '''
        Decoder for Graceful Restart Capability.
        '''
        self.data['value'] = collections.OrderedDict()
        n = self.val_num(2)
        self.data['value']['flags'] = n & 0xf000
        self.data['value']['seconds'] = n & 0x0fff
        self.data['value']['entries'] = []
        cap_len = self.data['length']
        while cap_len > 2:
            entry = collections.OrderedDict()
            entry['afi'] = [self.val_num(2)]
            entry['afi'].append(AFI_T[entry['afi'][0]])
            entry['safi'] = [self.val_num(1)]
            entry['safi'].append(SAFI_T[entry['safi'][0]])
            entry['flags'] = self.val_num(1)
            self.data['value']['entries'].append(entry)
            cap_len -= 4

    def unpack_support_as4(self):
        '''
        Decoder for Support for 4-octet AS number capability.
        '''
        self.data['value'] = self.val_as(4)

    def unpack_add_path(self):
        '''
        Decoder for ADD-PATH Capability
        '''
        self.data['value'] = []
        cap_len = self.data['length']
        while cap_len > 2:
            entry = collections.OrderedDict()
            entry['afi'] = [self.val_num(2)]
            entry['afi'].append(AFI_T[entry['afi'][0]])
            entry['safi'] = [self.val_num(1)]
            entry['safi'].append(SAFI_T[entry['safi'][0]])
            entry['send_receive'] = [self.val_num(1)]
            entry['send_receive'].append(
                ADD_PATH_SEND_RECV[entry['send_receive'][0]]
            )
            self.data['value'].append(entry)
            cap_len -= 4

class BgpAttr(Base):
    '''
    Class for BGP path attributes
    '''
    __slots__ = []

    def __init__(self, buf):
        Base.__init__(self)
        self.buf = buf

    def unpack(self):
        '''
        Decoder for BGP path attributes
        '''
        self.data['flag'] = self.val_num(1)
        self.data['type'] = [self.val_num(1)]
        self.data['type'].append(BGP_ATTR_T[self.data['type'][0]])

        if self.data['flag'] & 0x01 << 4:
            self.data['length'] = self.val_num(2)
        else:
            self.data['length'] = self.val_num(1)

        if self.data['type'][0] == BGP_ATTR_T['ORIGIN']:
            self.unpack_origin()
        elif self.data['type'][0] == BGP_ATTR_T['AS_PATH']:
            self.unpack_as_path()
        elif self.data['type'][0] == BGP_ATTR_T['NEXT_HOP']:
            self.unpack_next_hop()
        elif self.data['type'][0] == BGP_ATTR_T['MULTI_EXIT_DISC']:
            self.unpack_multi_exit_disc()
        elif self.data['type'][0] == BGP_ATTR_T['LOCAL_PREF']:
            self.unpack_local_pref()
        elif self.data['type'][0] == BGP_ATTR_T['AGGREGATOR']:
            self.unpack_aggregator()
        elif self.data['type'][0] == BGP_ATTR_T['COMMUNITY']:
            self.unpack_community()
        elif self.data['type'][0] == BGP_ATTR_T['ORIGINATOR_ID']:
            self.unpack_originator_id()
        elif self.data['type'][0] == BGP_ATTR_T['CLUSTER_LIST']:
            self.unpack_cluster_list()
        elif self.data['type'][0] == BGP_ATTR_T['MP_REACH_NLRI']:
            self.unpack_mp_reach_nlri()
        elif self.data['type'][0] == BGP_ATTR_T['MP_UNREACH_NLRI']:
            self.unpack_mp_unreach_nlri()
        elif self.data['type'][0] == BGP_ATTR_T['EXTENDED COMMUNITIES']:
            self.unpack_extended_communities()
        elif self.data['type'][0] == BGP_ATTR_T['AS4_PATH']:
            self.unpack_as4_path()
        elif self.data['type'][0] == BGP_ATTR_T['AS4_AGGREGATOR']:
            self.unpack_as4_aggregator()
        elif self.data['type'][0] == BGP_ATTR_T['AIGP']:
            self.unpack_aigp()
        elif self.data['type'][0] == BGP_ATTR_T['ATTR_SET']:
            self.unpack_attr_set()
        elif self.data['type'][0] == BGP_ATTR_T['LARGE_COMMUNITY']:
            self.unpack_large_community()
        else:
            if self.data['length']:
                self.data['value'] = self.val_bytes(self.data['length'])
            else:
                self.data['value'] = ''
        return self.p

    def unpack_origin(self):
        '''
        Decoder for ORIGIN attribute
        '''
        self.data['value'] = self.val_num(1)

    def unpack_as_path(self):
        '''
        Decoder for AS_PATH attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = []
        while self.p < attr_len:
            path_seg = collections.OrderedDict()
            path_seg['type'] = [self.val_num(1)]
            path_seg['type'].append(AS_PATH_SEG_T[path_seg['type'][0]])
            path_seg['length'] = self.val_num(1)
            path_seg['value'] = []
            for _ in range(path_seg['length']):
                path_seg['value'].append(self.val_as(self.as_len))
            self.data['value'].append(path_seg)

    def unpack_next_hop(self):
        '''
        Decoder for NEXT_HOP attribute
        '''
        if self.data['length'] == 4:
            self.data['value'] = self.val_addr(AFI_T['IPv4'])
        elif self.data['length'] == 16:
            self.data['value'] = self.val_addr(AFI_T['IPv6'])
        else:
            self.p += self.data['length']
            self.data['value'] = None

    def unpack_multi_exit_disc(self):
        '''
        Decoder for MULTI_EXIT_DISC attribute
        '''
        self.data['value'] = self.val_num(4)

    def unpack_local_pref(self):
        '''
        Decoder for LOCAL_PREF attribute
        '''
        self.data['value'] = self.val_num(4)

    def unpack_aggregator(self):
        '''
        Decoder for AGGREGATOR attribute
        '''
        self.data['value'] = collections.OrderedDict()
        n = 2 if self.data['length'] < 8 else 4
        self.data['value']['as'] = self.val_as(n)
        self.data['value']['id'] = self.val_addr(AFI_T['IPv4'])

    def unpack_community(self):
        '''
        Decoder for COMMUNITY attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = []
        while self.p < attr_len:
            val = self.val_num(4)
            self.data['value'].append(
                '%d:%d' % ((val & 0xffff0000) >> 16, val & 0x0000ffff)
            )

    def unpack_originator_id(self):
        '''
        Decoder for ORIGINATOR_ID attribute
        '''
        self.data['value'] = self.val_addr(AFI_T['IPv4'])

    def unpack_cluster_list(self):
        '''
        Decoder for CLUSTER_LIST attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = []
        while self.p < attr_len:
            self.data['value'].append(self.val_addr(AFI_T['IPv4']))

    def unpack_mp_reach_nlri(self):
        '''
        Decoder for MP_REACH_NLRI attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = collections.OrderedDict()
        self.data['value']['afi'] = [self.val_num(2)]
        self.data['value']['afi'].append(AFI_T[self.data['value']['afi'][0]])

        if self.data['value']['afi'][1] != 'Unknown':
            self.af_num_afi = self.data['value']['afi'][0]
            self.af_num_safi = self.val_num(1)
            self.data['value']['safi'] = [self.af_num_safi, SAFI_T[self.af_num_safi]]
            self.data['value']['next_hop_length'] = self.val_num(1)

            if self.af_num_afi != AFI_T['IPv4'] and self.af_num_afi != AFI_T['IPv6']:
                self.p = attr_len
                return

            if self.af_num_safi != SAFI_T['UNICAST'] \
                and self.af_num_safi != SAFI_T['MULTICAST'] \
                and self.af_num_safi != SAFI_T['L3VPN_UNICAST'] \
                and self.af_num_safi != SAFI_T['L3VPN_MULTICAST']:
                self.p = attr_len
                return

            if self.af_num_safi == SAFI_T['L3VPN_UNICAST'] \
                or self.af_num_safi == SAFI_T['L3VPN_MULTICAST']:
                self.data['value']['route_distinguisher'] = self.val_rd()

        #
        # RFC6396
        # 4.3.4. RIB Entries:
        # There is one exception to the encoding of BGP attributes for the BGP
        # MP_REACH_NLRI attribute (BGP Type Code 14) [RFC4760].  Since the AFI,
        # SAFI, and NLRI information is already encoded in the RIB Entry Header
        # or RIB_GENERIC Entry Header, only the Next Hop Address Length and
        # Next Hop Address fields are included.  The Reserved field is omitted.
        # The attribute length is also adjusted to reflect only the length of
        # the Next Hop Address Length and Next Hop Address fields.
        #
        else:
            self.p -= 2
            self.data['value'] = collections.OrderedDict()
            self.data['value']['next_hop_length'] = self.val_num(1)

        self.data['value']['next_hop'] = [self.val_addr(self.af_num_afi)]

        # RFC2545
        if self.data['value']['next_hop_length'] == 32 \
            and self.af_num_afi == AFI_T['IPv6']:
            self.data['value']['next_hop'].append(self.val_addr(self.af_num_afi))

        if 'afi' in self.data['value']:
            self.data['value']['reserved'] = self.val_num(1)
            self.data['value']['nlri'] \
                = self.val_nlri(attr_len, self.af_num_afi, self.af_num_safi)

    def unpack_mp_unreach_nlri(self):
        '''
        Decoder for MP_UNREACH_NLRI attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = collections.OrderedDict()
        self.data['value']['afi'] = [self.val_num(2)]
        self.data['value']['afi'].append(AFI_T[self.data['value']['afi'][0]])
        self.data['value']['safi'] = [self.val_num(1)]
        self.data['value']['safi'].append(SAFI_T[self.data['value']['safi'][0]])

        if self.data['value']['afi'][0] != AFI_T['IPv4'] \
            and self.data['value']['afi'][0] != AFI_T['IPv6']:
            self.p = attr_len
            return

        if self.data['value']['safi'][0] != SAFI_T['UNICAST'] \
            and self.data['value']['safi'][0] != SAFI_T['MULTICAST'] \
            and self.data['value']['safi'][0] != SAFI_T['L3VPN_UNICAST'] \
            and self.data['value']['safi'][0] != SAFI_T['L3VPN_MULTICAST']:
            self.p = attr_len
            return

        self.data['value']['withdrawn_routes'] = self.val_nlri(
            attr_len,
            self.data['value']['afi'][0],
            self.data['value']['safi'][0]
        )

    def unpack_extended_communities(self):
        '''
        Decoder for EXT_COMMUNITIES attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = []
        while self.p < attr_len:
            ext_comm = self.val_num(8)
            self.data['value'].append(ext_comm)

    def unpack_as4_path(self):
        '''
        Decoder for AS4_PATH attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = []
        while self.p < attr_len:
            path_seg = collections.OrderedDict()
            path_seg['type'] = [self.val_num(1)]
            path_seg['type'].append(AS_PATH_SEG_T[path_seg['type'][0]])
            path_seg['length'] = self.val_num(1)
            path_seg['value'] = []
            for _ in range(path_seg['length']):
                path_seg['value'].append(self.val_as(4))
            self.data['value'].append(path_seg)

    def unpack_as4_aggregator(self):
        '''
        Decoder for AS4_AGGREGATOR attribute
        '''
        self.data['value'] = collections.OrderedDict()
        self.data['value']['as'] = self.val_as(4)
        self.data['value']['id'] = self.val_addr(AFI_T['IPv4'])

    def unpack_aigp(self):
        '''
        Decoder for AIGP attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = []
        while self.p < attr_len:
            aigp = collections.OrderedDict()
            aigp['type'] = self.val_num(1)
            aigp['length'] = self.val_num(2)
            aigp['value'] = self.val_num(aigp['length'] - 3)
            self.data['value'].append(aigp)

    def unpack_attr_set(self):
        '''
        Decoder for ATTR_SET attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = collections.OrderedDict()
        self.data['value']['origin_as'] = self.val_as(4)
        attr_len -= 4
        self.data['value']['path_attributes'] = []
        while self.p < attr_len:
            attr = BgpAttr(self.buf[self.p:])
            self.p += attr.unpack()
            self.data['value']['path_attributes'].append(attr.data)

    def unpack_large_community(self):
        '''
        Decoder for LARGE_COMMUNITY attribute
        '''
        attr_len = self.p + self.data['length']
        self.data['value'] = []
        while self.p < attr_len:
            global_admin = self.val_num(4)
            local_data_part_1 = self.val_num(4)
            local_data_part_2 = self.val_num(4)
            self.data['value'].append(
                '%d:%d:%d'
                % (global_admin, local_data_part_1, local_data_part_2)
            )
