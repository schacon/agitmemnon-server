"""Cassandra Agitmemnon Client and Daemon Backend"""

import sys
import pprint
import zlib
import base64

from urlparse import urlparse

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

from cassandra import Cassandra
from cassandra.ttypes import *

from dulwich.server import (
    Backend,
    )
from dulwich.object_store import *
from dulwich.objects import *

type_num_map = {
    BLOB_ID: 3,
    TREE_ID : 2,
    COMMIT_ID : 1,
    TAG_ID: 4,
}

class Agitmemnon(BaseObjectStore):
    """Object store that keeps all objects in cassandra."""
    def __init__(self):
        host = '127.0.0.1'
        port = 9160
        self.keyspace = 'Agitmemnon'
        self.memcache = {}

        socket = TSocket.TSocket(host, port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Cassandra.Client(protocol)
        transport.open()

    def get(self, column, key, count=100, consistency_level=1):
        start = ''
        finish = ''
        is_ascending = True
        result = self.client.get_slice(self.keyspace, key, ColumnParent(column),
                                start, finish, is_ascending, 
                                count, consistency_level)
        return result

    def get_value(self, column_family, key, column, consistency_level=1):
        try:
            result = self.client.get_column(self.keyspace, key, 
                                ColumnPath(column_family, None, column), consistency_level)
            return result.value
        except:
            return False

    def get_super(self, column, key, count=100, consistency_level=1):
        start = ''
        finish = ''
        is_ascending = True
        result = self.client.get_slice_super(self.keyspace, key, column, 
                                start, finish, is_ascending, 
                                count, consistency_level)
        return result

    def get_object(self, sha):
        return self.get('Objects', sha)

    def get_object_value(self, sha, column):
        return self.get_value('Objects', sha, column)

    def __contains__(self, sha):
        """Check if the object with a particular SHA is present."""
        if self.get_object_value(sha, 'size'):
            return True
        else:
            return False

    # TODO: look for packfiles of objects, cache all items in packfile, pull from caches
    def __getitem__(self, name):
        if name in self.memcache:
            return self.memcache[name]
        o = self.get_object(name)
        data = ''
        otype = ''
        for col in o:
            if col.name == 'data':
                data = col.value
            if col.name == 'type':
                otype = col.value
        data = zlib.decompress(base64.b64decode(data))
        shafile = ShaFile.from_raw_string(type_num_map[otype], data)
        if otype != BLOB_ID: # caching commit/tree/tag objects since they are hit twice
            self.memcache[name] = shafile
        return shafile


    def find_common_revisions(self, graphwalker):
        """Find which revisions this store has in common using graphwalker."""
        haves = []
        sha = graphwalker.next()
        while sha:
            if sha in self:
                haves.append(sha)
                graphwalker.ack(sha)
            sha = graphwalker.next()
        return haves

    def find_missing_objects(self, haves, wants, progress=None):
        return iter(MissingObjectFinder(self, haves, wants, progress).next, None)

    def iter_shas(self, shas):
        """Iterate over the objects for the specified shas."""
        return ObjectStoreIterator(self, shas)

    def fetch_objects(self, determine_wants, graph_walker, progress):
        wants = determine_wants(self.get_refs())
        haves = self.find_common_revisions(graph_walker)
        return self.iter_shas(self.find_missing_objects(haves, wants, progress))

    def get_refs(self):
        """Get dictionary with all refs."""
        print self.repo_name
        ret = {}
        refs = self.get_super('Repositories', self.repo_name)
        for x in refs:
            for col in x.columns:
                if len(col.value) == 40:
                    ret['refs/' + x.name + '/' + col.name] = col.value
                    if x.name == 'heads' and col.name == 'master':
                        ret['HEAD'] = col.value
        return ret

    def set_args(self, args):
        rname = args[0]
        if rname[0] == '/':
            rname = rname[1:len(rname)]
        if rname.endswith('.git'):
            rname = rname.replace('.git','')
        self.repo_name = rname

class AgitmemnonBackend(Backend):

    def __init__(self):
        self.repo = Agitmemnon()
        self.fetch_objects = self.repo.fetch_objects
        self.get_refs = self.repo.get_refs
        self.set_args = self.repo.set_args


#a = Agitmemnon()
#print a.get_object('7486f4075d2b9307d02e3905c69e28e456a51a32')[0].value
#print a['7486f4075d2b9307d02e3905c69e28e456a51a32'].get_parents()
#print a.get_object('7486f4075d2b9307d02e3905c69e28e456a51a32')
