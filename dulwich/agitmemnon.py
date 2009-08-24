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
        self.revtree = {}
        
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
        o = self.get_object(name)
        data = ''
        otype = ''
        for col in o:
            if col.name == 'data':
                data = col.value
            if col.name == 'type':
                otype = col.value
        data = zlib.decompress(base64.b64decode(data))
        return ShaFile.from_raw_string(type_num_map[otype], data)

    def get_revtree_objects(self, sha):
        # check for entry in revtree cache
        # if it's not there, pull another chunk, check there, loop
        # return all the objects included in that commit
        if sha in self.revtree:
            return self.revtree[sha]
        else:
            self.load_next_revtree_hunk()
            if sha in self.revtree:
                return self.revtree[sha]
            else:
                return False
    
    def load_next_revtree_hunk(self):
        if len(self.revtree) > 0: # hack
            return False 
        o = self.get_super('RevTree', self.repo_name, 100000)
        nilsha = '0000000000000000000000000000000000000000'
        for col in o:
            print col.name
            self.revtree[col.name] = []
            for sup in col.columns:
                objects = sup.value.split(":")
                if nilsha in objects:
                    objects.remove(nilsha)
                if '' in objects:
                    objects.remove('')
                self.revtree[col.name].extend(objects)

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
        return iter(AgitMissingObjectFinder(self, haves, wants, progress).next, None)

    def iter_shas(self, shas):
        """Iterate over the objects for the specified shas."""
        return ObjectStoreIterator(self, shas)

    def fetch_objects(self, determine_wants, graph_walker, progress):
        wants = determine_wants(self.get_refs())
        haves = self.find_common_revisions(graph_walker)
        return self.iter_shas(self.find_missing_objects(haves, wants, progress))

    def partial_sender(self, objects, f, entries):
        # PackCacheIndex (projectname) [(cache_key) => (list of objects/offset/size), ...]
                
        objs = {}
        for sha, path in objects.itershas():
            objs[sha] = true
        
        # parse cache_index entries, figure out what we need to pull
        # (which caches have enough objects that we need)
        # "sha:offset:size:base_sha\n"
        
        # pull each partial cache and send all the objects that are needed
        # cache = self.get('PackCache', cache_key)

        # add each sent object to the sent[] array to return
        # return the sent[] array

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

class AgitMissingObjectFinder(object):
    """Find the objects missing from another object store.

    :param object_store: Object store containing at least all objects to be 
        sent
    :param haves: SHA1s of commits not to send (already present in target)
    :param wants: SHA1s of commits to send
    :param progress: Optional function to report progress to.
    """

    def __init__(self, object_store, haves, wants, progress=None):
        self.sha_done = set(haves)
        self.objects_to_send = set([w for w in wants if w not in haves])
        self.object_store = object_store
        if progress is None:
            self.progress = lambda x: None
        else:
            self.progress = progress

    def add_todo(self, entries):
        self.objects_to_send.update([e for e in entries if not e in self.sha_done])

    def next(self):
        if not self.objects_to_send:
            return None
        sha = self.objects_to_send.pop()
        obs = self.object_store.get_revtree_objects(sha)
        if obs:
            self.add_todo(obs)
        self.sha_done.add(sha)
        self.progress("counting objects: %d\r" % len(self.sha_done))
        return (sha, sha) # sorry, hack
        
class AgitmemnonBackend(Backend):

    def __init__(self):
        self.repo = Agitmemnon()
        self.fetch_objects = self.repo.fetch_objects
        self.get_refs = self.repo.get_refs
        self.set_args = self.repo.set_args
        self.partial_sender = self.repo.partial_sender


#a = Agitmemnon()
#a.repo_name = 'fuzed2'
#a.load_next_revtree_hunk()
#print a.revtree

#print a.get_object('7486f4075d2b9307d02e3905c69e28e456a51a32')[0].value
#print a['7486f4075d2b9307d02e3905c69e28e456a51a32'].get_parents()
#print a.get_object('7486f4075d2b9307d02e3905c69e28e456a51a32')
