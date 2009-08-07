"""Cassandra Agitmemnon Client and Daemon Backend"""

import sys
import pprint
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
    
class Agitmemnon:
    
    def __init__(self):
        host = '127.0.0.1'
        port = 9160
        self.keyspace = 'Agitmemnon'

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


    def fetch_objects(self, determine_wants, graph_walker, progress):
        """
        Yield the objects required for a list of commits.

        :param progress: is a callback to send progress messages to the client
        """
        print 'fetch'

    def get_refs(self):
        """Get dictionary with all refs."""
        ret = {}
        refs = a.get_super('Repositories', 'fuzed')
        ret['HEAD'] = 'refs/heads/master' # TODO: fix this
        for x in refs:
            for col in x.columns:
                if len(col.value) == 40:
                    ret['refs/' + x.name + '/' + col.name] = col.value
        return ret

class AgitmemnonBackend(Backend):

    def __init__(self):
        self.repo = Agitmemnon()
        self.fetch_objects = self.repo.fetch_objects
        self.get_refs = self.repo.get_refs


a = Agitmemnon()
print a.get_refs()

#print a.get_object('7486f4075d2b9307d02e3905c69e28e456a51a32')
