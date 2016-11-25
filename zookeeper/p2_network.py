import re

from Queue import Queue
from socket import socket as Socket, error as SocketError, AF_INET, SOCK_STREAM, SHUT_RDWR
from threading import Event, Lock, Thread
from time import sleep


node_config_re = re.compile( r'^(?P<idx>\d+) (?P<ip>.*?):(?P<port>\d+)$', re.M )
connect_lock = Lock( )


class Node:

    def __init__( self, idx, addr ):

        self.idx = idx
        self.addr = addr
        self.socket = None

    def __repr__( self ):

        return '{} {} {}'.format( self.idx, self.addr, 'UP' if self.socket else 'DOWN' )


class NetworkBase:

    def __init__( self, local_node_idx, config_filepath ):

        self.local_node_idx = local_node_idx

        config_file = open( config_filepath, 'r' )
        config_data = config_file.read( )
        config_file.close( )

        self.remote_node_dict = { }

        for m in node_config_re.finditer( config_data.replace('\r','') ):

            idx = int( m.group( 'idx' ) )
            ip = m.group( 'ip' )
            port = int( m.group( 'port' ) )
            addr = ( ip, port )

            if idx == local_node_idx:
                self.local_node_addr = addr
            else:
                node = Node( idx, addr )
                self.remote_node_dict[ idx ] = node
                print node

        self.event_queue = Queue( )
        Thread( target = self.handle_event_loop ).start( )

        self.listen_socket = Socket( AF_INET, SOCK_STREAM )
        self.listen_socket.bind( self.local_node_addr )
        self.listen_socket.listen( 5 )

        for idx in self.remote_node_dict:
            if idx > local_node_idx:
                self.notify_client( self.remote_node_dict[ idx ] )

        Thread( target = self.listen_for_clients ).start( )

        for idx in self.remote_node_dict:
            if idx < local_node_idx:
                Thread( target = self.connect_to_server, args = ( self.remote_node_dict[ idx ], ) ).start( )

        Event( ).wait( )

    def handle_event_loop( self ):

        while True:

            target, args = self.event_queue.get( )

            if target == self.send:

                remote_node_idxs = args[ 0 ]
                msg = args[ 1 ]
                self.on_recv( remote_node_idxs, msg )

            elif target == self.on_connect:

                remote_node_idx = args[ 0 ]
                Thread( target = self.recv_loop, args = ( self.remote_node_dict[ remote_node_idx ], ) ).start( )
                self.on_connect( remote_node_idx )

            elif target == self.on_disconnect:

                remote_node_idx = args[ 0 ]
                self.remote_node_dict[ remote_node_idx ].socket = None
                self.on_disconnect( remote_node_idx )

            elif target == self.on_recv:

                remote_node_idx = args[ 0 ]
                msg = args[ 1 ]
                self.on_recv( remote_node_idx, msg )

    # triggered on startup/recovery or on notification from server
    # checks if already connected to proceed (should connect once, if both triggers received simultaneously)
    def connect_to_server( self, server ):

        connect_lock.acquire( )

        if server.socket != None:
            connect_lock.release( )
            return

        connect_socket = Socket( AF_INET, SOCK_STREAM )
        try:
            connect_socket.connect( server.addr )
            connect_socket.send( str( self.local_node_idx ) )
            server.socket = connect_socket
            self.event_queue.put( ( self.on_connect, [ server.idx ] ) )
        except SocketError:
            pass

        connect_lock.release( )

    # scenario:
    # local node fails and restarts
    # remote client did not detect failure, receives notification and does not connect because it thinks it's already connected
    # thus local node repeatedly sends notification until remote client connects
    def notify_client( self, client ):
        notify_socket = Socket( AF_INET, SOCK_STREAM )
        try:
            while client.socket == None:
                notify_socket.connect( client.addr )
                notify_socket.send( str( self.local_node_idx ) )
                sleep( 1 )
        except SocketError:
            pass

    def listen_for_clients( self ):

        while True:

            client_socket, client_addr = self.listen_socket.accept( )
            msg = client_socket.recv( 10 )
            client_idx = int( msg )
            client = self.remote_node_dict[ client_idx ]

            if client_idx < self.local_node_idx:
                client_socket.shutdown( SHUT_RDWR )
                client_socket.close( )
                Thread( target = self.connect_to_server, args = ( client, ) ).start( )
            else:
                client.socket = client_socket
                self.event_queue.put( ( self.on_connect, [ client_idx ] ) )

    def recv_loop( self, remote_node ):

        while True:

            try:

                msg = remote_node.socket.recv( 4096 )
                if len(msg) > 0:
                    self.event_queue.put( ( self.on_recv, [ remote_node.idx, msg ] ) )
                    continue

            except SocketError:

                pass

            self.event_queue.put( ( self.on_disconnect, [ remote_node.idx ] ) )
            break

    def send( self, remote_node_idxs, msg ):
        pass

    def on_connect( self, remote_node_idx ): pass
    def on_disconnect( self, remote_node_idx ): pass
    def on_recv( self, remote_node_idx, msg ): pass

