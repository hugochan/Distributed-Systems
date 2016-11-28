from os import _exit as osExit
from socket import socket as Socket, error as SocketError, AF_INET, SOCK_STREAM
from sys import argv, stdin, stdout
from threading import Event, RLock, Thread, Timer
from time import sleep
import Queue

from config import *
from p2_common import Peer, exitCmds, opening, readConfigFile, parseCmd
from zookeeper import ZooKeeper
########

class NetworkBaseLayer:

    def __init__( self, localPeer, remotePeerDict ):

        self.localPeer = localPeer
        self.remotePeerDict = remotePeerDict

        localPeer.network = self
        for idx, peer in self.remotePeerDict.iteritems( ):
            peer.network = self

        self.prettyPrintLn( opening )

    def prettyPrint( self ): stdout.write( 'peer #{} >>> '.format( self.localPeer.idx ) )

    def prettyPrintLn( self, text ): stdout.write( '{}\npeer #{} >>> '.format( text, self.localPeer.idx ) )

    def start( self ):

        Thread( target = self.readStdIn ).start( )
        Thread( target = self.listen ).start( )
        for idx, peer in self.remotePeerDict.iteritems( ):
            Thread( target = self.connect, args = ( peer, ) ).start( )

    def readStdIn( self ):

        while True:
            if stdin.readline( ).strip( ).lower( ) in exitCmds: break
            self.prettyPrint( )
        osExit( 1 )

    def listen( self ):

        listenSocket = Socket( AF_INET, SOCK_STREAM )
        listenSocket.bind( self.localPeer.addr )
        listenSocket.listen( len( self.remotePeerDict ) )
        while True:
            socket, addr = listenSocket.accept( )
            msg = socket.recv( 6 )
            if msg[ : 4 ] == 'PEER':
                idx = int( msg[ 4 : ] )
                self.remotePeerDict[ idx ].onConnect( socket )
            elif msg == 'CLIENT':
                idx = 'client'
                addr = None
                client = Peer( idx, addr )
                client.network = self
                client.onConnect( socket )

    def connect( self, remotePeer ):

        socket = Socket( AF_INET, SOCK_STREAM )
        msg = 'PEER{}'.format( str( self.localPeer.idx ).rjust( 2, '0' ) )
        try:
            while remotePeer.socket == None:
                socket.connect( remotePeer.addr )
                socket.send( msg )
                remotePeer.onConnect( socket )
                sleep( 2 ) # if connection mistakenly closed by remote peer, repeat
                           # i.e. if remotr peer did not detect local peer's crash
                           # and denied local peer's connection attempt on recovery
        except SocketError:
            pass # remotePeer down

########

class Network( NetworkBaseLayer ):

    def __init__( self, localPeer, remotePeerDict ):

        NetworkBaseLayer.__init__( self, localPeer, remotePeerDict )
        self.peersUp = [ localPeer ]
        self.quorum = ( len( remotePeerDict ) + 1 ) / 2 + 1
        self.lock = RLock( )
        self.phase = None
        self.enterPhase( 0 )
        self.leader = None
        self.electionMgr = ElectionMgr( self )
        self.zookeeper = None
        self.user_event_queue = Queue.Queue(maxsize=0)

    def onConnect( self, remotePeer ):

        self.lock.acquire( )
        if remotePeer.idx != 'client':
            self.prettyPrintLn( 'connected to peer #{}'.format( remotePeer.idx ) )
            self.peersUp.append( remotePeer )
            self.electionMgr.onConnect( remotePeer )
        self.lock.release( )

    def onDisconnect( self, remotePeer ):

        self.lock.acquire( )
        if remotePeer.idx != 'client':
            self.prettyPrintLn( 'disconnected from peer #{}'.format( remotePeer.idx ) )
            self.peersUp.remove( remotePeer )
            self.electionMgr.onDisconnect( remotePeer )
        self.lock.release( )

    def onRecv( self, remotePeer, msg ):

        self.lock.acquire( )
        if remotePeer.idx == 'client':
            self.prettyPrintLn( 'from client received "{}"'.format( msg ) )
            ####
            ## TO DO
            ###
            cmd = parseCmd( msg )
            # drop unrecognized cmd
            if cmd:
                if cmd[ 0 ].lower() == READ_OP:
                    if self.zookeeper.blocked:
                        msg = 'The system is blocked, please wait a moment'
                    else:
                        content = self.zookeeper.filesys.read( cmd[ 1 ] )
                        msg = content if content != False else 'The file does not exist'
                    self.send( remotePeer, msg )
                else:
                    self.user_event_queue.put( cmd )
            # self.send( remotePeer, msg ) # for now, just replying with what was sent
        else:
            self.prettyPrintLn( 'from peer #{} received {}'.format( remotePeer.idx, msg ) )
            # dispatch msgs
            if msg[ 0 ] in [SYNCHREQ, SYNCHACK, SYNCHCOM, BCASTREQ, BROADCAST, BCASTACK, BCASTCOM]:
                self.zookeeper.onRecv( remotePeer, msg )
            elif msg[ 0 ] in [ELECTION, ELECTOK, COORDINATOR]:
                self.electionMgr.onRecv( remotePeer, msg )

        self.lock.release( )

    def send( self, remotePeer, msg ):

        self.lock.acquire( )
        if remotePeer.idx == 'client':
            self.prettyPrintLn( 'to client sending "{}"'.format( msg ) )
        else:
            self.prettyPrintLn( 'to peer #{} sending {}'.format( remotePeer.idx, msg ) )
        remotePeer.write( msg )
        self.lock.release( )

    def broadcast( self, msg ):

        self.lock.acquire( )
        self.prettyPrintLn( 'broadcasting {} to #{}'.format( msg,  [x.idx for x in self.peersUp if x != self.localPeer]) )
        for peer in self.peersUp:
            if peer != self.localPeer:
                peer.write( msg )
        self.lock.release( )

    def enterPhase( self, phase ):

        self.lock.acquire( )
        if phase != self.phase:
            self.prettyPrintLn( 'entering phase {}'.format( phase ) )
            self.phase = phase
        self.lock.release( )

########

class ElectionMgr:
    # A server's `process id` is the largest zxid in a server's
    timeout = 3

    def __init__( self, network ):

        self.network = network
        self.droppedOut = False
        self.timerIdx = 0

    def onConnect( self, remotePeer ):

        # if self.network.phase == 0 and not self.droppedOut and len( self.network.peersUp ) >= self.network.quorum:
        if self.network.phase == 0 and len( self.network.peersUp ) >= self.network.quorum:
            msg = [ ELECTION ]
            if len( self.network.peersUp ) == self.network.quorum and not self.droppedOut:
                self.network.broadcast( msg )
            else:
                self.network.send( remotePeer, msg )
            self.timerIdx += 1
            Timer( ElectionMgr.timeout, self.onTimeout, [ self.timerIdx ] ).start( )

    def onDisconnect( self, remotePeer ):

        if self.network.phase == 0 and len( self.network.peersUp ) < self.network.quorum:
            self.droppedOut = False
        elif self.network.phase > 0 and ( len( self.network.peersUp ) < self.network.quorum or remotePeer == self.network.leader ):
            self.network.leader = None
            self.droppedOut = False
            self.network.enterPhase( 0 )
            self.network.zookeeper.blocked = True # system is blocked
            if len( self.network.peersUp ) >= self.network.quorum:
                msg = [ ELECTION ]
                self.network.broadcast( msg )
                self.timerIdx += 1
                Timer( ElectionMgr.timeout, self.onTimeout, [ self.timerIdx ] ).start( )

    def onRecv( self, remotePeer, msg ):

        if self.network.phase == 0:
            if msg[ 0 ] == ELECTION and remotePeer.idx < self.network.localPeer.idx:
                msg = [ ELECTOK ]
                self.network.send( remotePeer, msg )
            elif len( self.network.peersUp ) >= self.network.quorum:
                if msg[ 0 ] == ELECTOK:
                    self.droppedOut = True
                elif msg[ 0 ] == COORDINATOR:
                    self.network.leader = remotePeer
                    self.network.enterPhase( 1 )
                    self.network.enterPhase( 2 )
        else:
            if msg[ 0 ] == ELECTION:
                msg = [ ELECTOK ]
                self.network.send( remotePeer, msg )
                if self.network.localPeer == self.network.leader:
                    msg = [ COORDINATOR ]
                    self.network.send( remotePeer, msg )
                    # Leader sends its history to the new follower to bring the  follower up to date
                    self.network.send( remotePeer, [SYNCHCOM] )

    def onTimeout( self, timerIdx ):

        self.network.lock.acquire( )
        if not self.droppedOut and timerIdx == self.timerIdx: # must be latest timer
            msg = [ COORDINATOR ]
            self.network.broadcast( msg )
            self.network.leader = self.network.localPeer
            self.network.enterPhase( 1 )
            Thread( target = self.network.zookeeper.prepare ).start( ) # call zookeeper prepare
        self.network.lock.release( )

########

usage = 'Usage: python p2_server.py <localPeerIdx>'

def main( ):

    if not ( len( argv ) == 2 and argv[ 1 ].isdigit( ) ):
        print usage
        return

    localPeerIdx = int( argv[ 1 ] )
    localPeer, remotePeerDict = readConfigFile( localPeerIdx )

    network = Network( localPeer, remotePeerDict )
    network.start( )
    ZooKeeper( network ).start( )
    Event( ).wait( )

########

main( )
