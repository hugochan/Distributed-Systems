import json
import os

from socket import socket as Socket, error as SocketError, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, SHUT_RDWR
from sys import argv, stdin, stdout
from threading import Event, RLock, Thread, Timer
from time import sleep

from p2_common import *

########

listening = False
listenSocket = None

class Network:

    def __init__( self, localPeer, remotePeerDict ):

        self.localPeer = localPeer
        self.remotePeerDict = remotePeerDict
        localPeer.network = self
        for idx, peer in self.remotePeerDict.iteritems( ): peer.network = self

    def start( self ):

        Thread( target = self.readStdIn ).start( )
        Thread( target = self.listen ).start( )
        for idx, peer in self.remotePeerDict.iteritems( ): Thread( target = self.connect, args = ( peer, ) ).start( )

    def readStdIn( self ):

        while True:
            if stdin.readline( ).strip( ).lower( ) in exitCmds: break
            prettyPrint( )
        if listening:
            listenSocket.shutdown( SHUT_RDWR )
            listenSocket.close( )
            #sleep( 1 )
        os._exit( 1 )

    def listen( self ):

        global listening, listenSocket

        listenSocket = Socket( AF_INET, SOCK_STREAM )
        listenSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        listenSocket.bind( ( '0.0.0.0', self.localPeer.addr[ 1 ] ) )
        listening = True
        listenSocket.listen( len( self.remotePeerDict ) )
        try:
            while True:
                socket, addr = listenSocket.accept( )
                msg = socket.recv( 6 )
                if msg[ : 4 ] == 'PEER':
                    idx = int( msg[ 4 : ] )
                    self.remotePeerDict[ idx ].onConnect( socket )
                elif msg == 'CLIENT':
                    client = Peer( 'client', None )
                    client.network = self
                    client.onConnect( socket )
        except SocketError:
            pass

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

class Zookeeper( Network ):

    def __init__( self, localPeer, remotePeerDict ):

        Network.__init__( self, localPeer, remotePeerDict )
        self.peersUp = [ localPeer ]
        self.quorum = ( len( remotePeerDict ) + 1 ) / 2 + 1
        self.lock = RLock( )
        self.epoch = 0
        self.phase = None
        self.timerIdx = 0
        self.peerReqs = [ ]
        self.clientReqs = [ ]
        
    def start( self ):
        
        self.enterPhase( 0 )        
        Network.start( self )

    def onConnect( self, remotePeer ):

        self.lock.acquire( )
        if remotePeer.idx != 'client':
            prettyPrintLn( 'connected to peer #{}'.format( remotePeer.idx ) )
            self.peersUp.append( remotePeer )

            if self.phase == 0 and not self.droppedOut and len( self.peersUp ) >= self.quorum:
                msg = [ ELECTION, persistState.getLastAcceptZxid( ) ]
                if len( self.peersUp ) == self.quorum:
                    self.broadcast( msg )
                else:
                    self.send( remotePeer, msg )
                self.timerIdx += 1
                Timer( electionTimeout, self.onTimeout, [ self.timerIdx ] ).start( )

        self.lock.release( )

    def onDisconnect( self, remotePeer ):

        self.lock.acquire( )
        if remotePeer.idx != 'client':
            prettyPrintLn( 'disconnected from peer #{}'.format( remotePeer.idx ) )
            self.peersUp.remove( remotePeer )
            
            if len( self.peersUp ) < self.quorum:
                if self.phase == 0:                
                    self.droppedOut = False
                elif self.phase > 0:
                    self.enterPhase( 0 )
            elif remotePeer == self.leader:
                self.enterPhase( 0 )                    
                self.broadcast( [ ELECTION, persistState.getLastAcceptZxid( ) ] )
                self.timerIdx += 1
                Timer( electionTimeout, self.onTimeout, [ self.timerIdx ] ).start( )            

        self.lock.release( )

    def onRecv( self, remotePeer, msg ):

        self.lock.acquire( )
        if remotePeer.idx == 'client':
            prettyPrintLn( 'from client received "{}"'.format( msg ) )
            
            if self.phase < 3:
                self.send( remotePeer, 'server is up, but service is down' )
                return
            cmd = parseCmd( msg )            
            if cmd == None or len( cmd ) == 1:
                self.send( remotePeer, '"{}" is not a valid command'.format( msg ) )
                return
            if cmd[ 0 ] == READ_OP:
                if cmd[ 1 ] in fileDict:
                    self.send( remotePeer, fileDict[ cmd[ 1 ] ] )
                else:
                    self.send( remotePeer, '{} does not exist'.format( cmd[ 1 ] ) )
            else:                
                if self.localPeer == self.leader:
                    if cmd[ 0 ] == CREATE_OP and cmd[ 1 ] in fileDict:
                        self.send( remotePeer, '{} already exists'.format( cmd[ 1 ] ) )
                    elif cmd[ 0 ] in [ DELETE_OP, APPEND_OP ] and not cmd[ 1 ] in fileDict:
                        self.send( remotePeer, '{} does not exist'.format( cmd[ 1 ] ) )
                    else:
                        self.clientReqs.append( [ remotePeer, cmd ] )
                        self.peerReqs.append( [ self.localPeer, cmd ] )
                        self.counter += 1
                        zabLayer.abcast( [ [ self.epoch, self.counter ], cmd ] )
                else:
                    self.clientReqs.append( [ remotePeer, cmd ] )
                    self.send( self.leader, [ BCASTREQ, cmd ] )
            
        else:
            prettyPrintLn( 'from peer #{} received {}'.format( remotePeer.idx, msgDict[ msg[ 0 ] ]( msg ) ) )
            
            if msg[ 0 ] == ELECTION:
                msg2 = [ ELECTOK ]
                if self.phase == 0:
                    zxid = persistState.getLastAcceptZxid( )
                    if msg[ 1 ] < zxid or msg[ 1 ] == zxid and remotePeer.idx < self.localPeer.idx:
                        self.send( remotePeer, msg2 )
                elif self.phase > 0:
                    self.send( remotePeer, msg2 )
                    if self.localPeer == self.leader:
                        self.send( remotePeer, [ COORD_SYNCHPROP, [ self.epoch, persistState.history ] ] )   
            elif msg[ 0 ] == ELECTOK:
                if self.phase == 0:
                    self.droppedOut = True
            elif msg[ 0 ] == COORD_SYNCHPROP:
                if self.phase == 0:
                    self.epoch, history = msg[ 1 ]
                    self.leader = remotePeer
                    self.enterPhase( 2 )
                    persistState.updateHistory( history )
                    self.send( remotePeer, [ SYNCHACK, self.epoch ] )
            elif msg[ 0 ] == SYNCHACK:
                msg = [ SYNCHCOM, self.epoch ]
                if self.phase == 2:
                    self.numSynchAcks += 1
                    if self.numSynchAcks + 1 == self.quorum:
                        self.broadcast( msg )
                        self.peerReqs = [ ]
                        self.enterPhase( 3 )
                elif self.phase == 3:
                    self.send( remotePeer, msg )
            elif msg[ 0 ] == SYNCHCOM:
                if self.phase == 2:
                    self.enterPhase( 3 )
            elif msg[ 0 ] == BCASTREQ:
                if self.phase == 3:
                    cmd = msg[ 1 ]
                    if cmd[ 0 ] == CREATE_OP and cmd[ 1 ] in fileDict:
                        self.send( remotePeer, [ BCASTREQREP, '{} already exists'.format( cmd[ 1 ] ) ] )
                    elif cmd[ 0 ] in [ DELETE_OP, APPEND_OP ]  and not cmd[ 1 ] in fileDict:
                        self.send( remotePeer, [ BCASTREQREP, '{} does not exist'.format( cmd[ 1 ] ) ] )
                    else:
                        self.peerReqs.append( [ remotePeer, cmd ] )
                        self.counter += 1
                        zabLayer.abcast( [ [ self.epoch, self.counter], cmd ] )                    
            elif msg[ 0 ] == BCASTPROP:
                if self.phase == 3:
                    zxid, _ = msg[ 1 ]
                    persistState.updateHistory( [ msg[ 1 ] ] )
                    self.send( remotePeer, [ BCASTACK, zxid ] )
            elif msg[ 0 ] == BCASTACK:
                if self.phase == 3:
                    self.numBcastAcks += 1
                    if self.numBcastAcks + 1 == self.quorum:                    
                        self.broadcast( [ BCASTCOM, msg[ 1 ] ] )
                        zabLayer.abdeliver( persistState.history[ -1 ] )
                        peer, cmd = self.peerReqs.pop( 0 )
                        msg = '{} successful'.format( cmd[ 0 ] )
                        if peer == self.localPeer:
                            client, _ = self.clientReqs.pop( 0 )
                            self.send( client, msg )
                        else:
                            self.send( peer, [ BCASTREQREP, msg ] )
            elif msg[ 0 ] == BCASTCOM:
                if self.phase == 3:
                    zabLayer.abdeliver( persistState.history[ -1 ] )
            elif msg[ 0 ] == BCASTREQREP:
                if self.phase == 3:
                    client, cmd = self.clientReqs.pop( 0 )
                    self.send( client, msg[ 1 ] )
            
        self.lock.release( )

    def send( self, remotePeer, msg ):

        self.lock.acquire( )
        if remotePeer.idx == 'client':
            prettyPrintLn( 'to client sending "{}"'.format( msg ) )
        else:
            prettyPrintLn( 'to peer #{} sending {}'.format( remotePeer.idx, msgDict[ msg[ 0 ] ]( msg ) ) )
        remotePeer.write( msg )
        self.lock.release( )

    def broadcast( self, msg ):

        self.lock.acquire( )
        peers = [ peer for peer in self.peersUp if peer != self.localPeer ]
        prettyPrintLn( '( to {} ) broadcasting {}'.format( ', '.join( '#{}'.format( peer.idx ) for peer in peers ), msgDict[ msg[ 0 ] ]( msg ) ) )
        for peer in peers: peer.write( msg )
        self.lock.release( )

    def enterPhase( self, phase ):

        self.lock.acquire( )
        if phase != self.phase:
            prettyPrintLn( 'entering phase {}: {}'.format( phase, phaseDict[ phase ] ) )
            if phase == 0:
                self.leader = None
                self.droppedOut = False
                if self.phase == 3:
                    for client, cmd in self.clientReqs:
                        self.send( client, '{} aborted'.format( cmd[ 0 ] ) )
            elif phase == 3:
                self.clientReqs = [ ]
                for trans in subtractHistory( persistState.history, persistState.lastDelivZxid ):
                    zabLayer.abdeliver( trans )                
            self.phase = phase    
        self.lock.release( )
        
    def onTimeout( self, timerIdx ):
    
        self.lock.acquire( )
        if not self.droppedOut and timerIdx == self.timerIdx: # must be latest timer
            epoch, _ = persistState.getLastAcceptZxid( )
            self.leader = self.localPeer
            self.epoch = max( epoch, self.epoch ) + 1
            self.counter = 0
            self.numSynchAcks = 0
            self.broadcast( [ COORD_SYNCHPROP, [ self.epoch, persistState.history ] ] )
            self.enterPhase( 2 )
        self.lock.release( )    

########

class ZABLayer:

    def abcast( self, trans ):
        
        prettyPrintLn( 'abcasting {}'.format( trans ) )
        zookeeper.numBcastAcks = 0
        zookeeper.broadcast( [ BCASTPROP, trans ] )
        persistState.updateHistory( [ trans ] )        

    def abdeliver( self, trans ):
        
        prettyPrintLn( 'abdelivering {}'.format( trans ) )
        zxid, cmd = trans
        if cmd[ 0 ] == CREATE_OP:
            fileDict[ cmd[ 1 ] ] = ''
        elif cmd[ 0 ] == DELETE_OP:
            del fileDict[ cmd[ 1 ] ]
        elif cmd[0] == APPEND_OP:
            fileDict[ cmd[ 1 ] ] += cmd[ 2 ] 
        persistState.updatelastDelivZxid( zxid )

########

fileDict = { }

class FileSys:

    def __init__ ( self ):
        
        '''
        self.dirPath = '{}/filesys_{}/'.format( storageDirPath, zookeeper.localPeer.idx )
        if not os.path.isdir( self.dirPath ):
            os.makedirs( self.dirPath )
        '''
        pass
            
########

class PersistState:
    
    def __init__( self ):
        
        dumpDirPath = '{}/dump_{}/'.format( storageDirPath, zookeeper.localPeer.idx )
        if not os.path.isdir( dumpDirPath ): os.makedirs( dumpDirPath )
        
        self.historyFilePath = '{}/history.txt'.format( dumpDirPath )
        if os.path.isfile( self.historyFilePath ):
            with open( self.historyFilePath, 'r' ) as f: self.history = [ json.loads( l.strip( ) ) for l in f.readlines( ) ]
        else:
            with open( self.historyFilePath, 'w' ) as f: self.history = [ ]
        prettyPrintLn( 'loaded to history {}'.format( self.history ) )
            
        self.updatelastDelivZxid( [ 0, 0 ] )                
            
    def updateHistory( self, history ):
        
        deltaHistory = subtractHistory( history, self.getLastAcceptZxid( ) )
        prettyPrintLn( 'to history {} adding {}'.format( self.history, deltaHistory ) )        
        with open( self.historyFilePath, 'a' ) as f: f.write( ''.join( '{}\n'.format( json.dumps( trans ) ) for trans in deltaHistory ) )
        self.history = self.history + deltaHistory
        
    def updatelastDelivZxid( self, zxid ):        
        
        self.lastDelivZxid = zxid
        
    def getLastAcceptZxid( self ):
            
        if self.history:
            lastAcceptTrans = self.history[ -1 ]
            lastAcceptZxid = lastAcceptTrans[ 0 ]
        else:
            lastAcceptZxid = [ 0, 0 ]
        return lastAcceptZxid
        
########

def prettyPrint( ): stdout.write( 'peer #{} >>> '.format( zookeeper.localPeer.idx ) )

def prettyPrintLn( text ): stdout.write( '{}\npeer #{} >>> '.format( text, zookeeper.localPeer.idx ) )

########

def subtractHistory( history, zxid ):
        
    startIdx = 0
    for i in reversed( range( len( history ) ) ):
        zxid2, _ = history[ i ]
        if zxid2 <= zxid:
            startIdx = i + 1
            break
    return history[ startIdx : ] 

########

usage = 'Usage: python p2_server.py <localPeerIdx>'

def main( ):

    global zookeeper, persistState, fileSys, zabLayer

    if not ( len( argv ) == 2 and argv[ 1 ].isdigit( ) ):
        print usage
        return

    localPeerIdx = int( argv[ 1 ] )
    localPeer, remotePeerDict = readConfigFile( localPeerIdx )
    zookeeper = Zookeeper( localPeer, remotePeerDict )    
    prettyPrintLn( opening )
    zabLayer = ZABLayer( )
    persistState = PersistState( )
    fileSys = FileSys( )
    zookeeper.start( )
    Event( ).wait( )

########

main( )
