import json
import re

from socket import error as SocketError
from threading import RLock, Thread
from config import *
########

opening = 'CSCI 6963 Distributed Systems & Algorithms F16 P2. Yu Chen and Rhaad Rabbani.\nType "exit" or "quit" to terminate.'
exitCmds = [ 'exit', 'quit' ]

########

class Peer:

    def __init__( self, idx, addr ):

        self.idx = idx
        self.addr = addr
        self.socket = None
        self.lock = RLock( )
        self.network = None # self.network be set before any method is called
                            # self.network should itself have the following methods:
                            #   onConnect( self, remotePeer )
                            #   onDisconnect( self, remotePeer )
                            #   onRecv( self, remotePeer, msg ):

    # could be called from either listen thread or connect thread
    # potentially simultaneously
    # require lock for atomicity
    def onConnect( self, socket ):

        self.lock.acquire( )
        if self.socket == None:
            self.socket = socket
            Thread( target = self.readSocket ).start( )
            self.network.onConnect( self )
        else:
            socket.close( )
        self.lock.release( )

    def readSocket( self ):
        try:
            while True:
                chunk = self.socket.recv( 6 )
                if not chunk.isdigit( ): raise SocketError( 'error' )
                msgLen = int( chunk )
                msg = [ ]
                while msgLen > 0:
                    chunk = self.socket.recv( min( msgLen, 2048 ) )
                    if len( chunk ) == 0: raise SocketError( 'error' )
                    msg.append( chunk )
                    msgLen -= len(chunk)
                msg = json.loads( ''.join( msg ) )
                self.network.onRecv( self, msg )
        except SocketError:
            self.socket = None
            self.network.onDisconnect( self )

    def write( self, msg ):
        mm = msg
        msg = json.dumps( msg )
        msgLen = len( msg )
        try:
            chunkLen = self.socket.send( str( msgLen ).rjust( 6, '0' ) )
            if chunkLen == 0: return
            while msgLen > 0:
                chunkLen = self.socket.send( msg[ -msgLen : ] )
                if chunkLen == 0: return
                msgLen -= chunkLen
        except SocketError as e:
            print e
            # pass # socket will be set to None when read thread fails

########

# configFilePath = 'config.txt'
# configRE = re.compile( r'^(?P<idx>\d+) (?P<ip>.*?):(?P<port>\d+)$', re.M )

def readConfigFile( localPeerIdx ):

    localPeer = None
    remotePeerDict = { }

    configFile = open( configFilePath, 'r' )
    configData = configFile.read( )
    configFile.close( )

    for m in configRE.finditer( configData.replace('\r','') ):

        idx = int( m.group( 'idx' ) )
        addr = ( m.group( 'ip' ), int( m.group( 'port' ) ) )
        peer = Peer( idx, addr )

        if idx == localPeerIdx:
            localPeer = peer
        else:
            remotePeerDict[ idx ] = peer

    return localPeer, remotePeerDict

def parseCmd( cmd ):
    cmd = cmd.split()
    if cmd and ( cmd[0].lower() in [CREATE_OP, DELETE_OP, READ_OP] and len(cmd) == 2 or\
            cmd[0].lower() == APPEND_OP and len(cmd) == 3 ):
        return cmd
    else:
        return None
