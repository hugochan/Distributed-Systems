import json
import re

from socket import error as SocketError
from threading import RLock, Thread

########

storageDirPath = 'backup'

configFilePath = 'config.txt'
configRE = re.compile( r'^(?P<idx>\d+) (?P<ip>.*?):(?P<port>\d+)$', re.M )

electionTimeout = 3

opening = 'CSCI 6963 Distributed Systems & Algorithms F16 P2. Yu Chen and Rhaad Rabbani.\nType "exit" or "quit" to terminate.'
exitCmds = [ 'exit', 'quit' ]

CREATE_OP = 'create'
DELETE_OP = 'delete'
APPEND_OP = 'append'
READ_OP = 'read'

# phase 0 - 2
ELECTION = 'ELECTION'
ELECTOK = 'ELECTOK'
COORD_SYNCHPROP = 'COORD_SYNCHPROP'
SYNCHACK = 'SYNCHACK'
SYNCHCOM = 'SYNCHCOM'

# phase 3
BCASTREQ = 'BCASTREQ'
BCASTPROP = 'BCASTPROP'
BCASTACK = 'BCASTACK'
BCASTCOM = 'BCASTCOM'
BCASTREQREP = 'BCASTREQREP'

usageDict = { CREATE_OP: 'Usage: create <filename>: creates an empty file named <filename>.',
              DELETE_OP: 'Usage: delete <filename>: deletes file named <filename>.',
              READ_OP: 'Usage: read <filename>: displays the contents of <filename>.',
              APPEND_OP: 'Usage: append <filename> <line>: appends a <line> to <filename>.' }

phaseDict = { 0: 'pre-broadcast, leaderless',
              2: 'pre-broadcast, with leader',
              3: 'broadcast'}

msgDict = { ELECTION: lambda msg: '[ELECTION, zxid={}]'.format( msg[ 1 ] ),
            ELECTOK: lambda msg: '[ELECTOK]',
            COORD_SYNCHPROP: lambda msg: '[COORD_SYNCHPROP, [epoch={}, hist={}]]'.format( msg[ 1 ][ 0 ], msg[ 1 ][ 1 ] ),
            SYNCHACK: lambda msg: '[SYNCHACK, epoch={}]'.format( msg[ 1 ] ),
            SYNCHCOM: lambda msg: '[SYNCHCOM, epoch={}]'.format( msg[ 1 ] ),
            BCASTREQ: lambda msg: '[BCASTREQ, cmd={}]'.format( msg[ 1 ] ),
            BCASTPROP: lambda msg: '[BCASTPROP, [zxid={}, cmd={}]]'.format( msg[ 1 ][ 0 ], msg[ 1 ][ 1 ] ),
            BCASTACK: lambda msg: '[BCASTACK, zxid={}]'.format( msg[ 1 ] ),
            BCASTCOM: lambda msg: '[BCASTCOM, zxid={}]'.format( msg[ 1 ] ),
            BCASTREQREP: lambda msg: '[BCASTREQREP, {}]'.format( msg[ 1 ] ) }

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
            pass # socket will be set to None when read thread fails

########

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

#########
                
def parseCmd( cmd ):
    cmd = re.compile( ' +' ).split( cmd, 2 )
    if cmd:
        cmd[ 0 ] = cmd[ 0 ].lower( )
        if cmd[ 0 ] in [ CREATE_OP, DELETE_OP, READ_OP ] and len( cmd ) == 2  or cmd[ 0 ] == APPEND_OP and len( cmd ) == 3:
            return cmd
        elif cmd[ 0 ] in [ CREATE_OP, DELETE_OP, READ_OP, APPEND_OP ]:
            return cmd[ : 1 ]
    