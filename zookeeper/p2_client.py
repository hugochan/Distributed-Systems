from os import _exit as osExit
from socket import socket as Socket, error as SocketError, AF_INET, SOCK_STREAM
from sys import argv, stdin, stdout
from threading import Event, Thread

from config import *
from p2_common import Peer, exitCmds, opening, readConfigFile, parseCmd
########

class Network( ):

    def __init__( self, remotePeer ):

        self.remotePeer = remotePeer
        remotePeer.network = self

        self.prettyPrintLn( opening )

    def prettyPrint( self ): stdout.write( 'client >>> ' )

    def prettyPrintLn( self, text ): stdout.write( '{}\nclient >>> '.format( text ) )

    def start( self ):

        Thread( target = self.readStdIn ).start( )

        socket = Socket( AF_INET, SOCK_STREAM )
        msg = 'CLIENT'
        try:
            socket.connect( self.remotePeer.addr )
            socket.send( msg )
            self.remotePeer.onConnect( socket )
        except SocketError:
            print 'could not connect to server'.format( msg )
            osExit( 1 )

    def readStdIn( self ):

        while True:
            cmd = stdin.readline( ).strip( )
            if cmd.lower() in exitCmds: break
            # parse the cmd
            parse_cmd = parseCmd( cmd )
            if parse_cmd:
                self.prettyPrintLn( 'sending "{}"'.format( ' '.join( parse_cmd ) ) )
                self.remotePeer.write( ' '.join( parse_cmd ) )
            else:
                self.prettyPrintLn( '{}: command not found'.format( cmd ) )
            self.prettyPrint( )

        osExit( 1 )

    def onConnect( self, remotePeer ):

        pass

    def onDisconnect( self, remotePeer ):

        print 'disconnected from server'
        osExit( 1 )

    def onRecv( self, remotePeer, msg ):

        self.prettyPrintLn( msg )


########

usage = 'Usage: python p2_client.py <remotePeerIdx>'

def main( ):

    if not ( len( argv ) == 2 and argv[ 1 ].isdigit( ) ):
        print usage
        return

    remotePeerIdx = int( argv[ 1 ] )
    remotePeer, _ = readConfigFile( remotePeerIdx )

    Network( remotePeer ).start( )
    Event( ).wait( )

########

main( )
