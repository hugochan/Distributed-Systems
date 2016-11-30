import os

from socket import socket as Socket, error as SocketError, AF_INET, SOCK_STREAM
from sys import argv, stdin, stdout
from threading import Event, Thread

from p2_common import *

########

class Network( ):

    def __init__( self, remotePeer ):

        self.remotePeer = remotePeer
        remotePeer.network = self
        self.block = Event( )

    def start( self ):

        Thread( target = self.readStdIn ).start( )
        
        socket = Socket( AF_INET, SOCK_STREAM )
        try:
            socket.connect( self.remotePeer.addr )
            socket.send( 'CLIENT' )
            self.remotePeer.onConnect( socket )
        except SocketError:
            print 'failed to connect to server'
            os._exit( 1 )

    def readStdIn( self ):

        while True:
            cmd = stdin.readline( ).strip( )
            if cmd.lower( ) in exitCmds: break
            prettyPrint( )
            cmd2 = parseCmd( cmd )
            if cmd2:
                if len( cmd2 ) > 1:
                    self.remotePeer.write( cmd )
                    self.block.clear( )
                    self.block.wait( )
                else:
                    prettyPrintLn( usageDict[ cmd2[ 0 ] ] )
            else:
                prettyPrintLn( 'command not valid' )
        os._exit( 1 )

    def onConnect( self, remotePeer ):

        pass

    def onDisconnect( self, remotePeer ):

        print 'disconnected from server'
        os._exit( 1 )

    def onRecv( self, remotePeer, msg ):

        self.block.set( )
        prettyPrintLn( 'server says "{}"'.format( msg ) )

########

def prettyPrint( ): stdout.write( 'client >>> ' )

def prettyPrintLn( text ): stdout.write( '{}\nclient >>> '.format( text ) )

########

usage = 'Usage: python p2_client.py <remotePeerIdx>'

def main( ):

    if not ( len( argv ) == 2 and argv[ 1 ].isdigit( ) ):
        print usage
        return

    remotePeerIdx = int( argv[ 1 ] )
    remotePeer, _ = readConfigFile( remotePeerIdx )
    network = Network( remotePeer )
    
    prettyPrintLn( opening )
    network.start( )
    Event( ).wait( )

########

main( )
