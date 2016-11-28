import os
import json
import Queue
from time import sleep
from threading import Thread

from config import *
from filesys import History


class ZooKeeper:

    def __init__( self, network ):

        self.epoch = -1
        self.counter = 0
        self.filesys = None
        self.network = network
        self.network.zookeeper = self
        self.history = History( os.path.join( history_loc, 'history_%s.txt' % self.network.localPeer.idx ) )
        self.synack = set( )
        self.bcastack = set( )
        self.msgList = []
        self.next = None
        self.blocked = True
        self.abcast_queue = Queue.Queue(maxsize=0)

    def start( self ):

        Thread( target = self.run_abcast ).start( ) # call zookeeper run_abcast

        while True:
            if self.network.phase == 3:
                msg = self.consume_event( self.network.user_event_queue )
                if not msg:
                    sleep( 1 )
                    continue

                if self.is_leader( self.network.localPeer ):
                    self.abcast_queue.put( msg )
                else:
                    self.network.send( self.network.leader, [ BCASTREQ, msg ] )

    def run_abcast( self ):

        while True:
            if self.network.phase == 3 and self.is_leader( self.network.localPeer ):
                msg = self.consume_event( self.abcast_queue )
                if not msg:
                    sleep( 1 )
                    continue

                self.abcast( msg )
                self.deliver( ( self.epoch, self.counter ), msg )

    def prepare( self ):

        self.blocked = True
        # phase 0: Leader Elechon
        # decide which server is the leader
        # this is a built-in function of the Network layer

        while self.network.phase < 1:
            sleep( 1 )

        # check if is a leader
        if self.is_leader( self.network.localPeer ):
            # phase 1: Discovery
            # the leader learns the most up-to-date state (history)
            self.filesys = self.load_history( )

            self.network.enterPhase( 2 )
            # phase 2: Synchronizahon
            # the leader brings all followers' histories up to date
            self.synch_history( self.history.lookup_history( ) )
            self.network.enterPhase( 3 )
            self._increment_epoch( )
            self.blocked = False

    def synch_history( self, history ):

        self.network.broadcast( [ SYNCHREQ ] )
        while self.synack < set( [ x.idx for x in self.network.peersUp if x != self.network.localPeer ] )\
            or len( self.network.peersUp ) < self.network.quorum:
            sleep( 1 )
        self.network.broadcast( [ SYNCHCOM, history ] )
        self.synack = set( )

    def abcast( self, msg ):

        self.network.broadcast( [ BROADCAST, ( self.epoch, self.counter ) ] )
        while self.bcastack < set( [ x.idx for x in self.network.peersUp if x != self.network.localPeer ] )\
            or len( self.network.peersUp ) < self.network.quorum:
            # print 'self.bcastack %s' % self.bcastack
            sleep( 1 )
        self.network.broadcast( [ BCASTCOM, ( self.epoch, self.counter ), msg ] )
        self.bcastack = set( )
        self._increment_counter( ) # counter is incremented between each proposal

    def load_history( self ):

        return self.history.recover_filesys( )

    def is_leader( self, peer ):

        return self.network.leader == peer

    def onRecv( self, remotePeer, msg ):

        if self.network.phase == 2:
            if self.is_leader( remotePeer ):
                if msg[ 0 ] == SYNCHREQ:
                    msg = [ SYNCHACK ]
                    self.network.send( remotePeer, msg )
                elif msg[ 0 ] == SYNCHCOM:
                    self.history.rewrite_history( msg[ 1 ] if len( msg ) > 1 else [ ] )
                    self.history.dump( ) # save to disk
                    self.filesys = self.history.recover_filesys( )
                    self.network.enterPhase( 3 )
                    self.blocked = False
            elif self.is_leader( self.network.localPeer ):
                if msg[ 0 ] == SYNCHACK:
                    self.synack.add( remotePeer.idx )

        elif self.network.phase == 3:
            if self.is_leader( self.network.localPeer ):
                if msg[ 0 ] == BCASTREQ:
                    self.abcast_queue.put( msg[ 1 ] )
                elif msg[ 0 ] == BCASTACK:
                    self.bcastack.add( remotePeer.idx )

            elif self.is_leader( remotePeer ):
                if msg[ 0 ] == BROADCAST:
                    self.network.send( remotePeer, [ BCASTACK ] )
                elif msg[ 0 ] == BCASTCOM:
                    self.deliver( msg[ 1 ] , msg[ 2 ])
                    self._update_zxid( msg[ 1 ][ 0 ], msg[ 1 ][ 1 ] )

    def consume_event( self, queue ):

        try:
            msg = queue.get_nowait( )
        except Queue.Empty:
            return None
        else:
            return msg

    def deliver( self, zxid, msg ):
        # for now, just simply do as follows
        self.msgList.append( ( zxid, msg ) )
        _, msgs = zip(*sorted(self.msgList, cmp=self._sort_zxid))
        for each in msgs:
            self.history.append( each )
            self.history.dump( )
            filesys = self.history.op(self.filesys, each)
            if filesys:
                self.filesys = filesys
            # print 'deliver transaction: %s' % each
            # print 'filesys: %s' % self.filesys
        self.msgList = []

    def _update_zxid( self, epoch, counter ):

        self.epoch = max( self.epoch, epoch )
        self.counter = max( self.counter, counter )

    def _increment_epoch( self ):

        self.epoch += 1
        self.counter = 0

    def _increment_counter( self ):

        self.counter += 1

    def _sort_zxid( self, x, y ):
        if x[ 0 ][ 0 ] < y[ 0 ][ 0 ]:
            return 1
        elif x[ 0 ][ 0 ] > y[ 0 ][ 0 ]:
            return -1
        else:
            if x[ 0 ][ 1 ] < y[ 0 ][ 1 ]:
                return 1
            elif x[ 0 ][ 1 ] > y[ 0 ][ 1 ]:
                return -1
            else:
                return 0

