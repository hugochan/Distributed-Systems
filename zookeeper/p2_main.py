from p2_network import NetworkBase
from sys import argv


usage_line = 'Usage: python p2.py <local node idx>'
config_filepath = 'toy-config.txt'


class Network( NetworkBase ):

    def on_connect( self, remote_node_idx ):
        print '{} : {} connected'.format( self.local_node_idx, remote_node_idx )

    def on_disconnect( self, remote_node_idx ):
        print '{} : {} disconnected'.format( self.local_node_idx, remote_node_idx )

    def on_recv( self, remote_node_idx, msg ):
        print '{} : {} sends {}'.format( self.local_node_idx, remote_node_idx, msg )


def main( ):

    if len( argv ) != 2 or not argv[ 1 ].isdigit( ):
        print usage_line
        return

    local_node_idx = int( argv[ 1 ] )

    network = Network( local_node_idx, config_filepath )


main( )

