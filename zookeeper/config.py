import re

configFilePath = 'toy-config.txt'
history_loc = 'backup/'
configRE = re.compile( r'^(?P<idx>\d+) (?P<ip>.*?):(?P<port>\d+)$', re.M )


CREATE_OP = 'create'
DELETE_OP = 'delete'
APPEND_OP = 'append'
READ_OP = 'read'

ELECTION = 'ELECTION'
ELECTOK = 'ELECTOK'
COORDINATOR = 'COORDINATOR'

SYNCHREQ = 'SYNCHREQ'
SYNCHACK = 'SYNCHACK'
SYNCHCOM = 'SYNCHCOM'

BCASTREQ = 'BCASTREQ'
BROADCAST = 'BROADCAST'
BCASTACK = 'BCASTACK'
BCASTCOM = 'BCASTCOM'


create_usage_line = 'Usage: create <filename>: creates an empty file named <filename>.'
delete_usage_line = 'Usage: delete <filename>: deletes file named <filename>.'
read_usage_line = 'Usage: read <filename>: displays the contents of <filename>.'
append_usage_line = 'Usage: append <filename> <line>: appends a <line> to <filename>.'
exit_usage_line = 'Usage: exit: exits the shell.'
usage_line = 'Usage: python p2.py <local node idx>'
