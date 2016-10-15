import re
import json
import socket
import select
import threading
import Queue
from collections import defaultdict
from socket import socket as Socket, AF_INET, SOCK_STREAM
from sys import argv, stdin, stdout, stderr
from threading import RLock
from time import sleep

create_usage_line = 'Usage: create <filename>: creates an empty file named <filename>.'
delete_usage_line = 'Usage: delete <filename>: deletes file named <filename>.'
read_usage_line = 'Usage: read <filename>: displays the contents of <filename>.'
append_usage_line = 'Usage: append <filename> <line>: appends a <line> to <filename>.'
exit_usage_line = 'Usage: exit: exits the shell.'
usage_line = 'Usage: python run_node.py <tree file> <node idx>'
usage_args = 3
tree_file_line_re = re.compile(r'^\((?P<idx1>\d+),(?P<idx2>\d+)\)$', re.M)
config_file_line_re = re.compile(r'^(?P<idx>\d+) (?P<ip>.*?):(?P<port>\d+)$', re.M)


CREATE_OP = 'create'
DELETE_OP = 'delete'
READ_OP = 'read'
APPEND_OP = 'append'

INIT_TOKEN = 'init_token'
ASSIGN_TOKEN = 'assign_token'
DELETE_TOKEN = 'delete_token'
APPEND_TOKEN = 'append_token'
REQ_TOKEN = 'request_token'

msg_size = 4096
retry_delay = 5


class Node:  # idx, ip, port, socket
    def __init__(self, idx=None):
        self.idx = idx
    def __repr__(self):
        return '{} {}:{}'.format(self.idx, self.ip, self.port)

class State:
    def __init__(self, idx, holder_idx):
        self.idx = idx
        self.holder_idx = holder_idx
        self.using_res = False
        self.req_q = Queue.Queue(maxsize=0)
        self.asked = False

class UserState:
    def __init__(self, op, token):
        self.op = op
        self.token = token
        self.req = False
        self.grant = False
        self.release = False
        self.content = ''

class File:
    def __init__(self, name):
        self.__name = name
        self.__content = ''

    def read(self):
        return self.__content

    def append(self, line):
        self.__content += line

class FileSystem:
    def __init__(self):
        self.__files = dict() # token -> File

    def exist(self, token):
        return token in self.__files

    def display_tokens(self):
        return self.__files.keys()

    def create(self, token):
        if token in self.__files:
            return False
        else:
            self.__files[token] = File(token)
            return True

    def delete(self, token):
        try:
            del self.__files[token]
            return True
        except KeyError as e:
            print e
            return False

    def read(self, token):
        try:
            return self.__files[token].read()
        except KeyError as e:
            print e
            return False

    def append(self, token, line):
        try:
            self.__files[token].append(line)
            return True
        except KeyError as e:
            print e
            return False

lock = RLock()
user_state_lock = RLock()
# in_msg_lock = RLock()
# out_msg_lock = RLock()

node = Node()
neighbors = dict()  # idx -> node
user_state = UserState(None, None)
file_system = FileSystem()
token_thread_pool = dict() # token -> thread

# Global signal
shutdown_signal = False # Signal threads to finish as soon as possible

# Socket select
inputs = [] # Sockets from which we expect to read
outputs = [] # Sockets to which we expect to write
out_msg_queues = {} # Outgoing message queues (socket:Queue)
in_msg_queues_init = {} # Incoming message queues (socket:Queue)
in_msg_queues = defaultdict(dict) # Incoming message queues (socket:dict)
stdin_thread = None
sockets = None


def main():
    if init():
        connect_all()
        main_loops()

def run():
    if init():
        connect_all()
    else:
        return False

    for _, node in neighbors.iteritems():
        inputs.append(node.socket)
        outputs.append(node.socket)
        out_msg_queues[node.socket] = Queue.Queue(maxsize=0)
        in_msg_queues_init[node.socket] = Queue.Queue(maxsize=0)
        in_msg_queues[node.socket] = {}

    global stdin_thread
    stdin_thread = threading.Thread(target=stdin_repl)
    stdin_thread.start()
    main_loop()

def init():

    if len(argv) != usage_args:
        print 'Incorrect number of args'
        print usage_line
        return False

    tree_file_path = argv[1]
    try:
        tree_file = open(tree_file_path, 'r')
        tree_text = tree_file.read()
        tree_file.close()
    except IOError as e:
        print 'Incorrect arg "{}": {}'.format(tree_file_path, e.strerror)
        print usage_line
        return False

    idx = argv[2]
    if idx.isdigit():
        node.idx = int(idx)
    else:
        print 'Incorrect arg "{}": Expected integer'.format(argv[2])
        print usage_line
        return False

    for m in tree_file_line_re.finditer(tree_text):
        idx1 = int(m.group('idx1'))
        idx2 = int(m.group('idx2'))
        if idx1 == node.idx:
            neighbors[idx2] = Node(idx2)
        elif idx2 == node.idx:
            neighbors[idx1] = Node(idx1)

    config_file = open('config.txt', 'r')
    config_text = config_file.read()
    config_file.close()

    for m in config_file_line_re.finditer(config_text.replace('\r','')):
        idx = int(m.group('idx'))
        ip = m.group('ip')
        port = int(m.group('port'))
        if idx == node.idx:
            node.ip = ip
            node.port = port
        elif idx in neighbors:
            neighbors[idx].ip = ip
            neighbors[idx].port = port

    node_print('initialized as {}:{}'.format(node.ip, node.port))
    return True

def connect_all():

    node_print('connecting to neighbors {}'.format(neighbors))

    server_thread = threading.Thread(target=run_server)
    connect_thread = threading.Thread(target=connect)

    server_thread.start()
    connect_thread.start()

    server_thread.join()
    connect_thread.join()

    node_print('ready')

def run_server():

    client_idxs = [ idx for idx in neighbors if idx > node.idx ]

    server_socket = Socket(AF_INET, SOCK_STREAM)
    ip = '0.0.0.0'
    server_socket.bind(( ip, node.port ))
    server_socket.listen(5)

    while client_idxs:
        node_print(client_idxs)
        client_socket, client_addr = server_socket.accept()
        print "anything"
        msg = client_socket.recv(msg_size)

        if msg.isdigit():
            idx = int(msg)
            if idx in client_idxs:
                neighbors[idx].socket = client_socket
                client_idxs.remove(idx)
                node_print('connected to node #{}'.format(idx))
                continue

        client_socket.close()

    server_socket.close()

def connect():

    server_idxs = [ idx for idx in neighbors if idx < node.idx ]

    for idx in server_idxs:

        neighbors[idx].socket = Socket(AF_INET, SOCK_STREAM)

        while 1:

            try:
                print neighbors[idx].ip, neighbors[idx].port
                neighbors[idx].socket.connect(( neighbors[idx].ip , neighbors[idx].port ))
                break
            except socket.error:
                node_print('waiting for node #{}'.format(idx))
                sleep(retry_delay)

        neighbors[idx].socket.send(str(node.idx))
        node_print('connected to node #{}'.format(idx))

def main_loops():
    neighbor_read_thread_pool = []
    for idx in neighbors:
        neighbor_read_thread = threading.Thread(target=neighbor_read_loop, args=( idx, ))
        neighbor_read_thread.start()
        neighbor_read_thread_pool.append(neighbor_read_thread)

    stdin_read_loop()

    for neighbor_read_thread in neighbor_read_thread_pool:
        neighbor_read_thread.join()

    # Close all sockets
    for i in neighbors:
        neighbors[i].socket.close()

def neighbor_read_loop(idx):

    other_sockets = [ neighbors[i].socket for i in neighbors if i != idx ]

    while 1:
        msg = neighbors[idx].socket.recv(msg_size)
        node_print('node #{} sends {}'.format(idx, msg))
        if msg.strip('\r\n') == 'bye':
            break
        for s in other_sockets:
            s.send(msg)

def stdin_read_loop():
    sockets = [ neighbors[idx].socket for idx in neighbors ]

    while 1:
        msg = stdin.readline().strip()
        for s in sockets:
            s.send(msg)
        if msg.strip('\r\n') == 'bye':
            break

def main_loop():
    timeout = 1 # Number of seconds to wait before breaking off monitoring if no channels have become active
    global sockets
    sockets = [neighbors[idx].socket for idx in neighbors]
    for s in sockets:
        s.setblocking(0)

    while 1:
        if shutdown_signal:
            destructor()
            break

        if user_state.op == CREATE_OP and user_state.req and not user_state.grant:
            # broadcast init token event
            msg = pack_msg(node.idx, INIT_TOKEN, user_state.token) # send init-token msg
            for s in sockets:
                out_msg_queues[s].put(msg)

            thread = threading.Thread(target=raymond_alg_loop, args=(user_state.token, node.idx))
            thread.start()
            token_thread_pool[user_state.token] = thread
            user_state_lock.acquire()
            user_state.grant = True
            user_state_lock.release()

        # check init-token msgs from other nodes
        for s in list(in_msg_queues_init):
            try:
                msg = in_msg_queues_init[s].get_nowait()
            except Queue.Empty:
                pass
            else:
                msg = json.loads(msg)
                if msg['type'] == INIT_TOKEN:
                    # Note that here we assume that file names are unique
                    ret = file_system.create(msg['token'])
                    if ret:
                        print '\n[System info]: File {} was created by someone else'.format(msg['token'])
                        print_input_sign()
                    else:
                        print '\n[System info]: Failed to sync the file {}'.format(msg['token'])
                        print_input_sign()
                    thread = threading.Thread(target=raymond_alg_loop, args=(msg['token'], msg['sender']))
                    thread.start()
                    token_thread_pool[msg['token']] = thread
                    # broadcast init token event
                    new_msg = pack_msg(node.idx, INIT_TOKEN, msg['token']) # send init-token msg
                    for other_socket in sockets:
                        if other_socket == s:
                            continue
                        out_msg_queues[other_socket].put(new_msg)

        ## Check messages
        readable, writable, exceptional = select.select(inputs, outputs, inputs, timeout)
        # Handle inputs
        for s in readable:
            data = s.recv(msg_size)
            if data:
                # in_msg_lock.acquire()
                msg = json.loads(data)
                if msg['type'] == INIT_TOKEN:
                    in_msg_queues_init[s].put(data)
                else:
                    try:
                        in_msg_queues[s][msg['token']].put(data)
                    except:
                        in_msg_queues[s][msg['token']] = Queue.Queue(maxsize=0)
                        in_msg_queues[s][msg['token']].put(data)
                # in_msg_lock.release()
            else:
                # Interpret empty result as closed connection
                # Stop listening for input on the connection
                print 'closing connection to Node #{} after reading no data'.format(search_idx_by_socket(neighbors, s))
                if s in outputs:
                    outputs.remove(s)
                inputs.remove(s)
                sockets.remove(s)
                # out_msg_lock.acquire()
                del out_msg_queues[s]
                # out_msg_lock.release()
                # in_msg_lock.acquire()
                del in_msg_queues_init[s]
                del in_msg_queues[s]
                # in_msg_lock.release()
                s.close()

        # Handle outputs
        for s in writable:
            try:
                # out_msg_lock.acquire()
                if not s in out_msg_queues:
                    continue
                next_msg = out_msg_queues[s].get_nowait()
                # out_msg_lock.release()
            except Queue.Empty:
                # no messages waiting
                pass
            else:
                s.send(next_msg)

        # Handle exceptions
        for s in exceptional:
            print stderr, 'closing connection to Node #{} for exceptional condition'.format(search_idx_by_socket(neighbors, s))
            # Stop listening for input on the connection
            inputs.remove(s)
            sockets.remove(s)
            if s in outputs:
                outputs.remove(s)
            del out_msg_queues[s]
            del in_msg_queues_init[s]
            del in_msg_queues[s]
            s.close()

def raymond_alg_loop(token, holder_idx):
    """Raymond's algorithm"""
    state = State(node.idx, holder_idx)
    while 1:
        if shutdown_signal and state.req_q.empty():
            break

        # Get the token from others and then use the resource
        if state.using_res and not user_state.grant and user_state.token == token and \
            (user_state.op == DELETE_OP or user_state.op == READ_OP or user_state.op == APPEND_OP):
            user_state.grant = True # Grant the access to token
            if user_state.op == APPEND_OP:
                append_token(token, user_state.content)
            elif user_state.op == DELETE_OP:
                delete_token(token)
                while not user_state.release:
                    pass
                user_state.release = False
                return

        # To request a resource
        if user_state.req and not user_state.grant and user_state.token == token and \
            (user_state.op == DELETE_OP or user_state.op == READ_OP or user_state.op == APPEND_OP):
            state = req_resource(token, state)
            user_state.req = False
            if state.using_res:
                user_state.grant = True # Grant the access to token
                if user_state.op == APPEND_OP:
                    append_token(token, user_state.content)
                elif user_state.op == DELETE_OP:
                    delete_token(token)
                    while not user_state.release:
                        pass
                    user_state.release = False
                    return

        # To release a resource
        if user_state.release and user_state.token == token:
            state = release_resource(token, state)
            user_state.release = False # Reset release flag

        # On receipt of REQ from neighbor
        # Check create msgs from other nodes
        for s in list(in_msg_queues):
            for t in list(in_msg_queues[s]):
                if not t == token:
                    continue
                try:
                    msg = in_msg_queues[s][t].get_nowait()
                except Queue.Empty:
                    pass
                else:
                    msg = json.loads(msg)
                    if msg['type'] == REQ_TOKEN:
                        nbr = search_idx_by_socket(neighbors, s)
                        if nbr == -1:
                            continue
                        state = recv_req(token, state, nbr)
                    elif msg['type'] == ASSIGN_TOKEN:
                        state = recv_token(token, state, search_idx_by_socket(neighbors, s))
                    elif msg['type'] == APPEND_TOKEN:
                        append_token(token, msg['content'], [s])
                        file_system.append(token, msg['content'])
                        print '\n[System info]: File {} was appended by someone else'.format(token)
                        print_input_sign()
                    elif msg['type'] == DELETE_TOKEN:
                        # del in_msg_queues
                        delete_token(token, [s])
                        file_system.delete(token) # delete local backup of the resource
                        print '\n[System info]: File {} was deleted by someone else'.format(token)
                        print_input_sign()
                        # in case that user is requiring this resource
                        if user_state.token == token and (user_state.op == DELETE_OP or \
                                user_state.op == READ_OP or user_state.op == APPEND_OP):
                            user_state.grant = True
                            while not user_state.release:
                                pass
                            user_state.release = False
                        return

def assign_token(token, state):
    if state.holder_idx == node.idx and not state.using_res and not state.req_q.empty():
        state.holder_idx = state.req_q.get_nowait()
        state.asked = False
        if state.holder_idx == node.idx:
            state.using_res = True
        else:
            # Send token to holder
            msg = pack_msg(node.idx, ASSIGN_TOKEN, token)
            out_msg_queues[neighbors[state.holder_idx].socket].put(msg)
            print '[Debug info]: send the token {} to Node#{}'.format(token, state.holder_idx)
    return state

def send_req(token, state):
    if not state.holder_idx == node.idx and not state.req_q.empty() and not state.asked:
        # Send req to holder
        msg = pack_msg(node.idx, REQ_TOKEN, token)
        out_msg_queues[neighbors[state.holder_idx].socket].put(msg)
        state.asked = True
    return state

def req_resource(token, state):
    # To request a resource
    # Add self to reqQ
    state.req_q.put(node.idx)
    state = assign_token(token, state)
    state = send_req(token, state)
    return state

def release_resource(token, state):
    # To release a resource
    state.using_res = False
    state = assign_token(token, state)
    state = send_req(token, state)
    return state

def recv_req(token, state, nbr):
    # On receipt of REQ from neighbor
    # Add neighbor to reqQ
    state.req_q.put(nbr)
    state = assign_token(token, state)
    state = send_req(token, state)
    return state

def recv_token(token, state, from_idx):
    # On receipt of token
    state.holder_idx = node.idx
    state = assign_token(token, state)
    state = send_req(token, state)
    print '[Debug info]: recv the token {} from Node#{}'.format(token, from_idx)
    return state

def delete_token(token, filter_list=[]):
    msg = pack_msg(node.idx, DELETE_TOKEN, token)
    for other_socket in sockets:
        if other_socket in filter_list:
            continue
        out_msg_queues[other_socket].put(msg)

def append_token(token, line, filter_list=[]):
    msg = pack_msg(node.idx, APPEND_TOKEN, token, line)
    for other_socket in sockets:
        if other_socket in filter_list:
            continue
        out_msg_queues[other_socket].put(msg)

def stdin_repl():
    print
    print '##########################'
    print '# Distributed File System#'
    print '##########################'
    usage_print()

    while 1:
        raw_user_input = raw_input('Node #{}> '.format(node.idx)).strip()
        if raw_user_input == '':
            continue
        if raw_user_input.lower() == 'exit':
            global shutdown_signal
            shutdown_signal = True
            print 'bye~'
            break
        user_input = raw_user_input.split()

        ## create <filename>
        if user_input[0].lower() == CREATE_OP:
            if not len(user_input) == 2:
                print create_usage_line
                continue
            if file_system.exist(user_input[1]):
                print '{}: File has alread been created'.format(raw_user_input)
                continue
            # Start a user request
            set_user_state(user_state, CREATE_OP, user_input[1])

            #################
            # Raymond's algorithm
            #################


            # Do until permitted
            while not user_state.grant:
                pass

            if file_system.exist(user_input[1]):
                print '{}: File has alread been created'.format(raw_user_input)
            else:
                ret = file_system.create(user_input[1])
                print 'File was created' if ret else 'Failed to create the file'

            # Release resource
            user_state.release = True
            while user_state.release:
                pass
            # print 'released read'
            # Reset the user state
            reset_user_state(user_state)


        ## delete <filename>
        elif user_input[0].lower() == DELETE_OP:
            if not len(user_input) == 2:
                print delete_usage_line
                continue
            if not file_system.exist(user_input[1]):
                print '{}: No such file'.format(raw_user_input)
                continue
            # Start a user request
            set_user_state(user_state, DELETE_OP, user_input[1])

            #################
            # Raymond's algorithm
            #################

            # Do until permitted
            while not user_state.grant:
                pass

            if not file_system.exist(user_input[1]):
                print '{}: No such file'.format(raw_user_input)
            else:
                ret = file_system.delete(user_input[1])
                print 'File was deleted' if ret else 'Failed to delete the file'

            # Release resource
            user_state.release = True
            while user_state.release:
                pass
            # print 'released delete'
            # Reset the user state
            reset_user_state(user_state)


        ## read <filename>
        elif user_input[0].lower() == READ_OP:
            if not len(user_input) == 2:
                print read_usage_line
                continue
            if not file_system.exist(user_input[1]):
                print '{}: No such file'.format(raw_user_input)
                continue
            # Start a user request
            set_user_state(user_state, READ_OP, user_input[1])


            #################
            # Raymond's algorithm
            #################

            # Do until permitted
            while not user_state.grant:
                pass

            if not file_system.exist(user_input[1]):
                print '{}: No such file'.format(raw_user_input)
            else:
                ret = file_system.read(user_input[1])
                print ret if not ret == False else 'Failed to read the file'

            # Release resource
            user_state.release = True
            while user_state.release:
                pass
            # print 'released read'
            # reset the user state
            reset_user_state(user_state)


        ## append <filename> <line>
        elif user_input[0].lower() == APPEND_OP:
            if len(user_input) < 3:
                print append_usage_line
                continue
            if not file_system.exist(user_input[1]):
                print '{}: No such file'.format(raw_user_input)
                continue
            line = ' '.join(user_input[2:])
            # Start a user request
            set_user_state(user_state, APPEND_OP, user_input[1], line)


            #################
            # Raymond's algorithm
            #################

            # Do until permitted
            while not user_state.grant:
                pass

            if not file_system.exist(user_input[1]):
                print '{}: No such file'.format(raw_user_input)
            else:
                ret = file_system.append(user_input[1], line)
                print 'Line was appended' if ret else 'Failed to append the line'

            # Release resource
            user_state.release = True
            while user_state.release:
                pass
            # print 'released append'
            # Reset the user state
            reset_user_state(user_state)
        else:
            print '{}: Command not found'.format(user_input[0])
            usage_print()
            continue


def destructor():
    stdin_thread.join()
    for _, thread in token_thread_pool.iteritems():
        thread.join()

    # Close all sockets
    for i in neighbors:
        try:
            neighbors[i].socket.close()
        except:
            pass

def set_user_state(ustate, op, token, content=''):
    user_state_lock.acquire()
    ustate.token = token
    ustate.op = op
    ustate.req = True
    if content:
        ustate.content = content
    user_state_lock.release()

def reset_user_state(ustate):
    user_state_lock.acquire()
    ustate.op = None
    ustate.token = None
    ustate.req = False
    ustate.grant = False
    ustate.release = False
    ustate.content = ''
    user_state_lock.release()

def node_print(text):

    lock.acquire()
    print 'Node #{}: {}'.format(node.idx, text)
    lock.release()

def usage_print():
    print
    print create_usage_line
    print delete_usage_line
    print read_usage_line
    print append_usage_line
    print exit_usage_line
    print

def search_idx_by_socket(d, s):
    for idx, val in d.items():
        if val.socket == s:
            return idx
    return -1

def pack_msg(sender, msg_type, token, content=''):
    msg = dict()
    msg['sender'] = sender
    msg['type'] = msg_type
    msg['token'] = token
    if content:
        msg['content'] = content
    return json.dumps(msg)

# def check_msg_empty(msg_queues, type_):
#     if type_ == 'in':
#         for s in msg_queues:
#             if not msg_queues[s][INIT].empty() or not msg_queues[s][OTHERS].empty():
#                 return False
#         return True
#     elif type_ == 'out':
#         for s in msg_queues:
#             if not msg_queues[s].empty():
#                 return False
#         return True
#     else:
#         return True

def print_input_sign():
    stdout.write('Node #{}> '.format(node.idx))
    stdout.flush()

# main()
run()
