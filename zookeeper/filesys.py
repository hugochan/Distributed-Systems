import os
import json
from config import *


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
            return False

class History:
    def __init__(self, dump_loc):
        self.history = []
        self.dump_loc = 'tmp/history.txt'

        if isinstance(dump_loc, str) and os.path.exists(os.path.dirname(dump_loc)):
            self.dump_loc = dump_loc
        else:
            try:
                os.makedirs(os.path.dirname(dump_loc))
                self.dump_loc = dump_loc
            except Exception as e:
                print e
                if not os.path.exists(os.path.dirname(self.dump_loc)):
                    os.makedirs(os.path.dirname(self.dump_loc))
        print 'dump history to this location: %s' % self.dump_loc

    def append(self, transaction):
        if self._check_transaction(transaction):
            self.history.append(transaction)
            return True
        else:
            print 'invalid transaction: %s' % transaction
            return False

    def recover_filesys(self):
        if self._load():
            filesys = FileSystem()
            for each in self.history:
                filesys = self._op(filesys, each)
                if not filesys:
                    return None
            return filesys
        else:
            return None

    def dump(self):
        try:
            with open(self.dump_loc, 'wb') as fp:
                json.dump(self.history, fp)
        except Exception as e:
            print 'failed to dump the history to this location: %s' % self.dump_loc
            print e
            return False
        else:
            return True

    def _load(self):
        try:
            with open(self.dump_loc, 'r') as fp:
                self.history = json.load(fp)
        except Exception as e:
            print 'failed to load the history from this location: %s' % self.dump_loc
            print e
            return False
        else:
            return True

    def _op(self, filesys, cmd):
        try:
            if cmd[0] == CREATE_OP:
                if not filesys.create(cmd[1]):
                    return None
            elif cmd[0] == DELETE_OP:
                if not filesys.delete(cmd[1]):
                    return None
            elif cmd[0] == APPEND_OP:
                if not filesys.append(cmd[1], cmd[2]):
                    return None
        except Exception as e:
            print e
            return None
        else:
            return filesys

    def _check_transaction(self, transaction):
        if isinstance(transaction, (list, tuple)) and \
                        (len(transaction) == 2 or len(transaction) == 3):
            if transaction[0] in [CREATE_OP, DELETE_OP, APPEND_OP] \
                        and isinstance(transaction[1], str):
                if transaction[0] == APPEND_OP and (len(transaction) != 3 \
                        or not isinstance(transaction[2], str)):
                    return False
                return True
        return False

if __name__ == '__main__':
    his = History(12)
    his.append([CREATE_OP, '1.txt'])
    his.append([APPEND_OP, '1.txt', 'hello'])
    his.append([CREATE_OP, '2.txt'])
    his.append([APPEND_OP, '2.txt', 'hello2'])
    his.dump()
    filesys = his.recover_filesys()
    if filesys:
        for token in filesys.display_tokens():
            print token
            print filesys.read(token)
            print
