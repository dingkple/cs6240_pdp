import sys
class SimpleDB():


    def __init__(self):
        self.data = {}
        self.total_counter = {}
        self.transactions = []

        self.IN_TRANSACTION = False
        self.END = False


    def _get_current_transaction(self):
        return self.transactions[-1]

    def _set_db(self, kvpair):
        k, v = kvpair
        if self.IN_TRANSACTION:
            current_transaction = self._get_current_transaction()
            if k not in current_transaction:
                if k in self.data:
                    current_transaction[k] = (False, self.data[k])
                else:
                    current_transaction[k] = (True, v)

        self._set_kvpair(k, v)

    def _set_kvpair(self, k, v):
        old_v = self.data[k] if k in self.data else None

        self.data[k] = v

        if v not in self.total_counter:
            self.total_counter[v] = 0
        self.total_counter[v] += 1

        if old_v and old_v != v:
            self.total_counter[old_v] -= 1
            if self.total_counter[old_v] == 0:
                del self.total_counter[old_v]

    def _unset(self, k):
        v = self.data[k]
        del self.data[k]

        self.total_counter[v] -= 1

        if self.total_counter[v] == 0:
            del self.total_counter[v]

    def get_db(self, key):
        if key in self.data:
            print self.data[key]
            return
        else:
            print 'NULL'

    def _un_set_db(self, key):
        if self.IN_TRANSACTION:
            current_transaction = self._get_current_transaction()
            if key in current_transaction:
                is_new, value = current_transaction[key]
                if is_new:
                    del current_transaction[key]
            else:
                current_transaction[key] = (False, self.data[key])

        if key in self.data:
            self._unset(key)


    def _num_equal_to(self, value):
        if value in self.total_counter:
            print self.total_counter[value]
        else:
            print 0

    def _begin_transaction(self):
        self.IN_TRANSACTION = True
        self.transactions += {},


    def _end_transaction(self):
        if self.IN_TRANSACTION:
            self.transactions = []
            self.IN_TRANSACTION = False
        else:
            print 'NO TRANSACTION'


    def _roll_back(self):
        if self.IN_TRANSACTION:
            current_transaction = self.transactions.pop()
            for key in current_transaction:
                is_new, value = current_transaction[key]
                if not is_new:
                    self._set_kvpair(key, value)
                else:
                    self._unset(key)
            if not self.transactions:
                self.IN_TRANSACTION = False
        else:
            print 'NO TRANSACTION'

    def _exec_cmd(self, cmd):
        if cmd[0] == 'SET':
            cmd = (cmd[0], cmd[1])
            self._set_db(cmd[1])
            return 
        elif cmd[0] in ['GET', 'UNSET', 'NUMEQUALTO']:    
            if cmd[0] == "GET":
                self.get_db(cmd[1])
            elif cmd[0] == 'UNSET':
                self._un_set_db(cmd[1])
            elif cmd[0] == 'NUMEQUALTO':
                self._num_equal_to(cmd[1])
        else:
            if cmd[0] == 'BEGIN':
                self._begin_transaction()
            elif cmd[0] == 'COMMIT':
                self._end_transaction()
            elif cmd[0] == 'ROLLBACK':
                self._roll_back()

    def parse(self, cmd):
        if len(cmd) == 1 and cmd[0] == 'END':
            self.END = True
            return

        if len(cmd) == 3:
            cmd = (cmd[0], cmd[1:])
          
        self._exec_cmd(cmd)

def main():
    db = SimpleDB()
    while True:
        if db.END:break
        s = raw_input().split()
        db.parse(s)

import os
def test():

    files = os.listdir('test_cases_5n9i93agh5d')

    input_file = []
    output_file = []

    for f in files:
        if f.startswith('input'):
            input_file +=
        else:
            output_file += f,
    input_file.sort()
    output_file.sort()
    print input_file
    for i in range(len(input_file)):
        s = SimpleDB()
        f = open('test_cases_5n9i93agh5d' + '/' + input_file[i])
        print input_file[i]
        for line in f.readlines():
            s.parse(line.split())
        print '*******************'

if __name__ == '__main__':
    main()

# test()

# db = SimpleDB()
# l = ['SET A 1', 'GET A', 'NUMEQUALTO 1']
# l = ['BEGIN', 'SET A 30', 'GET A', 'ROLLBACK', 'GET A', 'COMMIT', 'END']

# for k in l:
#     db.parse(k.split())

