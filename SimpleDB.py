import sys
class SimpleDB():


    def __init__(self):
        self.data = {}
        self.total_counter = {}
        self.transactions = []

        # self.current_transaction = {}

        self.SUCCESS = True
        self.IN_TRANSACTION = False


    def current_transaction(self):
        return self.transactions[-1]

    def set_db(self, kvpair):
        k, v = kvpair
        if self.IN_TRANSACTION:
            if k not in self.current_transaction:
                if k in self.data:
                    self.current_transaction[k] = (False, self.data[k])
                else:
                    self.current_transaction[k] = (True, v)

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

        self.SUCCESS = False
        print 'NULL'

    def unset_db(self, key):
        '''
            should we rollback when key not exist? 
        '''
        if self.IN_TRANSACTION:
            if key in self.current_transaction:
                is_new, value = self.current_transaction[key]
                if is_new:
                    del self.current_transaction[key]
            else:
                self.current_transaction[key] = (False, self.data[key])

        if key in self.data:
            self._unset(key)


    def num_equal_to(self, value):
        if value in self.total_counter:
            print self.total_counter[value]
            return

        print 0

    def begin_transaction(self):
        self.IN_TRANSACTION = True
        self.transactions += {},


    def end_transaction(self):
        if self.IN_TRANSACTION:
            self.transactions.pop()
            if not self.transactions:
                self.IN_TRANSACTION = False
        else:
            print 'NO TRANSACTION'


    def roll_back(self):
        if self.IN_TRANSACTION:
            current_transaction = self.current_transaction.pop()
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

    def exec_transaction(self):
        for operation in self.transactions:
            if operation[0] == 'UNSET':
                cmd, key = operation
                if key in self.data:
                    self.done.append((cmd, (key, self.data[key])))
                    self._exec_cmd(operation)
            else:
                self._exec_cmd(operation)

            if not self.SUCCESS:
                self.roll_back(operation)


    def _exec_cmd(self, cmd):
        if cmd[0] == 'SET':
            if len(cmd) != 2:
                self.SUCCESS = False
                return
            cmd = (cmd[0], cmd[1])
            self.set_db(cmd[1])
            return 
        elif cmd[0] in ['GET', 'UNSET', 'NUMEQUALTO']:    
            if cmd[0] == "GET":
                self.get_db(cmd[1])
            elif cmd[0] == 'UNSET':
                self.unset_db(cmd[1])
            elif cmd[0] == 'NUMEQUALTO':
                self.num_equal_to(cmd[1])
        else:
            if cmd[0] == 'BEGIN':
                self.begin_transaction()
            elif cmd[0] == 'COMMIT':
                self.end_transaction()
            elif cmd[0] == 'ROLLBACK':
                self.roll_back()

    def parse(self, cmd):
        self.SUCCESS = True
        if len(cmd) == 1 and cmd[0] == 'END':
            sys.exit(0)

        if len(cmd) == 3:
            cmd = (cmd[0], cmd[1:])
          
        self._exec_cmd(cmd)

def main():
    db = SimpleDB()
    while True:
        s = raw_input().split()
        db.parse(s)


def test():


if __name__ == '__main__':
    main()


# db = SimpleDB()
# l = ['SET A 1', 'GET A', 'NUMEQUALTO 1']
# l = ['BEGIN', 'SET A 30', 'GET A', 'ROLLBACK', 'GET A', 'COMMIT', 'END']

# for k in l:
#     db.parse(k.split())
