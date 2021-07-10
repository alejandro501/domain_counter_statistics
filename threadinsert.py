import datetime
import os
from tqdm import tqdm
import multiprocessing as mp

import pymysql
import threading
import sys
import re
import time
from queue import Queue
from DBUtils.PooledDB import PooledDB

class ThreadInsert(object):

    def __init__(self):

        self.params = sys.argv
        self.filename = ''
        self.process_count = -1

        for i, item in enumerate(self.params):
            if(item.endswith('.txt')):
                self.filename = item
            if(item == '--top'):
                self.process_count = int(self.params[i+1])

        if(self.filename):
            start_time = time.time()
            self.data = mp.Manager().list()
            self.result = mp.Manager().dict()
            # dirname = "email_stats_{0}".format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            # os.mkdir(dirname)
            self.pool = self.mysql_connection()
            self.getData()
            self.result = [ [(k, v) for k, v in self.result.items()] ]
            self.mysql_delete()
            self.task()
            print("========= Data insertion, a total of time-consuming: {}'s =========".format(round(time.time() - start_time, 3)))
        else:
            print('Wrong file format, should be a txt file.')

    #Database Connectivity
    def mysql_connection(self):
        maxconnections = 15    # Maximum number of connections
        pool = PooledDB(
            pymysql,
            maxconnections,
            host='localhost',
            user='homestead',
            port=3306,
            passwd='secret',
            db='homestead',
            use_unicode=True)
        return pool

         #Read data from local files
    def getData(self):
        st = time.time()

        data = []

        res = {}

        list = open(self.filename).read().split()
        if(self.process_count == -1):
            self.process_count = len(list)

        if(self.process_count > 0):
            list = list[:self.process_count]

        #chunking list
        n = int(len(list) /  mp.cpu_count())
        chunks = [list[i:i + n] for i in range(0, len(list), n)]

        processes = []
        print('Processing list on {0} cores...'.format(mp.cpu_count()))
        for chunk in chunks:
            p = mp.Process(target=self.process, args=[chunk])
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        # end of chunking list

    def process(self, list):
        for line in tqdm(list):
            if(self.checkEmailRegex(line)):
                line = line.lower().split("@")[1]

                self.data.append(line)

                self.result[line] = self.data.count(line)


                line = {line, self.data.count(line)}

    # check email validity
    def checkEmailRegex(self, email):
        regex_email = '^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$'
        if(re.search(regex_email, email)):
            return True
        else:
            return False
    #end of check email validity

         #Database rollback
    def mysql_delete(self):
        st = time.time()
        con = self.pool.connection()
        cur = con.cursor()
        sql = "TRUNCATE TABLE statistics"
        cur.execute(sql)
        con.commit()
        cur.close()
        con.close()
        print("Empty the original data. ==>> Time-consuming: {}'s".format(round(time.time() - st, 3)))

         #Data Insertion
    def mysql_insert(self, *args):
        con = self.pool.connection()
        cur = con.cursor()
        sql = "INSERT INTO statistics(name, cnt) VALUES(%s, %s)"
        try:
            cur.executemany(sql, *args)
            con.commit()
        except Exception as e:
            con.rollback()    # Transaction rollback
            print('SQL execution error, reason:', e)
        finally:
            cur.close()
            con.close()

         #Open multi-threaded tasks
    def task(self):
                 # Set the maximum number of queues and threads
        q = Queue(maxsize=10)
        st = time.time()
        while self.result:
            content = self.result.pop()

            t = threading.Thread(target=self.mysql_insert, args=(content,))
            q.put(t)
            if (q.full() == True) or (len(self.result)) == 0:
                thread_list = []
                while q.empty() == False:
                    t = q.get()
                    thread_list.append(t)
                    t.start()
                for t in thread_list:
                    t.join()
        print("Data insertion completed. ==>> Time-consuming: {}'s".format(round(time.time() - st, 3)))


if __name__ == '__main__':
    ThreadInsert()