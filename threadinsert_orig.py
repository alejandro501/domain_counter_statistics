import pymysql
import threading
import re
import time
from queue import Queue
from DBUtils.PooledDB import PooledDB

class ThreadInsert(object):

    def __init__(self):
        start_time = time.time()
        self.pool = self.mysql_connection()
        self.data = self.getData()

        print('self.data : ')
        print(self.data)

        self.mysql_delete()
        self.task()
        print("========= Data insertion, a total of time-consuming: {}'s =========".format(round(time.time() - start_time, 3)))

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
        with open("user2.txt", "rb") as f:
            data = []
            for line in f:
                line = re.sub("\s", "", str(line, encoding="utf-8"))
                line = tuple(line[1:-1].split("\"\""))
                data.append(line)
        n = 1000        # Press each1000Row data is split into nested lists for the smallest unit, which can be split according to actual conditions
        result = [data[i:i + n] for i in range(0, len(data), n)]
        print("To obtain a total of {} sets of data, each set of {} elements. ==>> Time-consuming: {}'s".format(len(result), n, round(time.time() - st, 3)))
        return result

         #Database rollback
    def mysql_delete(self):
        st = time.time()
        con = self.pool.connection()
        cur = con.cursor()
        sql = "TRUNCATE TABLE user3"
        cur.execute(sql)
        con.commit()
        cur.close()
        con.close()
        print("Empty the original data. ==>> Time-consuming: {}'s".format(round(time.time() - st, 3)))

         #Data Insertion
    def mysql_insert(self, *args):
        con = self.pool.connection()
        cur = con.cursor()
        sql = "INSERT INTO user3(uid, uname, email) VALUES(%s, %s, %s)"
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
        while self.data:
            content = self.data.pop()
            print('content:')
            print(content)
            t = threading.Thread(target=self.mysql_insert, args=(content,))
            q.put(t)
            if (q.full() == True) or (len(self.data)) == 0:
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