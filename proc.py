import sys
import os
import re
import datetime
import time
import csv

import mysql.connector as connector
from mysql.connector.pooling import MySQLConnectionPool

import mysql

import numpy as np
from tqdm import tqdm
from time import sleep
import multiprocessing as mp

import numpy

pool = MySQLConnectionPool( pool_name="sql_pool",
                            pool_size=32,

                            host="localhost",
                            port="3306",
                            user="homestead",
                            password="secret",
                            database="homestead")

sql_statement = "INSERT INTO statistics (name, cnt) VALUES (%s, %s)"

list = []
domains = mp.Manager().list()
unique_list = mp.Manager().list()
invalid_emails = mp.Manager().list()
result = mp.Manager().list()
regex_email = '^(\w|\.|\_|\-)+[@](\w|\_|\-|\.)+[.]\w{2,3}$'

# check email validity
def check(list, email):
    if(re.search(regex_email, email)):
        domains.append(email.lower().split('@')[1])
        return True
    else:
        invalid_emails.append(email)
        return False
#end of check email validity

# execution time converter
def convertTime(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    if(hour == 0):
        if(minutes == 0):
            return "{0} sec".format(seconds)
        else:
            return "{0}min {1}sec".format(minutes, seconds)
    else:
        return "{0}hr {1}min {2}sec".format(hour, minutes, seconds)
# execution time converter end

#process
def process(list):
    for item in tqdm(list):
        if(check(list, item)):
            item = item.lower().split('@')[1]
            item = item.lower().split('@')[1]
            if item not in unique_list:
                unique_list.append(item)
# end of process

def insert(list):
    global sql_statement

    # Add to db
    con = pool.get_connection()
    cur = con.cursor()

    print("PID %d: using connection %s" % (os.getpid(), con))
    #cur.executemany(sql_statement, sorted(map(set_result, list)))
    for item in list:
        dummy = (item, domains.count(item))
        cur.execute(sql_statement, dummy)
        con.commit()

# statistics
def statistics(list):
    for item in tqdm(list):
        if(domains.count(item) > 0):
            result.append([domains.count(item), item])
# end of statistics

params = sys.argv
filename = ''
process_count = -1
for i, item in enumerate(params):
    if(item.endswith('.txt')):
        filename = item
    if(item == '--top'):
        process_count = int(params[i+1])


def set_result(item):
    return item, domains.count(item)

# main
if(filename):
    try:
        start_time = time.time()
        now = datetime.datetime.now()
        dirname = "email_stats_{0}".format(now.strftime("%Y%m%d_%H%M%S"))
        os.mkdir(dirname)

        list = open(filename).read().split()

        if(process_count == -1):
            process_count = len(list)

        if(process_count > 0):
            list = list[:process_count]

        #chunking list
        n = int(len(list) /  mp.cpu_count())
        chunks = [list[i:i + n] for i in range(0, len(list), n)]

        processes = []
        print('Processing list on {0} cores...'.format(mp.cpu_count()))
        for chunk in chunks:
            p = mp.Process(target=process, args=[chunk])
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        # insert(unique_list)

        ## step 2 - write sql

        ##  Clearing out db before new data insert
        con = pool.get_connection()
        cur = con.cursor()

        delete_statement = "DELETE FROM statistics"
        cur.execute(delete_statement)

        u_processes = []

        #Maximum pool size for sql is 32, so maximum chunk number should be that too.
        if(mp.cpu_count() < 32):
            n2 = int(len(unique_list) /  mp.cpu_count())
        else:
            n2 = int(len(unique_list) /  32)

        u_chunks = [unique_list[i:i + n2] for i in range(0, len(unique_list), n2)]
        for u_chunk in u_chunks:
            p = mp.Process(target=insert, args=[u_chunk])
            p.start()
            u_processes.append(p)

        for p in u_processes:
            p.join()

        print('Creating statistics for {0} individual domains...'.format(len(unique_list)))

        print('Writing final statistics...')
        print('OK.')
        f = open("{0}/stat.txt".format(dirname),"w+")
        f.write("Number of processed emails: {0}\r\n".format(process_count))
        f.write("Number of valid emails: {0}\r\n".format(len(list) - len(invalid_emails)))
        f.write("Number of invalid emails: {0}\r\n".format(len(invalid_emails)))
        f.write("Execution time: {0}".format(convertTime(int(time.time() - start_time))))
        f.close()

    except FileNotFoundError:
        print('File not found, path or file broken.')
else:
    print('Wrong file format, should be a txt file.')
# main
