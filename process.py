import sys
import os
import re
import datetime
import time

import csv

from tqdm import tqdm
from time import sleep

import multiprocessing as mp

list = []
domains = mp.Manager().list()
unique_list = mp.Manager().list()
invalid_emails = mp.Manager().list()
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
            if item not in unique_list:
                unique_list.append(item)
# end of process

# statistics
def statistics(list):
    for item in tqdm(list):
        if(domains.count(item) > 0):
            result.append([domains.count(item), item])
            # print('{0} | {1}'.format(domains.count(item), item))
# end of statistics

## run invalid
# def writeInvalid(f, item):
#     f.write("{0}\r\n".format(item))
# # run invalid
#
# #run list
# def writeList(f, item):
#     f.write("{0}\r\n".format(item))
# run list

## write file output
# def output(dirname, type, arr):
#     if(type == 'invalid'):
#         if(len(arr) > 0):
#             f = open("{0}/invalid.txt".format(dirname),"w+")
#             f.write("Invalid email addresses: {0}\r\n".format(len(arr)))
#             f.write("\r\n")
#             f.write("Email addresses:\r\n")
#             print('Listing {0} invalid emails...'.format(len(arr)))
#             for i, item in enumerate(tqdm(arr)):
#                 with concurrent.futures.ThreadPoolExecutor() as executor:
#                     executor.submit(writeInvalid, f, item)
#             f.close()
#     elif(type =='list'):
#         print('Writing clean list of {0} emails to file...'.format(len(list)))
#         f = open("{0}/clean_list.txt".format(dirname),"w+")
#         for i in tqdm(range(len(arr))):
#                 with concurrent.futures.ThreadPoolExecutor() as executor:
#                     executor.submit(writeList, f, arr[i])
#         f.close()
# end of write file output

params = sys.argv
filename = ''
process_count = -1
for i, item in enumerate(params):
    if(item.endswith('.txt')):
        filename = item
    if(item == '--top'):
        process_count = int(params[i+1])


def shit(item):
    return domains.count(item), item

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

        for p in processes:
            p.close()

        print('Creating statistics for {0} individual domains...'.format(len(unique_list)))
        # chunking unique list
        # n = int(len(unique_list) / mp.cpu_count())
        # chunks2 = [unique_list[i:i + n] for i in range(0, len(unique_list), n)]

        # processes2 = []
        # for item in chunks2:
        #     p = mp.Process(target=statistics, args=[item])
        #     p.start()
        #     p.join()
        #     processes2.append(p)
        #
        # for p in processes2:
        #     p.close()

        # with open("{0}/result.txt".format(dirname), "r+") as f:
        #     sorted_contents =  ''.join(sorted(result, key = lambda x: x.split(' ')[0], reverse=True))
        #     f.seek(0)
        #     f.truncate()
        #     f.write(sorted_contents)

        print('Writing results to file...')
        print(sorted(map(shit, unique_list), reverse=True))
        with open('tmp_file.txt', 'w', newline='') as f:
            csv.writer(f).writerows(sorted(map(shit, unique_list), reverse=True))


        print('\n'.join(map(shit, unique_list)))
        ## Optional features : writing invalid list, and clean email list
        # output(dirname, 'invalid', invalid_emails)
        # output(dirname, 'list', sorted(clear_list))

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
