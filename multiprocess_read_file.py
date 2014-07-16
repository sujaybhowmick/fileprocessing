__author__ = 'sujay'

from multiprocessing import Pool
from itertools import izip_longest
from time import time
from StringIO import StringIO
import csv
import os
import json


current_time_in_millis = lambda: int(round(time() * 1000))


#
# This method processes the file using chunk of lines
# return a dictionary of lists where keys are the
# csv headers and list of values are column values
#

def process_chunk(row_line):
    str_line = StringIO(row_line)
    reader = csv.reader(str_line, delimiter=',')
    key_values = {}
    values = []
    for row in reader:
        if row:
            for item in row:
                column = item.split('=')
                if len(column) == 2:
                    values.append(column[1])
                else:
                    values.append('')
                if len(column) == 2:
                    key_values[column[0]] = values
    return key_values


#
# This method groups the lines into chunks and creates
# an iterable object
#

def chunk(n, iterable, pad_value=None):
    return izip_longest(*[iter(iterable)] * n, fillvalue=pad_value)


#
# This method writes the parsed_data of dictionary objects
# into a csv file.
#

def write_parsed_data(key_values, output_file_name):

    with open(output_file_name, 'a+') as f:
        writer = csv.writer(f)
        writer.writerow(list(key_values.keys()))
        for k, v in key_values.iteritems():
            writer.writerow(v)


#
# The main method to start
#

if __name__ == '__main__':

    # Parse the config file for parameters
    json_conf_file = 'conf.json'
    
    with open(json_conf_file, 'r') as json_data_file:
        conf = json.load(json_data_file)

    num_processes = conf['num_processes']
    # Get the number of processes to start
    if num_processes:
        num_processes = 4

    # Get the number of lines to chunk for each process
    lines_to_chunk = conf['chunk_size']
    
    if lines_to_chunk:
        lines_to_chunk = 1000

    file_name_in = os.environ['HOME'] + '/POC.csv'
    file_name_out = os.environ['HOME'] + '/processed_data.csv'
    print "Processing input file - ", file_name_in
    file_data = open(file_name_in, 'rU')
    
    # open and close the file for first time to make s
    # sure file is present
    
    file_out = open(file_name_out, 'w')
    file_out.close()

    # create pool of processes
    pool = Pool(num_processes)

    # start timer
    start_time = current_time_in_millis()

    # lines to chunk
    

    for chunk in chunk(lines_to_chunk, file_data):
        results = pool.map(process_chunk, chunk)
        for result in results:
            write_parsed_data(result, file_name_out)

    # stop timer
    stop_time = current_time_in_millis()

    timeTaken = stop_time - start_time

    if timeTaken > 1000:
        print "Processed output %s file in %d %s\n" % (file_name_out, (stop_time - start_time) / 1000, 'secs...')
    else:
        print "Processed output %s file in %d %s\n" % (file_name_out, (stop_time - start_time), 'millis...')