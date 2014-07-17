__author__ = 'sujay'

from StringIO import StringIO

import csv
from time import time
import os


current_time_in_millis = lambda: int(round(time() * 1000))


def process_chunk(row_line):
    str_line = StringIO(row_line)
    reader = csv.reader(str_line, delimiter=',')

    key_values = {}
    for row in reader:
        if row:
            for item in row:
                values = []
                column = item.split('=')
                if len(column) == 2:
                    values.append(column[1])
                else:
                    values.append('')
                if len(column) == 2:
                    key_values[column[0]] = values
    return key_values


def read_write_header(file_in, file_out):
    file_in_data = open(file_in, 'rU')
    with open(file_out, 'w') as f:
        header = process_chunk(file_in_data.readline())
        writer = csv.writer(f)
        writer.writerow(list(header.keys()))
    file_in_data.close()


def write_parsed_data(key_values, output_file_name):
    with open(output_file_name, 'a+') as f:
        writer = csv.writer(f)
        #writer.writerow(list(key_values.keys()))
        row = []
        for k, v in key_values.iteritems():
            row += v
        writer.writerow(row)


def main():
    file_name_in = os.environ['HOME'] + '/reallybig.csv'
    file_name_out = os.environ['HOME'] + '/processed_data.csv'

    print "Processing input file - ", file_name_in

    # read and write the header
    read_write_header(file_name_in, file_name_out)
    file_handle = open(file_name_in, 'rU')
    start_time = current_time_in_millis()
    for line in file_handle:
        write_parsed_data(process_chunk(line), file_name_out)

    stop_time = current_time_in_millis()

    time_taken = stop_time - start_time

    if time_taken > 1000:
        print "Processed output %s file in %d %s\n" % (file_name_out, time_taken / 1000, 'secs...')
    else:
        print "Processed output %s file in %d %s\n" % (file_name_out, time_taken, 'millis...')

if __name__ == '__main__':
    main()