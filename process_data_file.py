__author__ = 'sujay'

from StringIO import StringIO

import csv
from time import time
import os

current_time_in_millis = lambda: int(round(time() * 1000))

# defines the header row for the file to be written
header = ['col2', 'AmsLogType', 'AmsCtlSeq', 'AmsMod', 'col1', 'col3']


def process_line(row_line):
    global header

    str_line = StringIO(row_line)
    reader = csv.reader(str_line, delimiter=',')

    key_value_row = {}
    for row in reader:
        if row:
            for item in row:
                values = []
                column = item.split('=')
                if len(column) == 2:
                    values.append(column[1])
                    key_value_row[column[0]] = values

    for col in header:
        if key_value_row.__contains__(col):
            key_value_row[col] = key_value_row[col]
        else:
            key_value_row[col] = []

    return key_value_row


def write_parsed_data(key_value_row, output_file_name):
    with open(output_file_name, 'a+') as f:
        writer = csv.writer(f)
        #writer.writerow(list(key_values.keys()))
        row = []
        for k in sorted(key_value_row):
            row += key_value_row[k]
        writer.writerow(row)


def write_header(output_file_name):
    global header
    with open(output_file_name, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)


def main():
    global header
    header.sort()
    file_name_in = os.environ['HOME'] + '/test.csv'
    file_name_out = os.environ['HOME'] + '/processed_data.csv'

    print "Processing input file - ", file_name_in

    # read and write the header

    file_handle = open(file_name_in, 'rU')
    start_time = current_time_in_millis()

    write_header(file_name_out)

    for line in file_handle:
        write_parsed_data(process_line(line), file_name_out)

    stop_time = current_time_in_millis()

    time_taken = stop_time - start_time

    if time_taken > 1000:
        print "Processed output %s file in %d %s\n" % (file_name_out, time_taken / 1000, 'secs...')
    else:
        print "Processed output %s file in %d %s\n" % (file_name_out, time_taken, 'millis...')

if __name__ == '__main__':
    main()