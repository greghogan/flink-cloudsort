#!/usr/bin/env python

import collections
import math
import sys

if len(sys.argv) < 5:
  print("Usage: {0} <total bytes> <start time> <stop time> <price ($/GiB-month)>".format(sys.argv[0]))
  sys.exit(-1)

RECORD_SIZE = 100

total_bytes = int(sys.argv[1])
start = float(sys.argv[2])
stop = float(sys.argv[3])
price = float(sys.argv[4])

taskmanagers = collections.defaultdict(dict)

for line in sys.stdin.readlines():
  time, data = line.split(" ", 1)
  host, _, tm_id, job_name, op_name, task_number, gauge = data.split(".")
  gauge_name, value_and_type = gauge.split(":")
  gauge_value, gauge_type = value_and_type.split("|")

  if op_name.startswith("DataSink") and gauge_name == 'numRecordsIn':
    taskmanagers[task_number][time] = gauge_value

byte_seconds = 0
byte_count = 0

for taskmanager_number in taskmanagers:
  last_time = start
  last_bytes = 0

  for time in sorted(taskmanagers[taskmanager_number]):
    bytes = RECORD_SIZE * int(taskmanagers[taskmanager_number][time])
    byte_seconds += (bytes - last_bytes) * (stop - last_time)

    last_time = float(time)
    last_bytes = bytes

  byte_count += last_bytes

# the missing bytes are too insignificant to assign a proper
# reckoning from the last zero reading from the earliest
# TaskManager to report a non-zero value
byte_seconds += (total_bytes - byte_count) * (stop - start)
print total_bytes - byte_count

# add cost of data input
byte_seconds += total_bytes * (stop - start)

cost =  price * byte_seconds / (1024**3) / (30*24*60*60)

print("{0} bytes for a cost of ${1:,.2f}".format(byte_count, math.ceil(100*cost)/100))

