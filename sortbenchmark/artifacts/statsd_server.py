#!/usr/bin/env python

import socket
import sys
import time

if len(sys.argv) < 2:
  print('Usage {} <port>'.format(sys.argv[0]))
  sys.exit(-1)

UDP_IP = ''
UDP_PORT = int(sys.argv[1])

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP, UDP_PORT))

while True:
    data, addr = sock.recvfrom(4096)
    print('{:.6f} {}'.format(time.time(), data))

