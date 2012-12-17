#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  zmq2log.py
  ----------
  Log ZeroMQ traffic to log-rotated file.

  usage: zmq2log.py [-h] [--filter FILTER] publisher destfile

  positional arguments:
    publisher        publisher URI
    destfile         write log to this file

  optional arguments:
    -h, --help       show this help message and exit
    --filter FILTER  subscription filter

"""
import argparse
import logging
import logging.handlers
import zmq



#
# Parse command-line args
#

parser = argparse.ArgumentParser(description='Log ZeroMQ traffic to disk')
parser.add_argument('--filter', default='', help='subscription filter')
parser.add_argument('publisher', help='publisher URI')
parser.add_argument('destfile', help='write log to this file')
args = parser.parse_args()



#
# Configure logging
#

# Configure log rotation
logfile_handler = logging.handlers.TimedRotatingFileHandler(
        filename=args.destfile, when='midnight', encoding='utf-8', utc=True)
logfile_handler.setLevel(logging.INFO)

# Configuring logging to stderr
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s\t%(message)s'))
console_handler.setLevel(logging.DEBUG)  # Don't pollute log files with status

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logfile_handler)
log.addHandler(console_handler)
log.debug('Started. Logging to %s.' % args.destfile)



#
# Configure ZeroMQ Subscriber
#

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect(args.publisher)
socket.setsockopt(zmq.SUBSCRIBE, args.filter)
log.debug('Connected to %s (filter: %s).' % (args.publisher, args.filter))



while 1:
    log.info(socket.recv().rstrip())
