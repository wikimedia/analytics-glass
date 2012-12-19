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
import os
import subprocess
import zmq



class GzipTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    TimedRotatingFileHandler subclass that gzips logfiles and moves them
    to ./archive.
    """

    def doRollover(self):
        super(GzipTimedRotatingFileHandler, self).doRollover()
        try:
            dir, base = os.path.split(self.baseFilename)
            for f in os.listdir(dir):
                if f.startswith(base + '.'):
                    src = os.path.join(dir, f)
                    dst = os.path.join(dir, 'archive', f)
                    subprocess.call(('gzip', src))
                    os.renames(src + '.gz', dst + '.gz')
        except:
            logger.exception('Failed to archive logs')


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
logfile_handler = GzipTimedRotatingFileHandler(filename=args.destfile,
        when='S', interval=10, encoding='utf-8', utc=True)
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
