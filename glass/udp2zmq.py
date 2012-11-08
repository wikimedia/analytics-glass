#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urlparse import parse_qsl
from datetime import datetime

from gevent import queue, server, spawn
from gevent_zeromq import zmq


q = queue.Queue()


def format_ts(ts):
    return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%dT%H:%M:%S')

def munge_mw(logline):
    seq_id, db, qs = logline.split()
    event = dict(parse_qsl(qs))
    ts = format_ts(event.pop('timestamp'))
    return '?_db=%s&_id=%s mediawiki %s %s' % (db, qs.split('=', 1)[1], seq_id, ts)


stream = []

for line in stream:
    if 'event_id' in line:
        try:
            line = mungw_mw(line)
        except (ValueError, KeyError):
            pass


class DatagramRouter(server.DatagramServer):
    def handle(self, data, address):
        for line in data.strip().split('\n'):
            q.put(line.strip())


class ErrorLogRouter(server.DatagramServer):
    def handle(self, data, address):
        for line in data.split('\n'):
            if 'event_id' in line:
                try:
                    line = munge_mw(line)
                except (ValueError, KeyError):
                    pass
                else:
                    q.put(line.strip())


def zmq_worker(host='*', port=8006):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind('tcp://%s:%d' % (host, port))

    while 1:
        line = q.get()
        # XXX: Current only listens to enwiki data
        if line.startswith('?_db=enwiki'):
            sock.send(line)


if __name__ == '__main__':
    ErrorLogRouter(':8421').start()
    DatagramRouter(':8422').start()
    spawn(zmq_worker).join()
