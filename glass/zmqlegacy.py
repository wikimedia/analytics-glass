#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urlparse import parse_qsl
from datetime import datetime

from gevent import queue, server, spawn, joinall
from gevent_zeromq import zmq


q = queue.Queue()


def format_ts(ts):
    return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%dT%H:%M:%S')

def munge_mw(logline):
    seq_id, db, qs = logline.split()
    event = dict(parse_qsl(qs))
    ts = format_ts(event.pop('timestamp'))
    return '?_db=%s&_id=%s mediawiki %s %s 0.0.0.0' % (db, qs.split('=', 1)[1], seq_id, ts)



def zmq_listener(port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://127.0.0.1:%d' % port)
    socket.setsockopt(zmq.SUBSCRIBE, '')

    while 1:
        data = socket.recv()
        q.put(data)


def zmq_worker(host='*', port=8006):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind('tcp://%s:%d' % (host, port))

    while 1:
        line = q.get()
        sock.send(line)


if __name__ == '__main__':
    joinall((
        spawn(zmq_listener, 8421),
        spawn(zmq_listener, 8422),
        spawn(zmq_worker)
    ))
