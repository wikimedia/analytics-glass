#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gevent import queue, server, spawn
from gevent_zeromq import zmq


q = queue.Queue()


class DatagramRouter(server.DatagramServer):
    def handle(self, data, address):
        for line in data.split('\n'):
            q.put(data)


def zmq_worker(host='*', port=8006):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.bind('tcp://%s:%d' % (host, port))

    while 1:
        sock.send(q.get())


if __name__ == '__main__':
    DatagramRouter(':8421').start()
    DatagramRouter(':8422').start()
    spawn(zmq_worker).join()
