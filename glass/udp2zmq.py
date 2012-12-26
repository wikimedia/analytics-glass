#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gevent import server
from gevent_zeromq import zmq


class DatagramRouter(server.DatagramServer):
    def __init__(self, port):
        super(DatagramRouter, self).__init__(':%d' % port)
        context = zmq.Context()
        self.zsocket = context.socket(zmq.PUB)
        self.zsocket.bind('tcp://*:%d' % port)

    def handle(self, data, address):
        for line in data.split('\n'):
            line = line.strip()
            if line:
                self.zsocket.send(line + '\n')


if __name__ == '__main__':
    routers = [DatagramRouter(port) for port in (8421, 8422)]
    for router in routers:
        router.start()
    for router in routers:
        router.serve_forever()
