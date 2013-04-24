#!/usr/bin/env python3
# -*- coding: utf8 -*-
import collections
import logging
import zmq


ENDPOINT = 'tcp://localhost:8422'


def get_logger():
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    log.addHandler(stderr_handler)

    return log


def zsub(endpoint, topic=b''):
    context = zmq.Context.instance()
    sock = context.socket(zmq.SUB)
    sock.connect(endpoint)
    sock.setsockopt(zmq.SUBSCRIBE, b'')

    while 1:
        yield sock.recv()


log = get_logger()
lost = collections.defaultdict(int)
seqs = {}


for line in zsub(ENDPOINT):

    try:
        host, seq = line.split(' ', 3)[1:3]
    except IndexError:
        log.exception(line)
        continue

    seq = int(seq)
    last = seqs.get(host)
    seqs[host] = seq

    if last is not None and last < (seq - 1):
        skipped = seq - last - 1
        log.error('%s: %d -> %d (skipped: %d)', host, last, seq, skipped)
        lost[host] += skipped
