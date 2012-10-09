#!/usr/bin/env python3.3
# -*- coding: utf-8 -*-
"""
             .
       ___.  |     ___    ____   ____
     .'   `  |    /   `  (      (
     |    |  |   |    |  `--.   `--.
      `---| /\__ `.__/| \___.' \___.'
      \___/


    Reads UDP datagrams emitted by the ClickTracking
    and E3Experiments extensions.

"""
import datetime
import json
import multiprocessing
import redis
import signal
import socket
import time
import uuid


try:
    from urllib.parse import unquote, unquote_plus
except ImportError:
    from urllib import unquote, unquote_plus


log = multiprocessing.log_to_stderr()
r = redis.Redis()


# -----------------
# Data formatters
#

def unix_ts(ts):
    dt = datetime.datetime.fromtimestamp(int(ts))
    return int(time.mktime(dt.timetuple()))


def timestamp(ts):
    dt = datetime.datetime.strptime(ts, "%Y%m%d%H%M%S")
    return int(time.mktime(dt.timetuple()))


def parse_qs(qs):
    kvs = (kv.split('=') for kv in qs.lstrip('?').split('&'))
    return {k: unquote(v) for k, v in kvs}


def extra(raw):
    if raw.startswith('event_id'):
        return parse_qs(raw)
    if '|' in raw:
        return [unquote_plus(x) for x in raw.split('|')]
    return raw


raw = lambda x: x
boolean = lambda x: bool(int(x))

ct_fields = (
    ('event_id',        unquote),
    ('timestamp',       timestamp),
    ('authenticated',   boolean),
    ('token',           raw),
    ('namespace',       int),
    ('editcount',       int),
    ('edits_6mo',       int),
    ('edits_3mo',       int),
    ('edits_1mo',       int),
    ('extra',           extra)
)

e3_fields = {
    'article_id':        int,
    'authenticated':     boolean,
    'by_email':          bool,
    'creator_user_id':   int,
    'editcount':         int,
    'minor':             boolean,
    'namespace':         int,
    'registered':        int,
    'self_made':         boolean,
    'timestamp':         unix_ts,
    'user_id':           int,
    'userbuckets':       unquote,
    'using_api':         boolean,
    'version':           int
}


def parse_ct(e):
    map = {}
    values = e.split(None, 10)
    for (field, format), value in zip(ct_fields, values):
        map[field] = format(value)
    return map


def parse_e3(e):
    map = {}
    for field, value in parse_qs(e).items():
        format = e3_fields.get(field, unquote_plus)
        map[field] = format(value)
    return map


def parse(dgram):
    """Extract a normalized event query string out of a udp2log datagram"""
    dgram = dgram.decode('utf-8', 'replace')
    seq_id, wiki, raw = dgram.strip().split(' ', 2)
    map = parse_e3(raw) if raw.startswith('?') else parse_ct(raw)
    map['wiki'] = wiki
    map['seq_id'] = int(seq_id)
    return map


def read_udp(pipe, host='', port=8421):
    """Read UDP datagrams into pipe"""
    sock = socket.socket(type=socket.SOCK_DGRAM)
    sock.bind((host, port))

    while 1:
        dgram = sock.recv(4096)
        try:
            event = parse(dgram)
        except:
            log.exception('Parse failure: %s', dgram)
        else:
            pipe.send(event)


def store_event(event):
    """Store an event mapping in Redis"""
    euuid = uuid.uuid4()
    multi = r.pipeline()
    multi.zadd('eid:%(event_id)s' % event, euuid, event['timestamp'])
    multi.hmset(euuid, event)
    multi.publish(event['event_id'], json.dumps(event))
    multi.execute()


def process_events(pipe):
    while 1:
        e = pipe.recv()
        try:
            store_event(e)
        except:
            log.exception('Store failure: %s', e)


def handle_sigchld(signum, frame):
    log.error("Child process died; terminating")
    raise SystemExit(2)


if __name__ == '__main__':
    outflow, inflow = multiprocessing.Pipe(duplex=False)

    signal.signal(signal.SIGCHLD, handle_sigchld)
    reader = multiprocessing.Process(target=read_udp, args=(inflow,))
    reader.daemon = True
    reader.start()

    process_events(outflow)
    reader.join()
