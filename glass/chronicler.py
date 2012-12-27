#!/usr/bin/env python3
# -*- coding: utf8 -*-
from __future__ import division, print_function, unicode_literals

import calendar
import hashlib
import logging
import operator
import sys
import time

import jsonschema
import zmq


json = zmq.utils.jsonapi

if sys.version_info[0] >= 3:
    items = operator.methodcaller('items')
    from urllib.parse import parse_qsl
    from urllib.request import urlopen
else:
    items = operator.methodcaller('iteritems')
    from urlparse import parse_qsl
    from urllib2 import urlopen

json = zmq.utils.jsonapi

meta_readable = dict(_rv='revision', _id='schema', _ok='valid', _db='site')
url = 'http://meta.wikimedia.org/w/index.php?action=raw&oldid=%d'
schemas = {}

casters = {
    'integer' : int,
    'array'   : lambda x: x.split(','),
    'boolean' : lambda x: x.lower() == 'true',
    'null'    : lambda x: None,
    'number'  : lambda x: float(x) if '.' in x else int(x),
    'string'  : lambda x: x
}



def typecast(object, schema):
    properties = schema.get('properties', {})
    types = {k: v.get('type') for k, v in items(properties)}
    return {k: casters[types.get(k, 'string')](v) for k, v in items(object)}


def parse_meta(object):
    metaschema = get_schema(4891798)
    return typecast(object, metaschema)


def parse_qs(q):
    """Parse a query string."""
    q = dict(parse_qsl(q.strip('?;')))
    meta = {}
    e = {}
    for k, v in items(q):
        if k.startswith('_'):
            meta[k] = v
        else:
            e[k] = v
    meta = parse_meta(meta)

    schema = get_schema(meta['_rv'])
    e = typecast(e, schema)
    jsonschema.validate(e, schema)

    e['meta'] = {meta_readable.get(k, k): v for k, v in items(meta)}
    return e


def parse_ncsa_ts(ts):
    """Converts a timestamp in NCSA format to seconds since epoch."""
    return calendar.timegm(time.strptime(ts, '%Y-%m-%dT%H:%M:%S'))


def parse_bits_line(line):
    """Parse a log line emitted by varnishncsa on the bits hosts."""
    try:
        q, origin, seq_id, timestamp, client_ip = line.split()
        e = parse_qs(q)
    except (ValueError, KeyError, jsonschema.ValidationError):
        return None

    e['meta'].update({
        'truncated' : not q.endswith(';'),
        'origin'    : origin.split('.', 1)[0],
        'seqId'     : int(seq_id),
        'timestamp' : parse_ncsa_ts(timestamp),
        'clientIp'  : hashlib.sha1(client_ip.encode('utf8')).hexdigest()
    })

    return e


def get_schema(id):
    """Get schema from memory or HTTP."""
    schema = schemas.get(id)
    if schema is None:
        schema = http_get_schema(id)
        if schema is not None:
            schemas[id] = schema
    return schema


def http_get_schema(id):
    """Retrieve schema via HTTP."""
    req = urlopen(url % id)
    content = req.read(10240).decode('utf8')
    schema = json.loads(content)
    if isinstance(schema, dict):
        return schema


def iter_loglines():
    context = zmq.Context.instance()
    sock = context.socket(zmq.SUB)
    sock.connect(b'tcp://localhost:8422')
    sock.setsockopt(zmq.SUBSCRIBE, b'')
    while 1:
        yield sock.recv()


def iter_events():
    for event in iter_loglines():
        event = event.decode('utf8')
        event = parse_bits_line(event)
        if event is not None:
            yield event


def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    logger.addHandler(stderr_handler)

    return logger


if __name__ == '__main__':
    context = zmq.Context.instance()
    pub = context.socket(zmq.PUB)
    pub.bind(b'tcp://*:8484')

    for event in iter_events():
        pub.send_unicode(json.dumps(event) + '\n')
