#!/usr/bin/env python3
# -*- coding: utf8 -*-
import calendar
import json
import logging
import time

import jsonschema
import zmq

try:
    from urllib.parse import parse_qsl, urlencode
    from urllib.request import urlopen
except ImportError:
    from urlparse import parse_qsl
    from urllib import urlencode
    from urllib2 import urlopen



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
    types = {k: v.get('type') for k, v in properties.items()}
    return {k: casters[types.get(k, 'string')](v) for k, v in object.items()}


def parse_meta(object):
    metaschema = get_schema(4891798)
    return typecast(object, metaschema)


def parse_qs(q):
    """Parse a query string."""
    q = dict(parse_qsl(q.lstrip('?').rstrip(';')))
    meta = {}
    e = {}
    for k, v in q.items():
        if k.startswith('_'):
            meta[k] = v
        else:
            e[k] = v
    meta = parse_meta(meta)

    schema = get_schema(meta['_rv'])
    e = typecast(e, schema)
    jsonschema.validate(e, schema)

    e['meta'] = {meta_readable.get(k, k): v for k, v in meta.items()}
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
        'origin'    : origin,
        'seqId'     : int(seq_id),
        'timestamp' : parse_ncsa_ts(timestamp),
        'clientIp'  : client_ip,
    })

    return e


def get_schema(id):
    """Get schema from memory or HTTP."""
    schema = schemas.get(id)
    if schema is None:
        schema = http_get_schema(id)
        schemas[id] = schema
    return schema


def http_get_schema(id):
    """Retrieve schema via HTTP."""
    with urlopen(url % id) as req:
        content = req.read(10240).decode('utf-8')
        return json.loads(content)


def iter_loglines():
    context = zmq.Context()
    sock = context.socket(zmq.SUB)
    sock.connect('tcp://localhost:8422')
    sock.setsockopt(zmq.SUBSCRIBE, b'')
    for line in iter(sock.recv, ''):
        yield line


def iter_events():
    for event in iter_loglines():
        event = event.decode('utf-8')
        event = parse_bits_line(event)
        if event is not None:
            yield event


context = zmq.Context()
pub = context.socket(zmq.PUB)
pub.bind('tcp://*:8484')
for line in iter_events():
    pub.send(json.dumps(line) + '\n')
