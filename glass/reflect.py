#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

import zmq
import ConfigParser

from sqlalchemy import *
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import (DatabaseError, InvalidRequestError,
                            NoSuchTableError, SQLAlchemyError)

from chronicler import get_schema


__author__ = 'Ori Livneh'


logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

config = ConfigParser.ConfigParser()
config.read('config.ini')

meta = MetaData()
engine = engine_from_config(dict(config.items('db')), echo=True)
meta.bind = engine


def get_meta_properties():
    event_meta = get_schema(4909969)
    return {('_%s' % k): v for k, v in event_meta['properties'].items()}


types = {
    'boolean' : Boolean,
    'integer' : Integer,
    'number'  : Float,
    'string'  : String(255),
}


def gen_column(name, descriptor):
    json_type = descriptor['type']
    sql_type = types.get(json_type, types['string'])
    nullable = not descriptor.get('required', False)
    return Column(name, sql_type, nullable=nullable)


def get_table(name, rev):
    try:
        return Table('%s_%s' % (name, rev), meta, autoload=True)
    except NoSuchTableError:
        return create_table(name, rev)


def create_table(name, rev):
    schema = get_schema(rev)
    table_name = '%s_%s' % (name, rev)
    properties = schema['properties']
    properties.update(get_meta_properties())
    columns = [gen_column(k, v) for k, v in properties.items()]
    columns.append(Column('id', Integer, primary_key=True))
    return Table(table_name, meta, *columns).create()


def store_event(event):
    event.update({('_' + k):v for k, v in event.pop('meta').items()})
    table = get_table(event['_schema'], event['_revision'])
    return table.insert(values=event).execute()

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect(b'tcp://localhost:8484')
socket.setsockopt(zmq.SUBSCRIBE, b'')


while 1:
    try:
        store_event(socket.recv_json())
    except SQLAlchemyError:
        logging.exception('Unable to insert row.')
