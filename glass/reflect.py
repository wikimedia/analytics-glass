#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from chronicler import get_schema

from sqlalchemy import *
from sqlalchemy.schema import CreateTable


__author__ = 'Ori Livneh'

def get_meta_properties():
    event_meta = get_schema(4909969)
    return {('_%s' % k): v for k, v in event_meta['properties'].items()}

meta = MetaData()
engine = create_engine('mysql:///')

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


def get_sql(name, schema):
    if isinstance(schema, int):
        schema = get_schema(schema)
    properties = schema['properties']
    properties.update(get_meta_properties())
    columns = [gen_column(k, v) for k, v in properties.items()]
    columns.append(Column('id', Integer, primary_key=True))
    table = Table(name, meta, *columns)
    return CreateTable(table, bind=engine)


print get_sql('FishRetention', 4909813)
