#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from sqlalchemy import *
from sqlalchemy.schema import CreateTable


meta = MetaData()
engine = create_engine('mysql:///')

types = {
    'string'  : String(255),
    'boolean' : Boolean,
    'number'  : Integer
}


with open('models.json', 'rt') as f:
    models = json.load(f)


def gen_column(name, descriptor):
    type = descriptor['type']
    return Column(name, types[type], nullable=descriptor.get('optional', False))


def get_sql(model, schema):
    columns = (gen_column(k, v) for k, v in schema.items())
    table = Table(model, meta, *columns)
    return CreateTable(table).compile(engine)


for model, schema in models.items():
    print get_sql(model, schema)
