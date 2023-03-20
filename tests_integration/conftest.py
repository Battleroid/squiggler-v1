import os
import time
from datetime import datetime
from subprocess import run

import pytest
from click.testing import CliRunner
from elasticsearch import Elasticsearch

from squiggler import main

dockerfile = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'docker-compose.yaml'
)


def pytest_configure(config):
    run(f'docker-compose -f {dockerfile} up -d'.split())

    # give it some time to startup
    time.sleep(10)

    # create client
    es = Elasticsearch('localhost', use_ssl=False, port=9200)

    try:
        alive = es.ping()
        while not alive:
            time.sleep(2)
            alive = es.ping()
    except Exception:
        pass

    # setup templates for testing, or else it'll forever be yellow
    # which makes us unhappy
    template = {
        'order': 999999,
        'template': '*',
        'settings': {
            'index': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            }
        },
        'mappings': {},
        'aliases': {}
    }
    es.indices.put_template(name='default', body=template)

    # clear out the entire cluster
    for index in es.cat.indices(format='json'):
        print(f'Removing {index["index"]} ...')
        es.indices.delete(index['index'])

    # needs to be precreated
    es.indices.create(index='.tasks')

    # index some bogus docs
    print('Indexing 100 bogus docs ...')
    for x in range(100):
        es.index(
            index='sample',
            doc_type='logs',
            body={'field': f'Hello World #{x}!'}
        )


def pytest_unconfigure(config):
    run(f'docker-compose -f {dockerfile} down'.split())
    run(f'docker-compose -f {dockerfile} rm -f -v'.split())
