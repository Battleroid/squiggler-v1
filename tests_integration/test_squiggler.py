import json
import logging
import os

import pytest
from click.testing import CliRunner
from elasticsearch import Elasticsearch

from squiggler import main

squiggler_config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'sample.yaml'
)


def squiggler_run(*args):
    runner = CliRunner()
    return runner.invoke(main, args)


def test_squiggler():

    es = Elasticsearch('localhost', port=9200, use_ssl=False)

    initial_run = squiggler_run('--debug', squiggler_config)
    assert initial_run.exit_code == 0

    # Check if the alias is made, old index gone
    assert es.indices.exists_alias(name='sample')

    # You cannot use a HEAD or exists() here, the alias will
    # point to the current rollover index which generates a false
    # positive
    assert len(es.indices.get('sample*')) == 1

    import time
    # Was the pattern made in kibana?
    patterns = es.search(
        body={'query': {
            'match': {
                'title': 'sample-*'
            }
        }},
        doc_type='index-pattern',
        index='.kibana'
    )['hits']['hits']
    for _ in range(10):
        time.sleep(1)
        patterns = es.search(
            body={'query': {
                'match': {
                    'title': 'sample-*'
                }
            }},
            doc_type='index-pattern',
            index='.kibana'
        )['hits']['hits']
        if patterns:
            break
    assert len(patterns) == 1
