import pytest
import os

import squiggler as sq


@pytest.fixture(scope='function')
def entire_config():
    config = sq.load_config(os.path.join(os.path.dirname(__file__), 'sample.yaml'))
    config = sq.build_cluster_info(config)
    return config


@pytest.fixture(scope='function')
def cluster_config():
    _config = entire_config()['clusters'][0]
    _config['name'] = 'example-cluster'
    return _config
