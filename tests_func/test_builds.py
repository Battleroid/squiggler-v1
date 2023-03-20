from io import StringIO

import pytest

import squiggler as sq

from .fixtures import cluster_config, entire_config


def test_status():
    assert sq.Status.green <= sq.Status.green
    assert sq.Status.green == sq.Status.green
    assert sq.Status.green > sq.Status.red
    assert sq.Status.yellow < sq.Status.green
    assert sq.Status.yellow <= sq.Status.green


def test_build_auth(cluster_config):
    assert sq.build_auth(cluster_config) == (
        cluster_config['settings']['username'],
        cluster_config['settings']['password']
    )


def test_build_url(cluster_config):
    protocol = cluster_config['protocol']
    endpoint = cluster_config['endpoint']
    port = cluster_config['port']
    assert sq.build_url(cluster_config) == f'{protocol}://{endpoint}:{port}'


def test_build_kibana(cluster_config):
    protocol = cluster_config['kibana']['protocol']
    endpoint = cluster_config['kibana']['endpoint']
    port = cluster_config['kibana']['port']
    assert sq.build_kibana_url(cluster_config) == f'{protocol}://{endpoint}:{port}'


def test_load_config(entire_config, cluster_config):
    assert isinstance(entire_config, dict)
    assert isinstance(cluster_config, dict)


def test_load_config_fail():
    with pytest.raises(TypeError, message='Expected TypeError'):
        sq.load_config(None)

    with pytest.raises(IOError, message='Expected IOError (not found)'):
        sq.load_config('__does_not_exist__')
