from datetime import datetime
from unittest.mock import patch

import elasticsearch
import pytest

import squiggler as sq

from .fixtures import cluster_config, entire_config

# some sample & fake data
_NOW = datetime.now().strftime('%Y.%m.%d')

FAKE_NEW_INDEXES = [
    {
        'status': 'open',
        'index': 'sample-one'
    },
    {
        'status': 'open',
        'index': 'sample-two'
    },
]

FAKE_INDEXES = [{
    'status': 'open',
    'index': 'sample-one'
}, {
    'status': 'open',
    'index': 'sample-two'
}, {
    'status': 'open',
    'index': 'sample-three-2017.10.10-000001'
}, {
    'status': 'close',
    'index': 'sample-four'
}]

FAKE_NEW_INDEXES_STRS = sorted([index['index'] for index in FAKE_NEW_INDEXES])

FAKE_RO_PAIRS = [{
    'index': name,
    'rollover': f'{name}-{_NOW}-000001',
    'rollover_dm': f'<{name}-{{now/d}}-000001>'
} for name in FAKE_NEW_INDEXES_STRS]

FAKE_RO_TASK_GOOD = [{
    'index': 'sample-one',
    'rollover': f'sample-one-{_NOW}-000001',
    'rollover_dm': '<sample-one-{now/d}-000001>',
    'task_failures': []
}]

FAKE_RO_TASK_BAD = [{
    'index': 'sample-one',
    'rollover': f'sample-one-{_NOW}-000001',
    'rollover_dm': '<sample-one-{now/d}-000001>',
    'task_failures': [{
        'sample-failure': {}
    }]
}]

FAKE_RO_INDEXES = [{
    'index': 'sample-2017.01-000001',
    'pri.store.size': 25,
    'docs.count': 200,
    'status': 'open'
}, {
    'index': 'sample-2017.01-000002',
    'pri.store.size': 75,
    'docs.count': 100,
    'status': 'open'
}, {
    'index': 'sample-2017.01-000003',
    'pri.store.size': 0,
    'docs.count': 0,
    'status': 'close'
}]

FAKE_RO_SETTINGS = {
    'sample-2017.01-000001': {
        'settings': {
            'index': {
                'number_of_shards': 2
            }
        }
    },
    'sample-2017.01-000002': {
        'settings': {
            'index': {
                'number_of_shards': 2
            }
        }
    }
}


@patch.object(elasticsearch.client.cluster.ClusterClient, 'put_settings')
def test_allocation_toggle(mock_put, cluster_config):

    # expected payloads
    locked_payload = {
        'transient': {
            'cluster.routing.allocation.enable': 'none'
        }
    }

    unlocked_payload = {
        'transient': {
            'cluster.routing.allocation.enable': 'all'
        }
    }

    # lock
    sq.lock_allocation(cluster_config, True)
    mock_put.assert_called_with(locked_payload)

    # unlock
    sq.lock_allocation(cluster_config, False)
    mock_put.assert_called_with(unlocked_payload)


@patch.object(elasticsearch.client.CatClient, 'indices')
def test_get_new_indices(mock_cat, cluster_config):
    mock_cat.return_value = FAKE_INDEXES

    # check it
    assert sorted(sq.get_new_indices(cluster_config)) == FAKE_NEW_INDEXES_STRS
    mock_cat.assert_called_once_with(
        format='json', filter_path='**status,**index'
    )


@pytest.mark.parametrize(
    'fake_health,expected', [
        ({
            'status': 'green',
            'cluster_name': 'fake-cluster'
        }, sq.Status.green),
        ({
            'status': 'yellow',
            'cluster_name': 'fake-cluster'
        }, sq.Status.yellow),
        ({
            'status': 'red',
            'cluster_name': 'fake-cluster'
        }, sq.Status.red),
        ({}, sq.Status.fail),
    ]
)
@patch.object(elasticsearch.client.cluster.ClusterClient, 'health')
def test_check_credentials(mock_health, cluster_config, fake_health, expected):
    mock_health.return_value = fake_health
    status, error = sq.check_credentials(cluster_config)

    # check it
    mock_health.assert_called_with(format='json')
    assert status == expected


@patch.object(elasticsearch.client.indices.IndicesClient, 'create')
def test_create_rollover_indices(mock_idx, cluster_config):
    mock_idx.return_value = {}
    pairs = sq.create_rollover_indices(cluster_config, FAKE_NEW_INDEXES_STRS)

    dms = [pair['rollover_dm'] for pair in pairs]
    for name in dms:
        mock_idx.assert_any_call(name, ignore=400)

    assert pairs == FAKE_RO_PAIRS


@patch.object(elasticsearch.client.indices.IndicesClient, 'delete')
def test_delete_old_indices(mock_del, cluster_config):
    mock_del.return_value = True
    sq.delete_old_indices(cluster_config, FAKE_RO_PAIRS)

    for name in FAKE_NEW_INDEXES_STRS:
        mock_del.assert_any_call(name)


@patch.object(elasticsearch.client.Elasticsearch, 'reindex')
def test_reindex_to_rollover(mock_reindex, cluster_config):
    mock_reindex.return_value = {'task': 'bogusnodeid:bogustaskid'}

    pairs = FAKE_RO_PAIRS.copy()
    payloads = [{
        'source': {
            'index': pair['index']
        },
        'dest': {
            'index': pair['rollover']
        }
    } for pair in FAKE_RO_PAIRS]

    sq.reindex_to_rollover(cluster_config, pairs)

    for payload in payloads:
        mock_reindex.assert_any_call(
            payload, wait_for_completion=False, format='json'
        )

    for pair in pairs:
        assert 'task' in pair


def _fail_task(failed):
    return {
        'response': {
            'failures': [{'sample-failure': {}}] if failed else []
        }
    }


@pytest.mark.parametrize('fail', [False, True])
@patch.object(elasticsearch.client.tasks.TasksClient, 'get')
def test_check_for_failed_tasks(mock_get, cluster_config, fail):
    mock_get.return_value = _fail_task(fail)

    pairs = FAKE_RO_PAIRS.copy()
    sq.check_for_failed_tasks(cluster_config, pairs)

    for pair in pairs:
        mock_get.assert_any_call(task_id=pair['task'])

    if fail:
        for pair in pairs:
            assert pair['task_failures']
    else:
        for pair in pairs:
            assert not pair['task_failures']


@pytest.mark.parametrize(
    'pair,fail', [(FAKE_RO_TASK_BAD, True), (FAKE_RO_TASK_GOOD, False)]
)
@patch.object(elasticsearch.client.indices.IndicesClient, 'update_aliases')
def test_create_aliases(mock_update, cluster_config, pair, fail):
    mock_update.return_value = True

    sq.create_aliases(cluster_config, pair)

    if fail:
        mock_update.assert_not_called()
    else:
        mock_update.assert_called()


@patch.object(elasticsearch.client.CatClient, 'indices')
@patch.object(elasticsearch.client.indices.IndicesClient, 'get_settings')
def test_get_target_sizes(mock_settings, mock_cat, cluster_config):
    mock_cat.return_value = FAKE_RO_INDEXES
    mock_settings.return_value = FAKE_RO_SETTINGS

    groups = sq.get_target_sizes(cluster_config)
    assert groups['sample']['shards'] == 2
    assert groups['sample']['docs'] == 300
    assert groups['sample']['max_docs'] == 64424509440
    assert groups['sample']['human_size'] == '100.0000 Byte'
