import random
import logging
import os
import re
import sys
import socket
import traceback
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
from enum import IntEnum
from time import sleep

import bitmath
import click
import requests
import yaml
import structlog
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from requests.adapters import HTTPAdapter
from pyzabbix import ZabbixMetric, ZabbixSender


# logging
logging.basicConfig(
    format='%(message)s',
    stream=sys.stdout,
    level=logging.INFO
)

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper('iso'),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

log = structlog.get_logger('squiggler')

# shut up elasticsearch, no one asked you to speak,
structlog.get_logger('elasticsearch').setLevel(100)

__version__ = '0.2'


class Status(IntEnum):
    """
    Cluster status enum for comparisons
    """

    def __ge__(self, other):
        return self.value <= other.value

    def __le__(self, other):
        return self.value >= other.value

    def __gt__(self, other):
        return self.value < other.value

    def __lt__(self, other):
        return self.value > other.value

    green = 0
    yellow = 1
    red = 2
    fail = 3


@contextmanager
def lock_and_unlock(cluster_config):
    """
    Lock and unlock allocation for a block of functions.

    Args:
        cluster_config: cluster specific config
    """
    try:
        lock_allocation(cluster_config)
        yield
    finally:
        lock_allocation(cluster_config, lock=False)


def get_new_indices(cluster_config):
    """
    Get list of new indices.

    Args:
        cluster_config: cluster specific config

    Returns:
        Returns list of new indices from the given cluster
    """
    es = cluster_config['es']
    exclude_set = cluster_config['settings']['exclude_new_indexes']
    if isinstance(exclude_set, str):
        exclude_set = [exclude_set, ]
    exclude_set = set(exclude_set)
    indices = es.cat.indices(format='json', h='status,index', request_timeout=120)

    # Matches any index that has no date suffix
    new_indices = set([
        index['index'] for index in indices
        if re.match(r'^[0-9a-zA-Z-_]+$', index['index'])
        and index['status'] == 'open'
    ])

    new_indices = list(new_indices - exclude_set)

    if new_indices:
        log.debug(
            'found_new_indices',
            indexes=new_indices,
            cluster_name=cluster_config['name'],
            excluded_indexes=list(exclude_set)
        )

    return new_indices


def make_kibana_request(cluster_config, path, method='POST', **kwargs):
    """
    Send kibana request to path using Kibana URL & auth information, with
    default method of POST (since you'll likely be submitting info, not
    retrieving info).

    Args:
        cluster_config: cluster specific config
        path: path to hit
        method: HTTP method to use (default is 'GET')
        kwargs: args to pass to requests
    """
    session = requests.Session()
    session.auth = cluster_config['auth']
    session.mount('http', HTTPAdapter(max_retries=2))
    session.mount('https', HTTPAdapter(max_retries=2))

    kwargs.setdefault('headers', {'kbn-xsrf': 'anything'})
    kwargs.setdefault('timeout', 30)
    rv = session.request(
        method=method,
        url=f'{cluster_config["kibana_url"]}/{path}',
        auth=cluster_config['auth'],
        **kwargs
    )

    log.debug(
        'made_kibana_request',
        debug_details={
            'cluster_name': cluster_config['name'],
            'kwargs': kwargs
        }
    )

    return rv


def check_credentials(cluster_config):
    """
    Check if the credentials are even correct and add cluster name to config.

    Args:
        cluster_config: cluster specific config

    Returns:
        Status enum and dictionary of error information if applicable.
    """
    error = None
    health = {}
    try:
        es = cluster_config['es']
        health = es.cluster.health(format='json')
    except Exception as e:
        error = e

    if health:
        cluster_config['name'] = health['cluster_name']

    return Status[health.get('status', 'fail')], error


def build_auth(cluster_config):
    """
    Shorthand for creating our auth tuple.

    Args:
        cluster_config: cluster specific config

    Returns:
        Tuple in order of (user, pass)
    """
    username = cluster_config['settings']['username']
    password = cluster_config['settings']['password']
    auth = (username, password)
    return auth


# TODO: for mothership, if I'm not doing anything es related, why setup es?
def build_kibana_url(cluster_config):
    """
    Build url for talking to Kibana.

    Args:
        cluster_config: cluster specific config

    Returns:
        URL for hitting kibana
    """
    cluster_config.setdefault('kibana', {})
    kibana_config = cluster_config['kibana']
    kibana_config.setdefault('protocol', cluster_config['protocol'])
    kibana_config.setdefault('endpoint', cluster_config['endpoint'])
    kibana_config.setdefault('port', 443)
    protocol = kibana_config['protocol']
    endpoint = kibana_config['endpoint']
    port = kibana_config['port']
    return f'{protocol}://{endpoint}:{port}'


def build_url(cluster_config):
    """
    Build url for talking.

    Args:
        cluster_config: cluster specific config

    Returns:
        URL for hitting cluster
    """
    protocol = cluster_config['protocol']
    endpoint = cluster_config['endpoint']
    port = cluster_config['port']
    return f'{protocol}://{endpoint}:{port}'


def build_cluster_info(config):
    """
    Construct our cluster settings. Dumps global settings config to each
    cluster settings, however, does not overwrite local cluster settings.
    Cluster config takes precedence over global. Use global config for generic
    info to be applied by default.

    Also sets defaults for settings if they are missing, e.g. target shard size
    at 10 GiB.

    Args:
        config: global settings config

    Returns:
        Global settings config, but with each cluster config is updated with
        global settings.
    """
    global_settings = config['settings']
    cluster_settings = config['clusters']

    if 'mothership' in config['settings']:

        mothership_config = config['settings']['mothership']
        mothership_config['url'] = build_url(mothership_config)
        mothership_config['auth'] = build_auth(config)
        mothership_config['kibana_url'] = build_kibana_url(mothership_config)
        mothership_config['es'] = Elasticsearch(
            mothership_config['endpoint'],
            port=mothership_config['port'],
            use_ssl=True if mothership_config['protocol'] == 'https' else False,
            verify_certs=True,
            http_auth=mothership_config['auth']
        )

    # Setup global settings to cluster settings if they are not present
    # if they are, only update keys that do not exist
    for cluster in cluster_settings:

        if 'settings' not in cluster:
            cluster['settings'] = global_settings
            continue

        for k, v in global_settings.items():
            if k not in cluster['settings']:
                cluster['settings'][k] = v

    # Save, reuse, and/or set some basic information
    for cluster in cluster_settings:

        cluster['settings'].setdefault('create_kibana_patterns', True)
        cluster['settings'].setdefault('only_rollover', False)
        cluster['settings'].setdefault('exclude_new_indexes', [])
        cluster['url'] = build_url(cluster)
        cluster['auth'] = build_auth(cluster)
        cluster['kibana_url'] = build_kibana_url(cluster)
        cluster['es'] = Elasticsearch(
            cluster['endpoint'],
            port=cluster['port'],
            use_ssl=True if cluster['protocol'] == 'https' else False,
            verify_certs=True,
            http_auth=cluster['auth'],
            timeout=120
        )

        # We absolutely need this to do anything
        if 'target_size' not in cluster:
            cluster['target_size'] = bitmath.GiB(10)
        else:
            cluster['target_size'] = bitmath.parse_string(
                cluster['target_size']
            )

    return config


def load_config(config):
    """
    Load config, expand env vars, and load yaml. Data is read in to avoid a
    very odd problem with load_safe.

    Args:
        config: config filename

    Returns:
        Dictionary loaded from yaml file
    """
    with open(config) as f:
        data = f.read()
    data = os.path.expandvars(data)
    data = yaml.safe_load(data)
    return data


def create_rollover_indices(cluster_config, new_indices):
    """
    Creates the new rollover indices for the given indexes.

    Args:
        cluster_config: cluster specific config
        new_indices: list of new indices which need a rollover equivalent

    Returns:
        Pairs of index -> rollover index to avoid having to parse the indices
        all over again just to get rollover index information.
    """
    es = cluster_config['es']

    pairs = []
    for index in new_indices:
        rollover_index = f'{index}-{datetime.now():%Y.%m.%d}-000001'
        date_math_index = f'<{index}-{{now/d}}-000001>'

        # attempt to create, ignore if it exists, that's not a horrible thing
        rv = es.indices.create(date_math_index, ignore=400)
        if rv.get('error', None):
            log.error(
                'create_rollover_index_error',
                exc_info=rv.get('error'),
                index=index,
                rollover_index=rollover_index,
                date_math_index=date_math_index,
                rv=rv
            )

        pairs.append({
            'index': index,
            'rollover': rollover_index,
            'rollover_dm': date_math_index
        })

    log.debug(
        'made_rollover_pairs',
        pairs=pairs, cluster_name=cluster_config['name']
    )

    return pairs


def lock_allocation(cluster_config, lock=True):
    """
    Locks shard allocation so we can delete the old index without it being
    immediately recreated.

    Args:
        cluster_config: cluster specific config
        lock: lock allocation ('none'), false to unlock ('all')
    """
    payload = {
        'transient': {
            'cluster.routing.allocation.enable': 'none' if lock else 'all'
        }
    }
    es = cluster_config['es']
    es.cluster.put_settings(payload)
    status = 'locked' if lock else 'unlocked'
    log.debug(
        f'{status}_allocation',
        cluster_name=cluster_config['name']
    )


def delete_old_indices(cluster_config, pairs):
    """
    Deletes the old pre-rollover indices.

    Args:
        cluster_config: cluster specific config
        pairs: pairs from :func:`create_rollover_indices`
    """
    es = cluster_config['es']

    deleted = []
    for pair in pairs:

        index = pair['index']
        failures = pair.get('task_failures', [])

        if not failures:
            es.indices.delete(index)
            deleted.append(index)
        else:
            log.debug(
                'index_deletion_failure',
                index=index,
                cluster_name=cluster_config['name']
            )

    log.debug(
        'deleted_indexes',
        indexes=deleted,
        cluster_name=cluster_config['name']
    )


def delete_empty_indices(cluster_config):
    """
    Deletes the empty indices.

    Args:
        cluster_config: cluster specific config
    """
    es = cluster_config['es']

    indices = [
        i['index'] for i in es.cat.indices('*', format='json')
        if i['status'] == 'open' and i['docs.count'] == '0' and not re.match(".*backfill.*", i['index'])
    ]

    if indices:
        try:
            es.indices.delete(index=','.join(indices))
        except Exception as e:
            log.error('error_purging_empties', exc_info=e, indices=indices)
        else:
            log.info(
                'purged_empty_indices',
                deleted_indices=indices,
                cluster_name=cluster_config['name']
            )

    return indices


def reindex_to_rollover(cluster_config, pairs):
    """
    Reindex each old index to its corresponding new index, get the task id
    so we can wait for all the reindex tasks to complete. Add task id to
    pair information.

    Args:
        cluster_config: cluster specific config
        pairs: dictionary of old -> new names from
            :func:`create_rollover_indices`
    """
    es = cluster_config['es']

    for pair in pairs:

        src = pair['index']
        dst = pair['rollover']

        payload = {
            'source': {
                'index': src
            },
            'dest': {
                'index': dst
            }
        }

        task = es.reindex(
            payload,
            wait_for_completion=False,
            slices=10,
            format='json'
        )['task']

        pair['task'] = task

    tasks = [p['task'] for p in pairs]

    log.debug(
        'created_reindex_tasks',
        tasks=tasks,
        cluster_name=cluster_config['name']
    )


def wait_for_reindexing(cluster_config, pairs, interval=5.0):
    """
    Check up on each task, if completed remove from task list. Continue this
    until all tasks are done. Copies tasks list with shallow copy.

    Args:
        cluster_config: cluster specific config
        tasks: set of task IDs to check (from :func:`reindex_to_rollover`)
    """
    es = cluster_config['es']
    reindex_tasks = [p['task'] for p in pairs]
    while reindex_tasks:

        # Check up on each task, remove from set if they're finished
        for task in reindex_tasks:

            try:
                done = es.tasks.get(task).get('completed', True)
            except:
                done = True

            if done:
                reindex_tasks.remove(task)

        if reindex_tasks:
            sleep(interval)


def check_for_failed_tasks(cluster_config, pairs):
    """
    Check 'finished' tasks for failures. If failures are present for a task we
    add the task failures to the pair dict. With this info we'll avoid creating
    aliases for the new indexes and deleting them (and in turn spamming chat).

    Args:
        cluster_config: cluster specific config
        pairs: pairs from rollover indexes
    """
    es = cluster_config['es']

    for pair in pairs:

        task = pair['task']

        # same as before when using requests, task is gone
        try:
            task_info = es.tasks.get(task_id=task)
            if task_info['response']['failures']:
                pair['task_failures'] = task_info['response']['failures']
        except:
            pass
        finally:
            pair.setdefault('task_failures', [])


def create_aliases(cluster_config, pairs):
    """
    Create alias(es) for rollover API.

    Args:
        cluster_config: cluster specific config
        pairs: pair of old index name & rollover index name
    """
    es = cluster_config['es']

    for pair in pairs:

        failures = pair.get('task_failures', [])
        src = pair['index']
        dst = pair['rollover']

        if failures:
            log.debug(
                'skipped_alias_creation',
                index=src,
                cluster_name=cluster_config['name']
            )
            continue

        payload = {
            'actions': [
                {
                    'remove_index': {
                        'index': src
                    }
                },
                {
                    'add': {
                        'index': dst,
                        'alias': src
                    }
                }
            ]
        }

        es.indices.update_aliases(payload)


def get_target_sizes(cluster_config):
    """
    Calculate the ideal max docs per index to be used in the rollover API.

    Args:
        cluster_config: cluster specific config

    Returns:
        Dictionary of alias with doc, size, shard and max_docs size
    """
    es = cluster_config['es']

    indices = es.cat.indices(
        '*-*.*.*-*',
        format='json',
        bytes='b'
    )

    ro_settings = es.indices.get_settings(
        '*-*.*.*-*',
        format='json',
        filter_path='**.settings.index.number_of_shards'
    )

    groupings = dict()
    aliases = []

    # creates a unique list of the index aliases
    for index in indices:

        # closed indexes don't have any size information
        if index['status'] == 'close':
            continue

        short_name = re.match(r'(?P<alias>[\w\-]+)\-(?:\d+\.?){2,3}\-\d+', index['index']).group('alias')
        aliases.append(short_name)

    aliases = list(set(aliases))

    # Pair down the indices list to the 10 most recent
    filtered_indices = []
    for alias in aliases:
        pattern = re.compile(r"{0}\-(?:\d+\.?){{2,3}}\-\d+".format(alias))
        matched_indices = []
        for index in indices:
            if index['status'] == 'close':
                continue
            if pattern.match(index['index']):
                matched_indices.append(index)
                sorted_indices = sorted(matched_indices, key=lambda k: k['index'])
                shortened_indices = sorted_indices[-11:]

        for si in shortened_indices:
            filtered_indices.append(si)

    for index in filtered_indices:
        alias = re.match(
            r'(?P<alias>[\w\-]+)\-(?:\d+\.?){2,3}\-\d+',
            index['index']
        ).group('alias')

        shards = int(
            ro_settings[index['index']]['settings']['index']
            ['number_of_shards']
        )

        if alias not in groupings:
            groupings[alias] = {
                'index_count': 1,
                'docs': int(index['docs.count']),
                'size': bitmath.Byte(float(index['pri.store.size'])),
                'shards': shards
            }
        else:
            groupings[alias]['index_count'] += 1
            groupings[alias]['docs'] += int(index['docs.count'])
            groupings[alias]['size'] += bitmath.Byte(
                float(index['pri.store.size'])
            )

    to_be_removed = []
    for alias, v in groupings.items():
        if v['docs'] == 0:
            to_be_removed.append(alias)
            continue

        v['avg_docs'] = v['docs'] / v['index_count']
        v['docs_per_gig'] = v['docs'] / v['size'].GiB.value
        v['max_docs'] = int(v['docs_per_gig'] * v['shards'] * cluster_config['target_size'])
        v['max_age'] = cluster_config['settings']['max_age']
        v['human_size'] = v['size'].best_prefix().format('{value:0.4f} {unit}')
        v['avg_human_size'] = (v['size'] / v['index_count']).best_prefix().format('{value:0.4f} {unit}')
        v['size'] = int(v['size'].to_Byte().value)

    for alias in to_be_removed:
        del groupings[alias]

    return groupings


def submit_rollover_requests(cluster_config, rollover_data):
    """
    Submit requests to rollover API with given rollover data for each alias.

    Args:
        cluster_config: cluster specific config
        rollover_data: rollover information from :func:`get_target_sizes`
    """
    es = cluster_config['es']
    aliases = set(_['alias'] for _ in es.cat.aliases(format='json'))
    skipped_aliases = set()

    for alias, v in rollover_data.items():

        # avoid issue where alias does not exist but the indexes do
        if alias not in aliases:
            skipped_aliases.add(alias)
            continue

        max_age = v['max_age']
        max_docs = v['max_docs']

        payload = {
            'conditions': {
                'max_age': max_age,
                'max_docs': max_docs
            }
        }

        # TODO: maybe add a check to see if there even exists an alias
        # before we conduct a rollover request so we don't error out
        try:
            rollover_result = es.indices.rollover(
                alias,
                body=payload,
                request_timeout=120
            )
            v['rollover_result'] = rollover_result
        except Exception as e:
            log.error(
                'rollover_error',
                exc_info=e,
                payload=payload,
                alias=alias,
                cluster_name=cluster_config['name']
            )
        finally:
            v.setdefault('rollover_result', {})
            log.debug('rollover_result', alias=alias, result=v['rollover_result'])

    skipped_aliases = list(skipped_aliases)
    if skipped_aliases:
        log.error(
            'rollover_missing_alias',
            missing_aliases=list(skipped_aliases),
            current_aliases=list(aliases),
            cluster_name=cluster_config['name']
        )

    return skipped_aliases


def create_kibana_patterns(cluster_config, pairs):
    """
    Create Kibana index patterns via the .kibana index.

    Args:
        cluster_config: cluster specific config
        pairs: indices pairs from :func:`create_rollover_indices`
    """
    for pair in pairs:
        title = f'{pair["index"]}-*'

        payload = {
            'title': title,
            'timeFieldName': '@timestamp',
            'notExpandable': True
        }

        # Literally anything: https://github.com/elastic/kibana/issues/3709
        headers = {'kbn-xsrf': 'anything'}

        tries = 0
        while tries < 3:
            rv = make_kibana_request(
                cluster_config,
                f'es_admin/.kibana/index-pattern/{title}/_create',
                json=payload,
                params={'op_type': 'create'},
                headers=headers
            )
            tries += 1

            if rv.status_code != requests.codes.ok:
                log.warning(
                    'error_creating_kibana_pattern',
                    pattern=title,
                    rv=rv.text,
                    cluster_name=cluster_config['name'],
                    num_of_tries=tries
                )
            else:
                if rv.json()['created']:
                    break
                log.warning(
                    'unable_to_create_pattern',
                    pattern=title,
                    rv=rv.json(),
                    cluster_name=cluster_config['name'],
                    num_of_tries=tries
                )

        log.debug(
            'sent_kibana_request',
            pattern=title,
            cluster_name=cluster_config['name']
        )


def send_to_hipchat(results, config):
    """
    Send results to Hipchat URL (room, person, etc).

    Args:
        results: results from the complete evaluation of snapshots statuses
        config: global settings config
    """

    hc_url = config['notifiers']['hipchat']['url']
    hc_token = config['notifiers']['hipchat']['token']
    payload = {
        'message': None,
        'message_format': 'html',
        'notify': False,
        'from': 'Squiggler'
    }
    headers = {
        'Authorization': f'Bearer {hc_token}'
    }

    m_kbn_link = None
    if 'mothership' in config['settings']:
        m_ep = config['settings']['mothership']['endpoint']
        m_kbn = config['settings']['mothership']['kibana_url']
        m_kbn_link = f'{m_kbn}/app/kibana#/discover'

    for endpoint, cluster_results in results.items():

        indices = [
            p['index'] for p in cluster_results.get('_raw_pairs', [])
            if not p['task_failures']
        ]
        if indices:
            message = f'Created rollover indexes on cluster ' \
                f'{endpoint} for the following indices: ' \
                f'{", ".join(indices)}'

            if m_kbn_link:
                m_ep = config['settings']['mothership']['endpoint']
                m_pairs = [
                    pair['index'] for pair
                    in cluster_results['supersearch_pairs']
                ]
                m_pairs = [f"<a href=\"{m_kbn_link}?_a=(index:'{index}')\">{index}</a>" for index in m_pairs]
                message = f'Created rollover indexes on <a href="{m_ep}">{m_ep}</a> for the following indices: {", ".join(m_pairs)}'

            cluster_payload = payload
            cluster_payload['message'] = message

            requests.post(
                hc_url,
                json=cluster_payload,
                headers=headers,
                timeout=10
            )


def send_to_slack(results, config):
    """
    Send to slack (soon).

    Args:
        results: results from run of squiggler
        config: global settings config
    """
    slk_url = config['notifiers']['slack']['url']
    payload = {
        'text': None
    }

    m_kbn_link = None
    if 'mothership' in config['settings']:
        m_kbn = config['settings']['mothership']['kibana_url']
        m_kbn_link = f'{m_kbn}/app/kibana#/discover'

    for endpoint, cluster_results, in results.items():

        indices = [
            p['index'] for p in cluster_results.get('_raw_pairs', [])
            if not p['task_failures']
        ]

        c_kbn = cluster_results['_config']['kibana_url']
        c_kbn_link = f'{c_kbn}/app/kibana#/discover'

        if indices:

            pairs = [
                f"<{c_kbn_link}?_a=(index:'{index}')|{index}>" for index
                in indices
            ]

            if m_kbn_link:
                m_pairs = [
                    pair['kibana'] for pair
                    in cluster_results['supersearch_pairs']
                ]
                pairs = [f"<{m_kbn_link}?_a=(index:'{index}')|{index}>" for index in m_pairs]

            emoji = random.choice('💪🤗🍕🐒✌️🎉👉👌😎🚀👽')
            message = f'{emoji} New index patterns made for the following: {", ".join(pairs)}'
            payload['text'] = message

            logging.debug(
                'sent_slack_notification',
                cluster_name=cluster_results['name'],
                payload=message
            )

            requests.post(
                slk_url,
                json=payload,
                timeout=10
            )


def send_to_stdout(results, config):
    """
    Dump results to stdout, in this case it will be picked up by Elasticsearch
    in JSON format.

    Args:
        results: results from run of squiggler
    """
    for cluster, values in results.items():

        details = {
            'found_indexes': values.get('indices', []),
            'good_tasks': values.get('good_tasks', []),
            'failed_tasks': values.get('failed_tasks', []),
            'rolled_indexes': values.get('pairs', []),
            'rolled_sizes': values.get('rollover', {}),
            'skipped_aliases': values.get('skipped', []),
            'elapsed_time': values['elapsed_time'],
            'purged_indexes': values.get('purged', []),
            'notes': values.get('extra', [])
        }

        log.info(
            'completed_run',
            cluster_name=values['name'],
            details=details
        )


def send_to_graphite(results, config):
    """
    Send to graphite.

    Args:
        results: results from run of squiggler
        config: global settings config
    """
    url = config['notifiers']['graphite']['url']
    port = int(config['notifiers']['graphite'].get('port', 2003))
    prefix = config['notifiers']['graphite']['prefix']
    chunk_size = int(config['notifiers']['graphite'].get('chunk_size', 100))
    ts = datetime.now().strftime('%s')

    metrics = []
    for cluster, values in results.items():

        for alias, ro_values in values.get('rollover', {}).items():

            metric = '.'.join([
                prefix,
                values['name'],
                'squiggler',
                'rolled_sizes',
                alias,
                'max_docs',
            ])
            metric = f'{metric} {float(ro_values["max_docs"])} {ts}'

            metrics.append(metric)

    chunks = [
        metrics[x:x + chunk_size]
        for x in range(0, len(metrics), chunk_size)
    ]

    import socket

    exc = []
    for i, chunk in enumerate(chunks, 1):
        message = '\n'.join(chunk)
        message += '\n'
        try:
            sock = socket.socket()
            sock.connect((url, port))
            sock.sendall(message.encode('utf-8'))
        except Exception as e:
            exc.append({
                'message': f'Exception while sending chunk {i}/{len(chunks)}',
                'error': traceback.format_exc()
            })
        finally:
            sock.close()

    if exc:
        log.error(
            'graphite_sending_problem',
            exc_info=exc
        )


def send_to_zabbix(results, config):
    """
    Send status to zabbix

    Args:
        config: configuration dict
    """
    zabbix_name = config['notifiers']['zabbix']['name']
    zabbix_url = config['notifiers']['zabbix']['url']
    zabbix_port = int(config['notifiers']['zabbix'].get('port', 10051))
    zabbix_key = config['notifiers']['zabbix']['key']
    status = 1

    metric = [ZabbixMetric(
       zabbix_name,
       zabbix_key,
       status
    )]

    try:
        zabbix_sender = ZabbixSender(zabbix_url, zabbix_port)
        zabbix_sender.send(metric)
    except Exception as e:
        log.error('zabbix_sending_problem', exc_info=e)


def send_to_notifiers(results, config):
    """
    Whip through each notifier and do the thing.

    Note:
        This could be changed to iterate and submit information for each
        cluster utilizing the cluster specific settings, but there's no
        practical need for this behavior.

        Right now the only cluster specific information that we would possibly
        need is the login information for retrieving snapshot info.

    Args:
        results: results from rollover steps
        config: global settings config (used for some notifiers)
    """

    # Map out notifier to func
    notifier_map = {
        'zabbix': send_to_zabbix,
        'graphite': send_to_graphite,
        'hipchat': send_to_hipchat,
        'slack': send_to_slack,
        'stdout': send_to_stdout
    }

    # Run each notifier as they come in
    if 'notifiers' in config and config['notifiers']:
        notifiers = config['notifiers']
        for notifier in notifiers:
            try:
                notifier_map[notifier](results, config)
            except Exception as e:
                log.error(
                    f'{notifier}_sending_problem',
                    exc_info=e
                )
    else:
        # You dummy, you didn't set any outputs
        log.warning('no_notifiers_set', description='Using stdout instead!')
        send_to_stdout(results)


def do_new_indices(cluster_config):
    # Setup results for cluster
    cluster_results = {}
    cluster_results['name'] = cluster_config['name']
    cluster_results['cluster_status'] = cluster_config['status']
    cluster_results['extra'] = []

    if cluster_config['settings']['only_rollover']:
        cluster_results['extra'] = ['set to only perform rollover']
        return cluster_results

    # If nothing to operate on, skip everything
    indices = get_new_indices(cluster_config)
    if not indices:
        log.debug(
            'no_new_indexes_found',
            cluster_name=cluster_config['name']
        )
        cluster_results['extra'].append('no new indexes found')
    elif cluster_config['status'] >= Status.yellow:

        # Only continue if green, cannot do anything if we're not stable
        indices_pairs = create_rollover_indices(cluster_config, indices)

        reindex_to_rollover(cluster_config, indices_pairs)
        wait_for_reindexing(cluster_config, indices_pairs)
        check_for_failed_tasks(cluster_config, indices_pairs)

        # Delete indexes & create aliases for successful tasks
        good_pairs = [
            p for p in indices_pairs
            if not p['task_failures']
        ]
        create_aliases(cluster_config, good_pairs)

        if cluster_config['settings']['create_kibana_patterns']:
            create_kibana_patterns(cluster_config, good_pairs)

        # TODO: Need trap to always unlock alloc if we crap out
        # context manager helps, but need a sure fire way to always unlock

        # Create the pairs on the mothership
        if 'mothership' in cluster_config['settings']:
            supersearch_pairs = deepcopy(good_pairs)
            supersearch_pairs = [
                p for p in supersearch_pairs
                if not p['task_failures']
            ]
            cluster_results['supersearch_pairs'] = supersearch_pairs

            for pair in supersearch_pairs:
                pair['index'] = f'{cluster_config["name"]}:{pair["index"]}'
                pair['kibana'] = f'{pair["index"]}-*'

            if cluster_config['settings']['create_kibana_patterns']:
                create_kibana_patterns(
                    cluster_config['settings']['mothership'],
                    supersearch_pairs
                )

        # Construct our results
        cluster_results['_raw_pairs'] = indices_pairs
        cluster_results['indices'] = indices
        cluster_results['pairs'] = [
            [p['index'], p['rollover']] for p in indices_pairs
            if not p['task_failures']
        ]
        cluster_results['good_tasks'] = [
            p['task'] for p in indices_pairs
            if not p['task_failures']
        ]
        cluster_results['failed_tasks'] = [
            {p['task']: p['task_failures']} for p in indices_pairs
            if p['task_failures']
        ]

    return cluster_results


def do_rollover(cluster_config):
    rollover_data = get_target_sizes(cluster_config)
    skipped = submit_rollover_requests(cluster_config, rollover_data)

    # Don't include info for skipped aliases, they aren't submitted anyhow
    for alias in skipped:
        rollover_data.pop(alias)

    return rollover_data, skipped


@click.command()
@click.version_option(version=__version__)
@click.argument(
    'config',
    default='config.yaml',
    type=click.Path(exists=True, readable=True)
)
@click.option(
    '-d',
    '--debug',
    is_flag=True,
    help="Don't send info, show everything"
)
def main(config, debug):
    """
    Check each cluster for indexes which satisfy the requirements for rollover.
    """

    if debug:
        log.setLevel(logging.DEBUG)

    # Build settings
    config = load_config(config)
    config = build_cluster_info(config)

    # Check mothership
    if 'mothership' in config['settings']:
        mothership_config = config['settings']['mothership']

        status, error = check_credentials(mothership_config)
        if status < Status.yellow:
            log.error('mothership_trouble', error=error)
            sys.exit(0)

    # Check credentials for each cluster
    for cluster_config in config['clusters']:

        status, error = check_credentials(cluster_config)
        if status < Status.yellow:
            log.error(
                'cluster_credentials_check_failure',
                cluster_name=cluster_config['endpoint'],
                error=error
            )
            sys.exit(0)

        cluster_config['status'] = status

    results = {}
    for cluster_config in config['clusters']:

        # Do new indices first
        start_time = datetime.now()
        results[cluster_config['endpoint']] = {}
        cluster_results = results[cluster_config['endpoint']]

        # Purge empty indices before we do anything
        cluster_results['purged'] = delete_empty_indices(cluster_config)
        cluster_results.update(do_new_indices(cluster_config))
        cluster_results['_config'] = cluster_config

        # Finally, get all rollover indexes, get the max docs size
        # and hit the rollover API with the appropriate size
        _roll = do_rollover(cluster_config)
        cluster_results['rollover'] = _roll[0]
        cluster_results['skipped'] = _roll[1]

        cluster_results['elapsed_time'] = (datetime.now() - start_time) \
            .total_seconds()

        if cluster_config['status'] != Status.green:
            cluster_results['extra'].append(
                'status not green, skipped creation of rollover indexes'
            )

    # Send results to hipchat, stdout, slack, etc
    send_to_notifiers(results, config)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.error('unexpected_error', exc_info=e)
        sys.exit(0)
