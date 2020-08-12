import time
import pytest

from redis.client import Script
from redis.exceptions import ResponseError

from rb.cluster import Cluster
from rb.router import UnroutableCommand
from rb.promise import Promise
from rb.utils import text_type


def test_basic_interface():
    cluster = Cluster({
        0: {'db': 0},
        1: {'db': 2},
        2: {'db': 4, 'host': '127.0.0.1'},
    }, host_defaults={
        'password': 'pass',
    })

    assert len(cluster.hosts) == 3

    assert cluster.hosts[0].host_id == 0
    assert cluster.hosts[0].db == 0
    assert cluster.hosts[0].host == 'localhost'
    assert cluster.hosts[0].port == 6379
    assert cluster.hosts[0].password == 'pass'

    assert cluster.hosts[1].host_id == 1
    assert cluster.hosts[1].db == 2
    assert cluster.hosts[1].host == 'localhost'
    assert cluster.hosts[1].port == 6379
    assert cluster.hosts[1].password == 'pass'

    assert cluster.hosts[2].host_id == 2
    assert cluster.hosts[2].db == 4
    assert cluster.hosts[2].host == '127.0.0.1'
    assert cluster.hosts[2].port == 6379
    assert cluster.hosts[2].password == 'pass'


def test_router_access():
    cluster = Cluster({
        0: {'db': 0},
    })

    router = cluster.get_router()
    assert router.cluster is cluster
    assert cluster.get_router() is router

    cluster.add_host(1, {'db': 1})
    new_router = cluster.get_router()
    assert new_router is not router


def test_basic_cluster(cluster):
    iterations = 10000

    with cluster.map() as client:
        for x in range(iterations):
            client.set('key-%06d' % x, x)
    responses = []
    with cluster.map() as client:
        for x in range(iterations):
            responses.append(client.get('key-%06d' % x))
    ref_sum = sum(int(x.value) for x in responses)
    assert ref_sum == sum(range(iterations))


def test_basic_cluster_disabled_batch(cluster):
    iterations = 10000

    with cluster.map(auto_batch=False) as client:
        for x in range(iterations):
            client.set('key-%06d' % x, x)
    responses = []
    with cluster.map(auto_batch=False) as client:
        for x in range(iterations):
            responses.append(client.get('key-%06d' % x))
    ref_sum = sum(int(x.value) for x in responses)
    assert ref_sum == sum(range(iterations))


def make_zset_data(x):
    return [(str(i), float(i)) for i in range(x, x + 10)]


def test_simple_api(cluster):
    client = cluster.get_routing_client()
    with client.map() as map_client:
        for x in range(10):
            map_client.set('key:%d' % x, x)
            map_client.zadd('zset:%d' % x, **dict(make_zset_data(x)))

    for x in range(10):
        assert client.get('key:%d' % x) == str(x)
        assert client.zrange('zset:%d' % x, 0, -1, withscores=True) == make_zset_data(x)

    results = []  # (promise, expected result)
    with client.map() as map_client:
        for x in range(10):
            results.append((
                map_client.zrange('zset:%d' % x, 0, -1, withscores=True),
                make_zset_data(x),
            ))

    for promise, expectation in results:
        assert promise.value == expectation

    with client.map() as map_client:
        for x in range(10):
            map_client.delete('key:%d' % x)

    for x in range(10):
        assert client.get('key:%d' % x) is None


def test_routing_client_releases_connection_on_error(cluster):
    client = cluster.get_routing_client()
    with pytest.raises(ResponseError):
        client.sadd('key')

    host = cluster.get_router().get_host_for_command('sadd', ['key'])
    pool = cluster.get_pool_for_host(host)
    assert len(pool._available_connections) == pool._created_connections


def test_mapping_client_releases_connection_on_error(cluster):
    client = cluster.get_routing_client().get_mapping_client()
    client.sadd('key')
    with pytest.raises(ResponseError):
        client.join()

    host = cluster.get_router().get_host_for_command('sadd', ['key'])
    pool = cluster.get_pool_for_host(host)
    assert len(pool._available_connections) == pool._created_connections


def test_managed_mapping_client_releases_connection_on_error(cluster):
    with pytest.raises(ResponseError):
        with cluster.get_routing_client().map() as client:
            client.sadd('key')

    host = cluster.get_router().get_host_for_command('sadd', ['key'])
    pool = cluster.get_pool_for_host(host)
    assert len(pool._available_connections) == pool._created_connections


def test_multi_keys_rejected(cluster):
    client = cluster.get_routing_client()

    # Okay
    with client.map() as map_client:
        map_client.delete('key')

    # Not okay
    with client.map() as map_client:
        with pytest.raises(UnroutableCommand):
            map_client.delete('key1', 'key2')


def test_promise_api(cluster):
    results = []
    with cluster.map() as client:
        for x in range(10):
            client.set('key-%d' % x, x)
        for x in range(10):
            client.get('key-%d' % x).then(lambda x: results.append(int(x)))
    assert sorted(results) == range(10)


def test_fanout_api(cluster):
    for host_id in cluster.hosts:
        client = cluster.get_local_client(host_id)
        client.set('foo', str(host_id))
        client.zadd('zset', **dict(make_zset_data(host_id)))

    with cluster.fanout(hosts='all') as client:
        get_result = client.get('foo')
        zrange_result = client.zrange('zset', 0, -1, withscores=True)

    for host_id in cluster.hosts:
        assert get_result.value[host_id] == str(host_id)
        assert zrange_result.value[host_id] == make_zset_data(host_id)


def test_fanout_key_target(cluster):
    with cluster.fanout() as client:
        c = client.target_key('foo')
        c.set('foo', '42')
        promise = c.get('foo')
    assert promise.value == '42'

    client = cluster.get_routing_client()
    assert client.get('foo') == '42'


def test_fanout_targeting_api(cluster):
    with cluster.fanout() as client:
        client.target(hosts=[0, 1]).set('foo', 42)
        rv = client.target(hosts='all').get('foo')

    assert rv.value.values().count('42') == 2

    # Without hosts this should fail
    with cluster.fanout() as client:
        pytest.raises(RuntimeError, client.get, 'bar')


def test_emulated_batch_apis(cluster):
    with cluster.map() as map_client:
        promise = map_client.mset(dict(('key:%s' % x, x) for x in range(10)))
    assert promise.value is None
    with cluster.map() as map_client:
        promise = map_client.mget(['key:%s' % x for x in range(10)])
    assert promise.value == map(text_type, range(10))


def test_batch_promise_all(cluster):
    with cluster.map() as client:
        client.set('1', 'a')
        client.set('2', 'b')
        client.set('3', 'c')
        client.set('4', 'd')
        client.hset('a', 'b', 'XXX')

    with cluster.map() as client:
        rv = Promise.all([
            client.mget('1', '2'),
            client.hget('a', 'b'),
            client.mget('3', '4'),
        ])
    assert rv.value == [['a', 'b'], 'XXX', ['c', 'd']]


def test_execute_commands(cluster):
    TestScript = Script(
        cluster.get_local_client(0),
        'return {KEYS, ARGV}',
    )

    # XXX: redis<2.10.6 didn't require that a ``Script`` be instantiated with a
    # valid client as part of the constructor, which resulted in the SHA not
    # actually being set until the script was executed. To ensure the legacy
    # behavior still works, we manually unset the cached SHA before executing.
    actual_script_hash = TestScript.sha
    TestScript.sha = None

    results = cluster.execute_commands({
        'foo': [
            ('SET', 'foo', '1'),
            (TestScript, ('key',), ('value',)),
            ('GET', 'foo'),
        ],
        'bar': [
            ('INCRBY', 'bar', '2'),
            (TestScript, ('key',), ('value',)),
            ('GET', 'bar'),
        ],
    })

    assert TestScript.sha == actual_script_hash

    assert results['foo'][0].value
    assert results['foo'][1].value == [['key'], ['value']]
    assert results['foo'][2].value == '1'

    assert results['bar'][0].value == 2
    assert results['bar'][1].value == [['key'], ['value']]
    assert results['bar'][2].value == '2'


def test_reconnect(cluster):
    with cluster.map() as client:
        for x in range(10):
            client.set(text_type(x), text_type(x))

    with cluster.all() as client:
        client.config_set('timeout', 1)

    time.sleep(2)

    with cluster.map() as client:
        rv = Promise.all([
            client.get(text_type(x))
            for x in range(10)
        ])

    assert rv.value == list(map(text_type, range(10)))
