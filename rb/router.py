from weakref import ref as weakref

from rb.ketama import Ketama
from rb.utils import text_type, bytes_type, integer_types, crc32
from rb._rediscommands import COMMANDS


class UnroutableCommand(Exception):
    """Raised if a command was issued that cannot be routed through the
    router to a single host.
    """


class BadHostSetup(Exception):
    """Raised if the cluster's host setup is not compatible with the
    router.
    """


def extract_keys(args, key_spec):
    first, last, step = key_spec

    rv = []
    for idx, arg in enumerate(args, 1):
        if last >= 0 and idx > last:
            break
        if idx >= first and ((idx - first) % step) == 0:
            rv.append(arg)
    return rv


def assert_gapless_hosts(hosts):
    if not hosts:
        raise BadHostSetup("No hosts were configured.")
    for x in range(len(hosts)):
        if hosts.get(x) is None:
            raise BadHostSetup(
                'Expected host with ID "%d" but no such ' "host was found." % x
            )


class BaseRouter(object):
    """Baseclass for all routers.  If you want to implement a custom router
    this is what you subclass.
    """

    def __init__(self, cluster):
        # this is a weakref because the router is cached on the cluster
        # and otherwise we end up in circular reference land and we are
        # having problems being garbage collected.
        self._cluster = weakref(cluster)

    @property
    def cluster(self):
        """Reference back to the :class:`Cluster` this router belongs to."""
        rv = self._cluster()
        if rv is None:
            raise RuntimeError("Cluster went away")
        return rv

    def get_key(self, command, args):
        """Returns the key a command operates on."""
        spec = COMMANDS.get(command.upper())

        if spec is None:
            raise UnroutableCommand(
                'The command "%r" is unknown to the '
                "router and cannot be handled as a "
                "result." % command
            )

        if "movablekeys" in spec["flags"]:
            raise UnroutableCommand(
                'The keys for "%r" are movable and '
                "as such cannot be routed to a single "
                "host."
            )

        keys = extract_keys(args, spec["key_spec"])
        if len(keys) == 1:
            return keys[0]
        elif not keys:
            raise UnroutableCommand(
                'The command "%r" does not operate on a key which means '
                "that no suitable host could be determined.  Consider "
                "using a fanout instead."
            )

        raise UnroutableCommand(
            'The command "%r" operates on multiple keys (%d passed) which is '
            "something that is not supported." % (command, len(keys))
        )

    def get_host_for_command(self, command, args):
        """Returns the host this command should be executed against."""
        return self.get_host_for_key(self.get_key(command, args))

    def get_host_for_key(self, key):
        """Perform routing and return host_id of the target.

        Subclasses need to implement this.
        """
        raise NotImplementedError()


class ConsistentHashingRouter(BaseRouter):
    """Router that returns the host_id based on a consistent hashing
    algorithm.  The consistent hashing algorithm only works if a key
    argument is provided.

    This router requires that the hosts are gapless which means that
    the IDs for N hosts range from 0 to N-1.
    """

    def __init__(self, cluster):
        BaseRouter.__init__(self, cluster)
        self._host_id_id_map = dict(self.cluster.hosts.items())
        self._hash = Ketama(self._host_id_id_map.values())
        assert_gapless_hosts(self.cluster.hosts)

    def get_host_for_key(self, key):
        if isinstance(key, unicode):
            k = key.encode('utf-8')
        else:
            k = str(key)
        rv = self._hash.get_node(k)
        if rv is None:
            raise UnroutableCommand("Did not find a suitable " "host for the key.")
        return rv


class PartitionRouter(BaseRouter):
    """A straightforward router that just individually routes commands to
    single nodes based on a simple ``crc32 % node_count`` setup.

    This router requires that the hosts are gapless which means that
    the IDs for N hosts range from 0 to N-1.
    """

    def __init__(self, cluster):
        BaseRouter.__init__(self, cluster)
        assert_gapless_hosts(self.cluster.hosts)

    def get_host_for_key(self, key):
        if isinstance(key, text_type):
            k = key.encode("utf-8")
        elif isinstance(key, integer_types):
            k = text_type(key).encode("utf-8")
        else:
            k = bytes_type(key)
        return crc32(k) % len(self.cluster.hosts)
