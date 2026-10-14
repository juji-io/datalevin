"""Reusable query, pull, and KV point reads."""

from ._convert import to_edn_form, to_java, to_python
from ._forms import Form
from ._interop import _BINDINGS
from ._java import call_java
from ._native import current_registry, native_methods, native_scope


def _view(view):
    from .connection import Connection

    if isinstance(view, Connection):
        return _BINDINGS.connection_db(view.raw_handle())
    return view.raw_handle()


def _form(value):
    return value if isinstance(value, str) else to_edn_form(value)


@native_methods
class PreparedRead:
    """Reusable metadata; no close is needed. The owning store must stay open.

    Call ``execute(input)`` or ``execute(view, input)`` for a local pull/KV
    preparation against an explicit transaction view. Queries take a sequence
    of the remaining :in values, excluding the prepared database source.
    """

    def __init__(self, handle, *, query=False, typed=False):
        self._handle = handle
        self._query = query
        self._typed = typed
        self._native_registry = current_registry()

    def raw_handle(self):
        return self._handle

    def execute(self, *args):
        if len(args) not in (1, 2):
            raise TypeError("execute expects input or view, input")
        value = args[-1]
        if self._query:
            from .connection import Connection

            if not isinstance(value, (list, tuple)):
                raise TypeError("Prepared query input must be a list or tuple")
            value = [Connection._query_input(item, typed=self._typed) for item in value]
        value = to_java(value)
        if len(args) == 2:
            return to_python(call_java(self._handle.executeBridge, _view(args[0]), value))
        return to_python(call_java(self._handle.executeBridge, value))

    def __call__(self, *args):
        return self.execute(*args)


def prepare_pull(database, selector, opts=None):
    """Prepare a pull against a Connection or Database, with optional pull opts."""
    with native_scope(database._native_registry):
        return PreparedRead(_BINDINGS.prepare_pull(_view(database), _form(selector), opts))


def prepare_query(database, query):
    """Prepare a query; execute with the remaining input values as one sequence."""
    with native_scope(database._native_registry):
        return PreparedRead(
            _BINDINGS.prepare_query(_view(database), _form(query)),
            query=True, typed=isinstance(query, Form),
        )


def prepare_get_value(kv, dbi_name, k_type=None, v_type=None, ignore_key=True):
    """Prepare a KV point read. Types default to data; results default to values."""
    with native_scope(kv._native_registry):
        return PreparedRead(_BINDINGS.prepare_get_value(
            kv.raw_handle(), dbi_name, k_type, v_type, ignore_key,
        ))


def execute_prepared(prepared, *args):
    """Execute with input, or with an explicit local view followed by input."""
    return prepared.execute(*args)


def transact_ack(conn, tx_data, tx_meta=None):
    """Transact synchronously and return :transacted; listeners retain reports."""
    return conn.transact_ack(tx_data, tx_meta)


def index_attr(conn, attr):
    """Atomically backfill AVE indexing for a noindex attribute."""
    return conn.index_attr(attr)
