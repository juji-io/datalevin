from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import time

import pytest

from datalevin import connect, keyword, listen_db, open_kv, unlisten_db, update_kv
from datalevin.errors import DatalevinJavaError

pytestmark = pytest.mark.usefixtures("require_runtime")
INCREMENT = "(fn [old amount] (+ (long (or old 0)) (long amount)))"


def test_atomic_updates_callbacks_source_arguments_and_rollback(tmp_path):
    with open_kv(str(tmp_path / "kv")) as kv:
        kv.open_dbi("counts")
        assert update_kv(kv, "counts", "key", lambda old, amount: (old or 0) + amount,
                         ":string", ":long", 2) == ":transacted"
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(lambda _: kv.update("counts", "key", lambda old: old + 1,
                                                  ":string", ":long"), range(16)))
        assert kv.get_value("counts", "key", ":string", ":long", True) == 18
        kv.update("counts", "key", INCREMENT, ":string", ":long", 3)
        with kv.transaction() as tx:
            tx.update("counts", "key", lambda old: old + 10, ":string", ":long")
            assert tx.get_value("counts", "key", ":string", ":long", True) == 31
            tx.abort()
        assert kv.get_value("counts", "key", ":string", ":long", True) == 21
        with pytest.raises(DatalevinJavaError):
            kv.update("counts", "key", lambda _: None, ":string", ":long")

        def fail(_old):
            raise ValueError("update failed")

        with pytest.raises(DatalevinJavaError, match="update failed"):
            kv.update("counts", "key", fail, ":string", ":long")
        assert kv.get_value("counts", "key", ":string", ":long", True) == 21
        kv.open_dbi("data")
        kv.update("data", "key", lambda old: {"count": (old or {}).get("count", 0) + 1})
        kv.update("data", "key", lambda old: {"count": old["count"] + 1})
        assert kv.get_value("data", "key") == {"count": 2}
        kv.open_dbi("keywords")
        kv.update("keywords", keyword(":key"), lambda _: keyword(":value"), ":keyword", ":keyword")
        assert kv.get_value("keywords", keyword(":key"), ":keyword", ":keyword", True) == ":value"


def test_remote_updates_and_database_subscriptions(live_server):
    name = "python-notifications"
    uri = live_server.database_uri(name)
    opts = {":pool-size": 1, ":time-out": 5000}
    with connect(uri, schema={":name": {":db/valueType": ":db.type/string"}}, opts=opts) as reader, \
            connect(uri, opts=opts) as writer:
        kv = writer.datalog_kv()
        kv.open_dbi("counts")
        events = Queue()
        old_events = []
        assert reader.listen_db(old_events.append, key="watch") == "watch"

        def refresh(event):
            events.put((event, reader.query("[:find [?name ...] :where [?e :name ?name]]")))

        assert listen_db(reader, refresh, key="watch") == "watch"
        writer.transact([{":db/id": -1, ":name": "first"}])
        event, names = events.get(timeout=5)
        assert event == {":type": ":db-changed", ":db-name": name}
        assert "first" in names
        assert old_events == []
        assert kv.update("counts", "key", INCREMENT, ":string", ":long", 4) == ":transacted"
        events.get(timeout=5)
        with kv.transaction() as tx:
            update_kv(tx, "counts", "key", INCREMENT, ":string", ":long", 5)
            tx.abort()
        time.sleep(0.2)
        assert events.empty()
        assert kv.get_value("counts", "key", ":string", ":long", True) == 4
        unlisten_db(reader, "watch")
        writer.transact([{":db/id": -2, ":name": "second"}])
        time.sleep(0.2)
        assert events.empty()
        assert reader.listen_db(events.put) is not None
        writer.transact([{":db/id": -3, ":name": "third"}])
        events.get(timeout=5)
        reader.close()
        assert not reader._db_listeners
        writer.transact([{":db/id": -4, ":name": "fourth"}])
        time.sleep(0.2)
        assert events.empty()
