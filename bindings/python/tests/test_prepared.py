import pytest

from datalevin import (
    PreparedRead, connect, execute_prepared, index_attr, open_kv,
    prepare_get_value, prepare_pull, prepare_query, q, schema_attr, transact_ack,
)

pytestmark = pytest.mark.usefixtures("require_runtime")


def schema():
    return {
        ":key": schema_attr(value_type=":db.type/long", unique=":db.unique/identity"),
        ":name": schema_attr(value_type=":db.type/string"),
        ":body": schema_attr(value_type=":db.type/string", no_index=True),
    }


def test_prepared_datalog_and_index_backfill(tmp_path):
    with connect(str(tmp_path / "db"), schema=schema()) as conn:
        reports = []
        listener = conn.listen(reports.append)
        assert transact_ack(conn, [{":db/id": 1, ":key": 1, ":name": ":literal",
                                    ":body": "hidden"}], {"source": "test"}) == ":transacted"
        conn.unlisten(listener)
        assert len(reports) == 1
        assert reports[0][":tx-meta"] == {"source": "test"}
        pull = prepare_pull(conn, [":name", ":body"], {})
        assert isinstance(pull, PreparedRead)
        assert pull([":key", 1]) == {":name": ":literal", ":body": "hidden"}
        assert conn.prepare_pull("[:name]").execute(1) == {":name": ":literal"}
        query = prepare_query(conn, "[:find ?name . :in $ ?key :where "
                                    "[?e :key ?key] [?e :name ?name]]")
        assert execute_prepared(query, [1]) == ":literal"
        assert query([2]) is None
        entity, name = q.var("e"), q.var("name")
        typed = q.query(find=q.scalar(entity), inputs=[q.DB, name],
                        where=[q.datom(entity, "name", name)])
        assert conn.prepare_query(typed)([":literal"]) == 1
        window = conn.prepare_query(
            "[:find ?key :in $ ?start ?limit ?offset :where [?e :key ?key] "
            "[(>= ?key ?start)] :order-by ?key :limit ?limit :offset ?offset]")
        conn.transact_ack([{":db/id": 2, ":key": 2, ":name": "second"}])
        assert window([1, 1, 1]) == [[2]]
        assert window([1, 2, 0]) == [[1], [2]]
        conn.transact_ack([[":db/add", 1, ":name", "updated"]])
        assert pull(1)[":name"] == query([1]) == "updated"
        with pytest.raises(Exception, match="index-attr"):
            conn.query("[:find ?e :where [?e :body \"hidden\"]]")
        assert ":db/noindex" not in index_attr(conn, ":body")[":body"]
        assert conn.index_attr(":body") == conn.schema()
        assert conn.query("[:find ?e . :where [?e :body \"hidden\"]]") == 1
        with pytest.raises(Exception):
            conn.transact_ack([{":db/id": 3, ":key": "bad"}])


def test_prepared_transaction_and_simulated_views(tmp_path):
    with connect(str(tmp_path / "db"), schema=schema()) as conn:
        conn.transact_ack([{":db/id": 1, ":key": 1, ":name": "before"}])
        pull = conn.prepare_pull("[:name]")
        simulated = conn.tx_data_to_simulated_report([[":db/add", 1, ":name", "simulated"]])[":db-after"]
        assert execute_prepared(pull, simulated, 1) == {":name": "simulated"}
        assert simulated.prepare_pull("[:name]")(1) == {":name": "simulated"}
        query = "[:find ?name . :where [1 :name ?name]]"
        assert conn.query(query) == "before"
        assert simulated.prepare_query(query)([]) == "simulated"
        assert conn.query(query) == "before"
        assert pull(1) == {":name": "before"}

        def transaction(tx):
            assert tx.transact_ack([[":db/add", 1, ":name", "staged"]]) == ":transacted"
            assert pull(tx, 1) == {":name": "staged"}
            assert tx.prepare_query("[:find ?name . :where [1 :name ?name]]")([]) == "staged"
            tx.abort_transact()

        conn.with_transaction(transaction)
        assert pull(1) == {":name": "before"}


def test_prepared_kv_types_and_transaction_views(tmp_path):
    with open_kv(str(tmp_path / "kv")) as kv:
        kv.open_dbi("data")
        kv.open_dbi("typed")
        kv.transact([[":put", "data", "key", "value"],
                     [":put", "typed", q.kw("key"), b"abc", ":keyword", ":bytes"]])
        assert prepare_get_value(kv, "data")("key") == "value"
        assert kv.prepare_get_value("typed", ":keyword", ":bytes")(q.kw("key")) == b"abc"
        read = kv.prepare_get_value("typed", ":keyword", ":bytes", ignore_key=False)
        assert read(q.kw("key")) == [":key", b"abc"]
        assert read(q.kw("missing")) is None
        with kv.transaction() as tx:
            tx.transact([[":put", "typed", q.kw("key"), b"xyz", ":keyword", ":bytes"]])
            assert execute_prepared(read, tx, q.kw("key")) == [":key", b"xyz"]
            assert tx.prepare_get_value("typed", ":keyword", ":bytes")(q.kw("key")) == b"xyz"
            tx.abort()
        assert read(q.kw("key")) == [":key", b"abc"]


def test_remote_prepared_apis(live_server):
    with connect(live_server.database_uri("prepared"), schema=schema()) as conn:
        assert conn.transact_ack([{":db/id": 1, ":key": 1, ":name": "remote", ":body": "body"}]) == ":transacted"
        pull = conn.prepare_pull("[:name]")
        query = conn.prepare_query("[:find ?name . :in $ ?key :where [?e :key ?key] [?e :name ?name]]")
        assert pull([":key", 1]) == {":name": "remote"}
        assert query([1]) == "remote"
        conn.transact_ack([[":db/add", 1, ":name", "updated"]])
        assert query([1]) == "updated"
        assert pull(1) == {":name": "updated"}
        conn.index_attr(":body")
        assert conn.query("[:find ?e . :where [?e :body \"body\"]]") == 1
        simulated = conn.tx_data_to_simulated_report(
            [[":db/add", 1, ":name", "simulated"]])[":db-after"]
        simulated_query = "[:find ?name . :where [1 :name ?name]]"
        assert simulated.prepare_query(simulated_query)([]) == "simulated"
        assert conn.query(simulated_query) == "updated"
        with pytest.raises(Exception, match="explicit view"):
            pull(conn, 1)
    with open_kv(live_server.database_uri("prepared-kv")) as kv:
        kv.open_dbi("data")
        kv.transact([[":put", "data", "key", "value"]])
        assert kv.prepare_get_value("data")("key") == "value"
