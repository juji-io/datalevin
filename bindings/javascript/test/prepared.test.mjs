import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  PreparedRead, connect, executePrepared, indexAttr, openKv, prepareGetValue,
  preparePull, prepareQuery, q, schemaAttr, transactAck
} from "../src/index.js";
import { resolveClasspath } from "../src/jvm.js";

const runtimeAvailable = (() => {
  try { resolveClasspath(); return true; } catch { return false; }
})();
const schema = {
  ":key": schemaAttr({ valueType: ":db.type/long", unique: ":db.unique/identity" }),
  ":name": schemaAttr({ valueType: ":db.type/string" }),
  ":body": schemaAttr({ valueType: ":db.type/string", noIndex: true })
};

test("prepared reads preserve inputs, refresh after writes, and support index backfill", {
  skip: !runtimeAvailable
}, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-prepared-"));
  const conn = await connect(dir, { schema });
  try {
    const reports = [];
    const listener = await conn.listen(report => reports.push(report));
    assert.equal(await transactAck(conn, [{ ":db/id": 1, ":key": 1, ":name": ":literal", ":body": "hidden" }], { source: "test" }), ":transacted");
    await conn.unlisten(listener);
    assert.equal(reports.length, 1);
    assert.deepEqual(reports[0][":tx-meta"], { source: "test" });
    const pull = await preparePull(conn, [":name", ":body"], {});
    assert.ok(pull instanceof PreparedRead);
    assert.deepEqual(await pull.execute([":key", 1]), { ":name": ":literal", ":body": "hidden" });
    assert.deepEqual(await (await conn.preparePull("[:name]")).execute(1), { ":name": ":literal" });
    const query = await prepareQuery(conn, "[:find ?name . :in $ ?key :where [?e :key ?key] [?e :name ?name]]");
    assert.equal(await executePrepared(query, [1]), ":literal");
    assert.equal(await query.execute([2]), null);
    const entity = q.var("e"), name = q.var("name");
    const typed = q.query({ find: q.scalar(entity), inputs: [q.DB, name], where: [q.datom(entity, "name", name)] });
    assert.equal(Number(await (await conn.prepareQuery(typed)).execute([":literal"])), 1);
    const window = await conn.prepareQuery("[:find ?key :in $ ?start ?limit ?offset :where [?e :key ?key] [(>= ?key ?start)] :order-by ?key :limit ?limit :offset ?offset]");
    await conn.transactAck([{ ":db/id": 2, ":key": 2, ":name": "second" }]);
    assert.deepEqual((await window.execute([1, 1, 1])).map(row => row.map(Number)), [[2]]);
    assert.deepEqual((await window.execute([1, 2, 0])).map(row => row.map(Number)), [[1], [2]]);
    await conn.transactAck([[":db/add", 1, ":name", "updated"]], { source: "test" });
    assert.equal((await pull.execute(1))[":name"], "updated");
    assert.equal(await query.execute([1]), "updated");
    await assert.rejects(conn.query('[:find ?e :where [?e :body "hidden"]]'), /index-attr/);
    assert.equal((await indexAttr(conn, ":body"))[":body"][":db/noindex"], undefined);
    assert.deepEqual(await conn.indexAttr(":body"), await conn.schema());
    assert.equal(Number(await conn.query('[:find ?e . :where [?e :body "hidden"]]')), 1);
    await assert.rejects(conn.transactAck([{ ":db/id": 3, ":key": "bad" }]));
    const simulated = (await conn.txDataToSimulatedReport([[":db/add", 1, ":name", "simulated"]]))[":db-after"];
    assert.equal((await executePrepared(pull, simulated, 1))[":name"], "simulated");
    assert.deepEqual(await (await simulated.preparePull("[:name]")).execute(1), { ":name": "simulated" });
    const simulatedQuery = "[:find ?name . :where [1 :name ?name]]";
    assert.equal(await conn.query(simulatedQuery), "updated");
    assert.equal(await (await simulated.prepareQuery(simulatedQuery)).execute([]), "simulated");
    assert.equal(await conn.query(simulatedQuery), "updated");
    assert.equal((await pull.execute(1))[":name"], "updated");
  } finally {
    await conn.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("prepared KV reads normalize typed keys and accept explicit transaction views", {
  skip: !runtimeAvailable
}, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-prepared-kv-"));
  const kv = await openKv(dir);
  try {
    await kv.openDbi("data");
    await kv.openDbi("typed");
    await kv.transact([[":put", "data", "key", "value"], [":put", "typed", q.kw("key"), Buffer.from("abc"), ":keyword", ":bytes"]]);
    assert.equal(await (await prepareGetValue(kv, "data")).execute("key"), "value");
    const opts = { kType: ":keyword", vType: ":bytes" };
    const value = await kv.prepareGetValue("typed", opts);
    assert.deepEqual(Buffer.from(await value.execute(q.kw("key"))), Buffer.from("abc"));
    const read = await kv.prepareGetValue("typed", { ...opts, ignoreKey: false });
    const pair = await read.execute(q.kw("key"));
    assert.equal(pair[0], ":key");
    assert.deepEqual(Buffer.from(pair[1]), Buffer.from("abc"));
    assert.equal(await read.execute(q.kw("missing")), null);
    const tx = await kv.transaction();
    try {
      await tx.transact([[":put", "typed", q.kw("key"), Buffer.from("xyz"), ":keyword", ":bytes"]]);
      assert.deepEqual(Buffer.from((await executePrepared(read, tx, q.kw("key")))[1]), Buffer.from("xyz"));
      assert.deepEqual(Buffer.from(await (await tx.prepareGetValue("typed", opts)).execute(q.kw("key"))), Buffer.from("xyz"));
      await tx.abort();
    } finally { await tx.close(); }
    assert.deepEqual(Buffer.from(await value.execute(q.kw("key"))), Buffer.from("abc"));
  } finally {
    await kv.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("numeric listener keys round-trip through registration and removal", {
  skip: !runtimeAvailable
}, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-listener-"));
  const conn = await connect(dir, { schema });
  try {
    for (const method of ["transact", "transactAck"]) {
      for (const requestedKey of [null, 0.5]) {
        const reports = [];
        const key = await conn.listen(report => reports.push(report), requestedKey);
        assert.equal(typeof key, "number");
        if (requestedKey !== null) assert.equal(key, requestedKey);
        await conn[method]([{ ":db/id": 1, ":key": 1, ":name": "listening" }]);
        assert.equal(reports.length, 1);
        await conn.unlisten(key);
        await conn[method]([[":db/add", 1, ":name", "removed"]]);
        assert.equal(reports.length, 1);
        await conn.unlisten(key);
      }
    }
  } finally {
    await conn.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
