import { toEdnForm, toJava, toJs, toQueryInput } from "./convert.js";
import { Form } from "./form.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod } from "./jvm.js";
import { currentNativeRegistry, nativeMethods, withNativeScope } from "./native.js";

async function viewHandle(view) {
  const { Connection } = await import("./connection.js");
  return view instanceof Connection
    ? _BINDINGS.connectionDb(view.rawHandle())
    : view.rawHandle();
}

async function form(value) {
  return typeof value === "string" ? value : toEdnForm(value);
}

/** Reusable read metadata. No close is needed; the owning store must stay open. */
export class PreparedRead {
  constructor(handle, { query = false, typed = false } = {}) {
    this._handle = handle;
    this._query = query;
    this._typed = typed;
    this._nativeRegistry = currentNativeRegistry();
  }

  rawHandle() {
    return this._handle;
  }

  /** Execute with input, or with an explicit local view followed by input. */
  async execute(...args) {
    if (args.length !== 1 && args.length !== 2) {
      throw new TypeError("execute expects input or view, input");
    }
    let value = args.at(-1);
    if (this._query) {
      if (!Array.isArray(value)) throw new TypeError("Prepared query input must be an array");
      const { Connection } = await import("./connection.js");
      const convert = this._typed ? toJava : toQueryInput;
      value = await Promise.all(value.map(input => input instanceof Connection
        ? _BINDINGS.connectionDb(input.rawHandle()) : convert(input)));
    }
    const normalized = await toJava(value);
    const result = args.length === 2
      ? await callJavaMethod(this._handle, "executeBridge", await viewHandle(args[0]), normalized)
      : await callJavaMethod(this._handle, "executeBridge", normalized);
    return toJs(result);
  }
}

nativeMethods(PreparedRead);

/** Prepare a pull from a Connection or Database, with optional pull options. */
export async function preparePull(database, selector, opts = null) {
  return withNativeScope(database._nativeRegistry, async () => new PreparedRead(
    await _BINDINGS.preparePull(await viewHandle(database), await form(selector), opts)
  ));
}

/** Prepare a query; execute with an array of the remaining :in values. */
export async function prepareQuery(database, query) {
  return withNativeScope(database._nativeRegistry, async () => new PreparedRead(
    await _BINDINGS.prepareQuery(await viewHandle(database), await form(query)),
    { query: true, typed: query instanceof Form }
  ));
}

/** Prepare a KV point read. Types default to data; results default to values. */
export async function prepareGetValue(kv, dbiName, {
  kType = null, vType = null, ignoreKey = true
} = {}) {
  return withNativeScope(kv._nativeRegistry, async () => new PreparedRead(
    await _BINDINGS.prepareGetValue(kv.rawHandle(), dbiName, kType, vType, ignoreKey)
  ));
}

export async function executePrepared(prepared, ...args) {
  return prepared.execute(...args);
}

export async function transactAck(conn, txData, txMeta = null) {
  return conn.transactAck(txData, txMeta);
}

export async function indexAttr(conn, attr) {
  return conn.indexAttr(attr);
}
