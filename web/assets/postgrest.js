// A PostgREST stand-in, so the page can use htmx exactly as a deployed
// pg_shell does.
//
// In production the browser calls PostgREST, which invokes the RPC and returns
// its "text/html" domain value with that content type; htmx swaps the fragment
// in. Here the same requests are answered from PGlite in the tab. htmx itself
// is untouched and unaware -- the hx-get, hx-post and hx-swap attributes on the
// page are the ones from html/index.html.

const RPC_PREFIX = "/rpc/";

// Only these two RPCs are reachable over "HTTP", matching the surface the
// deployed UI actually calls.
const ENDPOINTS = {
  latest_output_html: {
    method: "GET",
    params: ["p_user_id", "p_session_id", "p_since_id"],
    sql: "SELECT latest_output_html($1::uuid, $2::uuid, COALESCE($3, 0)::integer) AS body",
  },
  submit_command_html: {
    method: "POST",
    params: ["p_user_id", "p_session_id", "p_command"],
    sql: "SELECT submit_command_html($1::uuid, $2::uuid, $3::text) AS body",
  },
};

function routeFor(method, url) {
  let parsed;
  try {
    parsed = new URL(url, document.baseURI);
  } catch {
    return null;
  }
  if (!parsed.pathname.startsWith(RPC_PREFIX)) return null;
  const name = parsed.pathname.slice(RPC_PREFIX.length);
  const endpoint = ENDPOINTS[name];
  if (!endpoint || endpoint.method !== method.toUpperCase()) return null;
  return { name, endpoint, search: parsed.searchParams };
}

function argumentsFrom(route, body) {
  const values = new Map();
  for (const [key, value] of route.search) values.set(key, value);
  if (typeof body === "string" && body.length) {
    // htmx posts application/x-www-form-urlencoded, which PostgREST accepts
    // for RPC alongside JSON.
    if (body.trimStart().startsWith("{")) {
      for (const [key, value] of Object.entries(JSON.parse(body))) values.set(key, value);
    } else {
      for (const [key, value] of new URLSearchParams(body)) values.set(key, value);
    }
  } else if (body instanceof FormData) {
    for (const [key, value] of body) values.set(key, value);
  }
  return route.endpoint.params.map((name) => values.get(name) ?? null);
}

// PostgREST reports a raised exception as a JSON error object with the
// SQLSTATE, which is what a real deployment would put in front of the UI.
function errorPayload(error) {
  return JSON.stringify({
    code: error.code ?? "P0001",
    message: error.message ?? String(error),
    details: null,
    hint: null,
  });
}

export function installPostgrestShim(db, { onRequest } = {}) {
  const native = {
    open: XMLHttpRequest.prototype.open,
    send: XMLHttpRequest.prototype.send,
    setRequestHeader: XMLHttpRequest.prototype.setRequestHeader,
    overrideMimeType: XMLHttpRequest.prototype.overrideMimeType,
    getAllResponseHeaders: XMLHttpRequest.prototype.getAllResponseHeaders,
    getResponseHeader: XMLHttpRequest.prototype.getResponseHeader,
    abort: XMLHttpRequest.prototype.abort,
  };

  // Shadow the readonly response getters on the instance only; requests that
  // are not RPC calls keep the native XMLHttpRequest end to end.
  function settle(xhr, status, contentType, body) {
    const headers = `content-type: ${contentType}\r\n`;
    for (const [name, value] of Object.entries({
      status,
      statusText: status === 200 ? "OK" : "Bad Request",
      readyState: 4,
      response: body,
      responseText: body,
      responseType: "",
      responseURL: new URL(xhr._pgShell.url, document.baseURI).href,
    })) {
      Object.defineProperty(xhr, name, { configurable: true, value });
    }
    xhr._pgShell.headers = headers;
    if (xhr._pgShell.aborted) return;
    xhr.dispatchEvent(new ProgressEvent("loadstart"));
    xhr.dispatchEvent(new ProgressEvent("load"));
    xhr.dispatchEvent(new ProgressEvent("loadend"));
  }

  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    const route = routeFor(method, url);
    if (!route) {
      delete this._pgShell;
      return native.open.call(this, method, url, ...rest);
    }
    this._pgShell = { method, url, route, headers: "", aborted: false };
    Object.defineProperty(this, "readyState", { configurable: true, value: 1 });
  };

  XMLHttpRequest.prototype.send = function (body) {
    const pending = this._pgShell;
    if (!pending) return native.send.call(this, body);

    const values = argumentsFrom(pending.route, body);
    onRequest?.({ name: pending.route.name, method: pending.method });

    db.query(pending.route.endpoint.sql, values).then(
      (result) => settle(this, 200, "text/html; charset=utf-8", result.rows[0]?.body ?? ""),
      (error) => settle(this, 400, "application/json; charset=utf-8", errorPayload(error))
    );
  };

  XMLHttpRequest.prototype.setRequestHeader = function (name, value) {
    if (this._pgShell) return;
    return native.setRequestHeader.call(this, name, value);
  };

  XMLHttpRequest.prototype.overrideMimeType = function (type) {
    if (this._pgShell) return;
    return native.overrideMimeType.call(this, type);
  };

  XMLHttpRequest.prototype.getAllResponseHeaders = function () {
    if (this._pgShell) return this._pgShell.headers;
    return native.getAllResponseHeaders.call(this);
  };

  XMLHttpRequest.prototype.getResponseHeader = function (name) {
    if (!this._pgShell) return native.getResponseHeader.call(this, name);
    const match = this._pgShell.headers.match(new RegExp(`^${name}: (.*)$`, "im"));
    return match ? match[1].trim() : null;
  };

  XMLHttpRequest.prototype.abort = function () {
    if (!this._pgShell) return native.abort.call(this);
    this._pgShell.aborted = true;
    this.dispatchEvent(new ProgressEvent("abort"));
    this.dispatchEvent(new ProgressEvent("loadend"));
  };
}
