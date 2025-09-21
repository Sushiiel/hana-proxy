# app.py — diagnostic-friendly hana-proxy
import os
import pickle
import socket
import ssl
import traceback
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify
from hdbcli import dbapi

# env (required): HANA_HOST, HANA_PORT, HANA_USER, HANA_PASSWORD, PROXY_API_KEY, optional HANA_SCHEMA
HANA_HOST = os.environ.get("HANA_HOST") or os.environ.get("HANA_ADDRESS")
HANA_PORT = int(os.environ.get("HANA_PORT", "443"))
HANA_USER = os.environ.get("HANA_USER")
HANA_PASSWORD = os.environ.get("HANA_PASSWORD")
HANA_SCHEMA = os.environ.get("HANA_SCHEMA", "SMART_RETAIL1")
API_KEY = os.environ.get("PROXY_API_KEY")
# Control whether to validate server cert (use False for trial/dev)
SSL_VALIDATE = os.environ.get("HANA_SSL_VALIDATE", "false").lower() in ("1","true","yes")

app = Flask(__name__)

def require_api_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if API_KEY and request.headers.get("X-API-KEY") != API_KEY:
            return jsonify({"error":"unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

# ---------- Diagnostics helpers ----------
def _tcp_tls_precheck(host, port, timeout=8):
    """Return dict with dns/tcp/tls info (no creds)."""
    out = {"host": host, "port": int(port)}
    try:
        ai = socket.getaddrinfo(host, port)
        out["dns"] = {"ok": True, "addr": ai[0][4]}
    except Exception as e:
        out["dns"] = {"ok": False, "error": str(e)}
        return out

    s = None
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        out["tcp"] = {"ok": True}
    except Exception as e:
        out["tcp"] = {"ok": False, "error": str(e)}
        if s:
            try: s.close()
            except: pass
        return out

    try:
        # Use default context (validates) for diagnostics to show cert issues.
        ctx = ssl.create_default_context()
        ss = ctx.wrap_socket(s, server_hostname=host)
        out["tls"] = {"ok": True, "cipher": ss.cipher()}
        ss.close()
    except Exception as e:
        out["tls"] = {"ok": False, "error": str(e)}
        try: s.close()
        except: pass

    return out

# Use pre-check then dbapi.connect
def get_conn():
    """Attempts a pre-check (DNS/TCP/TLS) then calls dbapi.connect(). Raises informative RuntimeError on failure."""
    # 1) pre-check to produce useful logs quickly
    pre = _tcp_tls_precheck(HANA_HOST, HANA_PORT)
    if pre.get("dns", {}).get("ok") is False:
        raise RuntimeError(f"Precheck DNS failed: {pre['dns']['error']}")
    if pre.get("tcp", {}).get("ok") is False:
        raise RuntimeError(f"Precheck TCP failed: {pre['tcp']['error']}")
    if pre.get("tls", {}).get("ok") is False:
        # If cert validation fails but you intentionally disabled certificate validation, note it
        tls_err = pre['tls']['error']
        if not SSL_VALIDATE:
            # proceed but warn in logs — we'll still attempt dbapi.connect with sslValidateCertificate=False
            app.logger.warning("TLS precheck failed but SSL validation disabled. TLS error: %s", tls_err)
        else:
            raise RuntimeError(f"Precheck TLS failed: {tls_err}")

    # 2) attempt dbapi.connect; don't log credentials
    try:
        conn = dbapi.connect(
            address=HANA_HOST,
            port=HANA_PORT,
            user=HANA_USER,
            password=HANA_PASSWORD,
            encrypt=True,
            sslValidateCertificate=SSL_VALIDATE,
            # you can add other hdbcli options here if needed
        )
        return conn
    except Exception as e:
        # include traceback in logs but return a safe message to HTTP
        tb = traceback.format_exc()
        app.logger.error("dbapi.connect exception: %s\n%s", e, tb)
        # raise a RuntimeError with the original exception string but not the password
        raise RuntimeError(f"dbapi.connect failed: {str(e)}")

# ---------- Routes ----------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok", "time": datetime.utcnow().isoformat()})

@app.route("/diag", methods=["GET"])
def diag():
    """Return pre-check diagnostics (safe; no secrets)."""
    try:
        if not HANA_HOST:
            return jsonify({"error": "HANA_HOST not set in environment"}), 500
        pre = _tcp_tls_precheck(HANA_HOST, HANA_PORT)
        # include minimal env visibility (no secrets)
        return jsonify({
            "precheck": pre,
            "ssl_validate_configured": SSL_VALIDATE,
            "time": datetime.utcnow().isoformat()
        })
    except Exception as e:
        app.logger.exception("Diag exception")
        return jsonify({"error": str(e)}), 500

@app.route("/products", methods=["GET"])
@require_api_key
def list_products():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{HANA_SCHEMA}"."PRODUCT_EMBEDDINGS"')
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"products":[{"product_id":r[0],"name":r[1],"description":r[2]} for r in rows]})
    except Exception as e:
        app.logger.exception("list_products failed")
        return jsonify({"error": str(e)}), 500

@app.route("/product", methods=["POST"])
@require_api_key
def insert_product():
    payload = request.get_json(force=True)
    name = payload.get("name"); description = payload.get("description")
    if not name or not description:
        return jsonify({"error":"name and description required"}), 400
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f'SELECT MAX(PRODUCT_ID) FROM "{HANA_SCHEMA}"."PRODUCT_EMBEDDINGS"')
        row = cur.fetchone(); max_id = row[0] if row and row[0] is not None else 0
        new_id = max_id + 1
        vec_blob = pickle.dumps([])  # placeholder
        cur.execute(f'INSERT INTO "{HANA_SCHEMA}"."PRODUCT_EMBEDDINGS" (PRODUCT_ID, NAME, DESCRIPTION, VECTOR) VALUES (?,?,?,?)',
                    (new_id, name, description, vec_blob))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"status":"ok","product_id":new_id}), 201
    except Exception as e:
        app.logger.exception("insert_product failed")
        return jsonify({"error": str(e)}), 500

# (keep/update other endpoints in same pattern)...

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
