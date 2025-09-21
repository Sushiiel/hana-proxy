# hana-proxy app.py
# Minimal Flask proxy that must be bound to the HANA service in CF (reads VCAP_SERVICES).
# Protects endpoints with X-API-KEY header (env PROXY_API_KEY).

import os, json, traceback
from functools import wraps
from flask import Flask, request, jsonify
try:
    from hdbcli import dbapi
except Exception:
    dbapi = None

app = Flask(__name__)
API_KEY = os.environ.get("PROXY_API_KEY")  # set in CF binding or in manifest params

def require_api_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if API_KEY:
            key = request.headers.get("X-API-KEY")
            if key != API_KEY:
                return jsonify({"error":"unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

def find_hana_credentials():
    vcap = os.environ.get("VCAP_SERVICES")
    if not vcap:
        raise RuntimeError("VCAP_SERVICES not found")
    vs = json.loads(vcap)
    for svc_type, instances in vs.items():
        for inst in instances:
            if "hana" in (inst.get("label","") + inst.get("name","")).lower():
                creds = inst.get("credentials", {}) or {}
                # map common fields
                return {
                    "address": creds.get("host") or creds.get("hostname") or creds.get("address"),
                    "port": int(creds.get("port") or creds.get("httpsPort") or 443),
                    "user": creds.get("user") or creds.get("username"),
                    "password": creds.get("password") or creds.get("pwd"),
                    "encrypt": True,
                    "sslValidateCertificate": False
                }
    raise RuntimeError("HANA credentials not found in VCAP_SERVICES")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok", "hdbcli_installed": dbapi is not None})

@app.route("/products", methods=["GET"])
@require_api_key
def products():
    try:
        cfg = find_hana_credentials()
        conn = dbapi.connect(address=cfg["address"], port=cfg["port"],
                             user=cfg["user"], password=cfg["password"],
                             encrypt=cfg["encrypt"], sslValidateCertificate=cfg["sslValidateCertificate"])
        cur = conn.cursor()
        cur.execute('SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "SMART_RETAIL1"."PRODUCT_EMBEDDINGS" ORDER BY PRODUCT_ID')
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"products":[{"product_id":r[0],"name":r[1],"description":r[2]} for r in rows]})
    except Exception as e:
        return jsonify({"error":"db_error","message": str(e), "trace": traceback.format_exc()}), 500

@app.route("/product", methods=["POST"])
@require_api_key
def insert_product():
    payload = request.get_json(force=True)
    name = payload.get("name"); description = payload.get("description","")
    if not name:
        return jsonify({"error":"name required"}), 400
    try:
        cfg = find_hana_credentials()
        conn = dbapi.connect(address=cfg["address"], port=cfg["port"],
                             user=cfg["user"], password=cfg["password"],
                             encrypt=cfg["encrypt"], sslValidateCertificate=cfg["sslValidateCertificate"])
        cur = conn.cursor()
        cur.execute('SELECT MAX(PRODUCT_ID) FROM "SMART_RETAIL1"."PRODUCT_EMBEDDINGS"')
        row = cur.fetchone(); max_id = row[0] if row and row[0] is not None else 0
        new_id = max_id + 1
        cur.execute('INSERT INTO "SMART_RETAIL1"."PRODUCT_EMBEDDINGS" (PRODUCT_ID, NAME, DESCRIPTION, VECTOR) VALUES (?,?,?,?)',
                    (new_id, name, description, None))
        conn.commit(); cur.close(); conn.close()
        return jsonify({"status":"ok","product_id":new_id}), 201
    except Exception as e:
        return jsonify({"error":"db_error","message": str(e), "trace": traceback.format_exc()}), 500

# Simple update and delete endpoints similar pattern (omitted here if you want them add later)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
