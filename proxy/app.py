import os, pickle
from datetime import datetime
from functools import wraps
from flask import Flask, request, jsonify
from hdbcli import dbapi

HANA_HOST = os.environ["HANA_HOST"]
HANA_PORT = int(os.environ.get("HANA_PORT", "443"))
HANA_USER = os.environ["HANA_USER"]
HANA_PASSWORD = os.environ["HANA_PASSWORD"]
HANA_SCHEMA = os.environ.get("HANA_SCHEMA", "SMART_RETAIL1")
API_KEY = os.environ["PROXY_API_KEY"]

def get_conn():
    return dbapi.connect(address=HANA_HOST, port=HANA_PORT,
                         user=HANA_USER, password=HANA_PASSWORD,
                         encrypt=True, sslValidateCertificate=False)

app = Flask(__name__)

def require_api_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if request.headers.get("X-API-KEY") != API_KEY:
            return jsonify({"error":"unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok", "time": datetime.utcnow().isoformat()})

@app.route("/products", methods=["GET"])
@require_api_key
def list_products():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute(f'SELECT PRODUCT_ID, NAME, DESCRIPTION FROM "{HANA_SCHEMA}"."PRODUCT_EMBEDDINGS"')
        rows = cur.fetchall(); cur.close(); conn.close()
        return jsonify({"products":[{"product_id":r[0],"name":r[1],"description":r[2]} for r in rows]})
    except Exception as e:
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
        return jsonify({"error": str(e)}), 500

# update/delete endpoints omitted here but same pattern...
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
