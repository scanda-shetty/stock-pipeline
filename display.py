import os
from flask import Flask, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

PG_CONN_INFO = dict(
    dbname=os.getenv("POSTGRES_DB", "stocks"),
    user=os.getenv("POSTGRES_USER", "stocks_user"),
    password=os.getenv("POSTGRES_PASSWORD", "supersecret"),
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
)

app = Flask(__name__)

def get_latest_stock_data(limit=10):

    conn = None
    try:
        conn = psycopg2.connect(**PG_CONN_INFO)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM stock_prices ORDER BY date DESC LIMIT {limit};")
            rows = cur.fetchall()
            return rows if rows else []
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()

@app.route("/")
def display_table():
    data = get_latest_stock_data()
    return jsonify(data)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3001)

