from flask import Flask, render_template, request, jsonify
from db import get_db

app = Flask(__name__)


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.get("/api/daily-gross-profit")
def api_daily_gross_profit():
    start = request.args.get("start")
    end = request.args.get("end")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT d.full_date, SUM(fs.gross_profit) AS total_gross_profit
        FROM fact_sales fs
        JOIN dim_date d ON fs.date_key = d.date_key
        WHERE d.full_date BETWEEN %s AND %s
        GROUP BY d.full_date
        ORDER BY d.full_date
    """, (start, end))

    rows = cur.fetchall()
    conn.close()

    return jsonify(rows)


@app.get("/api/payment-summary")
def api_payment_summary():
    start = request.args.get("start")
    end = request.args.get("end")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            pm.payment_type,
            CASE 
                WHEN SUM(fs.sales_amount) = 0 THEN 0
                ELSE ROUND(SUM(fs.gross_profit) / SUM(fs.sales_amount), 4)
            END AS margin
        FROM fact_sales fs
        JOIN dim_payment_method pm 
            ON pm.payment_method_key = fs.payment_method_key
        WHERE fs.date_key IN (
            SELECT date_key 
            FROM dim_date 
            WHERE full_date BETWEEN %s AND %s
        )
        GROUP BY pm.payment_type
        ORDER BY pm.payment_type;
    """, (start, end))

    rows = cur.fetchall()
    conn.close()
    return jsonify(rows)


@app.get("/api/top-products")
def api_top_products():
    start = request.args.get("start")
    end = request.args.get("end")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT product_name, SUM(sales_amount)
        FROM fact_sales fs
        JOIN dim_product dp ON dp.product_key = fs.product_key
        WHERE fs.date_key IN (
            SELECT date_key FROM dim_date WHERE full_date BETWEEN %s AND %s
        )
        GROUP BY product_name
        ORDER BY SUM(sales_amount) DESC
        LIMIT 5;
    """, (start, end))

    return jsonify(cur.fetchall())


@app.get("/api/category-sales")
def api_category_sales():
    start = request.args.get("start")
    end = request.args.get("end")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT dp.category, SUM(fs.sales_amount)
        FROM fact_sales fs
        JOIN dim_product dp ON dp.product_key = fs.product_key
        WHERE fs.date_key IN (
            SELECT date_key FROM dim_date WHERE full_date BETWEEN %s AND %s
        )
        GROUP BY dp.category
    """, (start, end))

    return jsonify(cur.fetchall())


@app.route("/api/daily-inventory")
def api_daily_inventory():
    start = request.args.get("start")
    end = request.args.get("end")
    warehouse = request.args.get("warehouse", type=int)
    product = request.args.get("product", type=int)

    conn = get_db()
    cur = conn.cursor()

    query = """
        SELECT d.full_date, fs.on_hand_qty
        FROM fact_daily_inventory_snapshot fs
        JOIN dim_date d ON fs.date_key = d.date_key
        WHERE d.full_date BETWEEN %s AND %s
    """
    params = [start, end]

    if warehouse:
        query += " AND fs.warehouse_key = %s"
        params.append(warehouse)
    if product:
        query += " AND fs.product_key = %s"
        params.append(product)

    query += " ORDER BY d.full_date"

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()

    # Return as JSON [{date: ..., qty: ...}, ...]
    data = [{"date": str(r[0]), "on_hand_qty": r[1]} for r in rows]
    return jsonify(data)


@app.route("/api/inventory-movement")
def api_inventory_movement():
    start = request.args.get("start")
    end = request.args.get("end")
    warehouse = request.args.get("warehouse", type=int)
    product = request.args.get("product", type=int)

    conn = get_db()
    cur = conn.cursor()

    query = """
        SELECT d.full_date, SUM(fs.quantity) AS total_qty
        FROM fact_inventory_movement fs
        JOIN dim_date d ON fs.date_key = d.date_key
        WHERE d.full_date BETWEEN %s AND %s
    """
    params = [start, end]

    if warehouse:
        query += " AND fs.warehouse_key = %s"
        params.append(warehouse)
    if product:
        query += " AND fs.product_key = %s"
        params.append(product)

    query += " GROUP BY d.full_date ORDER BY d.full_date"

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()

    data = [{"date": str(r[0]), "total_qty": r[1]} for r in rows]
    return jsonify(data)


@app.route("/api/inventory-movement-warehouse")
def api_inventory_movement_warehouse():
    start = request.args.get("start")
    end = request.args.get("end")

    conn = get_db()
    cur = conn.cursor()

    query = """
        SELECT w.warehouse_name, SUM(fs.quantity) AS total_qty
        FROM fact_inventory_movement fs
        JOIN dim_warehouse w ON fs.warehouse_key = w.warehouse_key
        JOIN dim_date d ON fs.date_key = d.date_key
        WHERE d.full_date BETWEEN %s AND %s
        GROUP BY w.warehouse_name
        ORDER BY w.warehouse_name
    """

    cur.execute(query, (start, end))
    rows = cur.fetchall()
    conn.close()

    data = [{"warehouse": r[0], "total_qty": r[1]} for r in rows]
    return jsonify(data)


@app.route("/api/inventory-movement-stacked")
def api_inventory_movement_stacked():
    start = request.args.get("start")
    end = request.args.get("end")

    conn = get_db()
    cur = conn.cursor()

    # Ambil sum per date dan per warehouse
    cur.execute("""
        SELECT d.full_date, w.warehouse_name, SUM(fs.quantity) AS total_qty
        FROM fact_inventory_movement fs
        JOIN dim_date d ON fs.date_key = d.date_key
        JOIN dim_warehouse w ON fs.warehouse_key = w.warehouse_key
        WHERE d.full_date BETWEEN %s AND %s
        GROUP BY d.full_date, w.warehouse_name
        ORDER BY d.full_date, w.warehouse_name
    """, (start, end))

    rows = cur.fetchall()
    conn.close()

    # Convert ke format: {dates: [...], datasets: [{warehouse, data: [...]}, ...]}
    data_dict = {}
    warehouses = set()
    for date, wh, qty in rows:
        if date not in data_dict:
            data_dict[date] = {}
        data_dict[date][wh] = qty
        warehouses.add(wh)

    warehouses = sorted(warehouses)
    dates = sorted(data_dict.keys())

    datasets = []
    for wh in warehouses:
        data = []
        for date in dates:
            data.append(data_dict[date].get(wh, 0))
        datasets.append({"label": wh, "data": data})

    return jsonify({"labels": dates, "datasets": datasets})


@app.route("/warehouse")
def inventory_chart():
    return render_template("warehouse.html")


@app.get("/facts")
def facts_page():
    return render_template("facts.html")


@app.get("/facts/data")
def facts_data():
    limit = request.args.get("limit", 25, type=int)

    fact_tables = [
        "fact_sales",
        "fact_promotion"
    ]

    results = {}

    conn = get_db()
    cur = conn.cursor()

    for tbl in fact_tables:
        try:
            cur.execute(f"SELECT * FROM {tbl} LIMIT %s", (limit,))
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        except Exception:
            colnames = []
            rows = []

        results[tbl] = {
            "columns": colnames,
            "rows": rows
        }

    cur.close()
    conn.close()

    return jsonify(results)


# @app.route("/warehouse")
# def warehouse():
#     return render_template("warehouse.html")


# @app.route("/warehouse/data")
# def warehouse_data():
#     limit = int(request.args.get("limit", 25))
#     conn = get_db()
#     cur = conn.cursor()

#     tables = {}

#     # Snapshot Fact
#     cur.execute(f"""
#         SELECT date_key, warehouse_key, product_key, on_hand_qty
#         -- ,reserved_qty, inbound_qty
#         FROM fact_daily_inventory_snapshot
#         ORDER BY date_key DESC
#         LIMIT {limit};
#     """)
#     rows = cur.fetchall()
#     columns = [desc[0] for desc in cur.description]
#     tables["Daily Inventory Snapshot"] = {"columns": columns, "rows": rows}

#     # Accumulation Fact
#     cur.execute(f"""
#         SELECT movement_type, date_key, warehouse_key, product_key, quantity, remarks
#         FROM fact_inventory_movement
#         ORDER BY date_key DESC
#         LIMIT {limit};
#     """)
#     rows = cur.fetchall()
#     columns = [desc[0] for desc in cur.description]
#     tables["Inventory Movement (Accumulation Fact)"] = {
#         "columns": columns, "rows": rows}

#     # Semi-additive Fact
#     cur.execute(f"""
#         SELECT warehouse_key, product_key, ending_balance, last_updated
#         FROM fact_inventory_balance
#         ORDER BY last_updated DESC
#         LIMIT {limit};
#     """)
#     rows = cur.fetchall()
#     columns = [desc[0] for desc in cur.description]
#     tables["Inventory Balance (Semi-additive Fact)"] = {
#         "columns": columns, "rows": rows}

#     conn.close()
#     return jsonify(tables)


@app.route("/dimensions")
def dimensions():
    limit = request.args.get("limit", 10, type=int)

    conn = get_db()
    cur = conn.cursor()

    # List of dimension tables
    dimension_tables = [
        "dim_date",
        "dim_store",
        "dim_product",
        "dim_customer",
        "dim_payment_method",
        "dim_promotion",
        "dim_warehouse"
    ]

    dimensions_data = {}

    for table in dimension_tables:
        cur.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        dimensions_data[table] = {"columns": colnames, "rows": rows}

    conn.close()

    return render_template("dimensions.html", dimensions_data=dimensions_data, limit=limit)


if __name__ == "__main__":
    app.run(debug=True)
