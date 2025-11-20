from flask import Flask, render_template, request, jsonify
from db import get_db

app = Flask(__name__)


@app.route("/")
def dashboard():
    conn = get_db()
    cur = conn.cursor()

    # Daily sales
    cur.execute(
        "SELECT full_date, total_sales FROM vw_daily_sales ORDER BY full_date")
    daily = cur.fetchall()

    # Payment summary
    cur.execute("SELECT payment_type, total_sales FROM vw_payment_summary")
    payment = cur.fetchall()

    # KPI
    cur.execute("SELECT SUM(sales_amount) FROM fact_sales")
    total_sales = cur.fetchone()[0]

    cur.execute("SELECT SUM(quantity) FROM fact_sales")
    total_qty = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fact_sales")
    total_trx = cur.fetchone()[0]

    # ============ NEW QUERIES ============

    # 1. Top 5 Products
    cur.execute("""
        SELECT product_name, total_sales_amount 
        FROM fact_product_sales 
        ORDER BY total_sales_amount DESC 
        LIMIT 5
    """)
    top_products = cur.fetchall()

    # 2. Daily product sales
    cur.execute("""
        SELECT full_date, product_name, sales_amount
        FROM fact_product_daily
        ORDER BY full_date, product_name
    """)
    product_daily = cur.fetchall()

    # 3. Sales by category
    cur.execute("""
        SELECT category, SUM(total_sales_amount)
        FROM fact_product_sales
        GROUP BY category
    """)
    sales_by_category = cur.fetchall()

    # 4. Product basket frequency
    cur.execute("""
        SELECT product_name, basket_count
        FROM fact_product_frequency
        ORDER BY basket_count DESC
        LIMIT 5
    """)
    basket_freq = cur.fetchall()

    # 5. Promotion Performance Summary
    cur.execute("""
        SELECT 
            p.promotion_name,
            p.promotion_type,
            COUNT(f.sales_key) AS trx_count,
            SUM(f.sales_amount) AS total_sales
        FROM fact_sales f
        JOIN dim_promotion p ON p.promotion_key = f.promotion_key
        WHERE f.promotion_key IS NOT NULL
        GROUP BY p.promotion_name, p.promotion_type
        ORDER BY total_sales DESC
    """)
    promo_summary = cur.fetchall()

    # 6. Top Promotions by Sales Uplift
    cur.execute("""
        SELECT 
            p.promotion_name,
            SUM(f.discount_amount) AS total_discount,
            SUM(f.sales_amount) AS total_sales
        FROM fact_sales f
        JOIN dim_promotion p ON p.promotion_key = f.promotion_key
        GROUP BY p.promotion_name
        ORDER BY total_sales DESC
        LIMIT 5
    """)
    top_promotions = cur.fetchall()

    # 7. Product Sales by Region
    cur.execute("""
        SELECT 
            s.region,
            SUM(f.sales_amount) AS total_sales
        FROM fact_sales f
        JOIN dim_store s ON s.store_key = f.store_key
        GROUP BY s.region
        ORDER BY total_sales DESC
    """)
    sales_by_region = cur.fetchall()

    conn.close()

    # print("DAILY =", daily)
    # print("PAYMENT =", payment)
    # print("TOP =", top_products)
    # print("CATEGORY =", sales_by_category)
    # print("BASKET =", basket_freq)
    print("PROMO SUMMARY", promo_summary)

    return render_template(
        "index.html",
        total_trx=total_trx,
        daily=daily,
        payment=payment,
        total_sales=total_sales,
        total_qty=total_qty,
        top_products=top_products,
        product_daily=product_daily,
        sales_by_category=sales_by_category,
        basket_freq=basket_freq,
        promo_summary=promo_summary,
        top_promotions=top_promotions,
        sales_by_region=sales_by_region
    )


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


@app.route("/snapshot")
def snapshot():
    conn = get_db()
    cur = conn.cursor()

    # Snapshot 1: dim_date summary
    cur.execute("""
        SELECT 
            COUNT(*) AS total_dates,
            MIN(full_date) AS start_date,
            MAX(full_date) AS end_date
        FROM dim_date;
    """)
    date_snapshot = cur.fetchone()

    # Snapshot 2: dim_store summary
    cur.execute("""
        SELECT 
            COUNT(*) AS total_stores,
            COUNT(DISTINCT city) AS unique_cities,
            COUNT(DISTINCT region) AS unique_regions
        FROM dim_store;
    """)
    store_snapshot = cur.fetchone()

    conn.close()

    return render_template(
        "snapshot.html",
        date_snapshot=date_snapshot,
        store_snapshot=store_snapshot,
    )


if __name__ == "__main__":
    app.run(debug=True)
