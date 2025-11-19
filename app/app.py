from flask import Flask, render_template
from db import get_db

app = Flask(__name__)

@app.route("/dashboard")
def dashboard():
    conn = get_db()
    cur = conn.cursor()

    # Daily sales
    cur.execute("SELECT full_date, total_sales FROM vw_daily_sales ORDER BY full_date")
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

    conn.close()

    print("DAILY =", daily)
    print("PAYMENT =", payment)
    print("TOP =", top_products)
    print("CATEGORY =", sales_by_category)
    print("BASKET =", basket_freq)


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
        basket_freq=basket_freq
    )


if __name__ == "__main__":
    app.run(debug=True)
