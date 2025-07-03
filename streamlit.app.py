# Enhanced Retail Sales & Returns Dashboard
import streamlit as st
import pandas as pd
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import warnings
import time
warnings.filterwarnings('ignore')

# ──────────────────────────────────────────────────────────────────────────────
# Page configuration & styling
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Retail Analytics Hub",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main > div { padding-top: 2rem; }
    .stMetric { background:#f8f9fa;border:1px solid #e9ecef;padding:1rem;
                border-radius:10px;box-shadow:0 2px 4px rgba(0,0,0,0.1);}
    .metric-container {background:linear-gradient(90deg,#667eea 0%,#764ba2 100%);
                        padding:20px;border-radius:15px;margin-bottom:20px;color:white;}
    .sidebar .sidebar-content {background:linear-gradient(180deg,#667eea 0%,#764ba2 100%);}
    .stSelectbox > div > div {background-color:#f8f9fa;}
    h1,h2,h3 {color:#2c3e50;font-family:'Arial',sans-serif;}
    .chart-container {background:#fff;padding:20px;border-radius:15px;
                      box-shadow:0 4px 6px rgba(0,0,0,0.1);margin-bottom:20px;}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Session-state initialisation
# ──────────────────────────────────────────────────────────────────────────────
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()
if 'connection_status' not in st.session_state:
    st.session_state.connection_status = None
if 'cached_countries' not in st.session_state:
    st.session_state.cached_countries = None
if 'cached_products' not in st.session_state:
    st.session_state.cached_products = None

# ──────────────────────────────────────────────────────────────────────────────
# MySQL connection helpers
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection_pool():
    try:
        return mysql.connector.pooling.MySQLConnectionPool(
            pool_name="retail_pool", pool_size=5, pool_reset_session=True,
            host="127.0.0.1", port=3306,
            user="project_user", password="Iluvme199185!",
            database="retail_db", autocommit=True, connection_timeout=10,
            auth_plugin='mysql_native_password'
        )
    except Exception as e:
        st.error(f"Connection pool creation failed: {e}")
        return None

def get_connection():
    try:
        pool = get_connection_pool()
        if pool:
            conn = pool.get_connection()
            if conn.is_connected():
                return conn
        return mysql.connector.connect(
            host="127.0.0.1", port=3306,
            user="project_user", password="Iluvme199185!",
            database="retail_db", autocommit=True, connection_timeout=10,
            auth_plugin='mysql_native_password'
        )
    except mysql.connector.Error as err:
        st.session_state.connection_status = f"Database connection failed: {err}"
        return None
    except Exception as e:
        st.session_state.connection_status = f"Unexpected connection error: {e}"
        return None

def run_query(query, params=None, max_retries=3):
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_connection()
            if not conn:
                if attempt == max_retries - 1:
                    st.warning("Database connection unavailable.")
                    return pd.DataFrame()
                time.sleep(1)
                continue
            if not conn.is_connected():
                if attempt == max_retries - 1:
                    st.error("Database connection lost.")
                    return pd.DataFrame()
                time.sleep(1)
                continue

            cur = conn.cursor()
            cur.execute(query, params or ())
            columns = [d[0] for d in cur.description]
            data = cur.fetchall()
            cur.close()
            st.session_state.connection_status = "Connected"
            return pd.DataFrame(data, columns=columns)
        except mysql.connector.Error as err:
            if attempt == max_retries - 1:
                st.error(f"MySQL Error: {err}")
                st.session_state.connection_status = f"MySQL Error: {err}"
                return pd.DataFrame()
            time.sleep(1)
        except Exception as e:
            if attempt == max_retries - 1:
                st.error(f"Query failed: {e}")
                st.session_state.connection_status = f"Query failed: {e}"
                return pd.DataFrame()
            time.sleep(1)
        finally:
            if conn and conn.is_connected():
                conn.close()
    return pd.DataFrame()

# ──────────────────────────────────────────────────────────────────────────────
# Data-loading helpers
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_countries():
    q = """
    SELECT DISTINCT c.Country
    FROM customers c JOIN transactions t ON c.CustomerID=t.CustomerID
    WHERE c.Country IS NOT NULL AND c.Country!='' AND TRIM(c.Country)!=''
    ORDER BY c.Country
    """
    df = run_query(q)
    if df.empty:
        return []
    lst = df['Country'].dropna().astype(str).str.strip()
    return [c for c in lst if c and c.lower() != 'nan']

@st.cache_data(ttl=300)
def load_products():
    q = """
    SELECT DISTINCT p.Description
    FROM products p JOIN transactions t ON p.StockCode=t.StockCode
    WHERE p.Description IS NOT NULL AND p.Description!='' AND TRIM(p.Description)!=''
    ORDER BY p.Description
    LIMIT 100
    """
    df = run_query(q)
    if df.empty:
        return []
    lst = df['Description'].dropna().astype(str).str.strip()
    return [p for p in lst if p and p.lower() != 'nan']

# ──────────────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div style='text-align:center;padding:20px;background:linear-gradient(90deg,#667eea 0%,#764ba2 100%);
            border-radius:15px;margin-bottom:30px;color:white;'>
    <h1 style='margin:0;'>🏪 Retail Analytics Hub</h1>
    <p style='margin:10px 0 0;font-size:18px;'>Advanced Sales & Returns Intelligence Dashboard</p>
</div>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar — filters
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:15px;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);
                border-radius:10px;margin-bottom:20px;'><h2 style='color:white;margin:0;'>🎛️ Control Panel</h2></div>
    """, unsafe_allow_html=True)

    countries = load_countries()
    selected_countries = st.multiselect(
        "🌍 Select Countries",
        countries,
        default=[],           # ← no pre-selection
        help="Choose countries to analyze"
    )

    products = load_products()
    selected_products = st.multiselect(
        "📦 Select Products",
        products,
        default=[],           # ← no pre-selection
        help="Choose products to analyze"
    )

    st.markdown("---")
    st.markdown("### 📅 Time Period")
    st.info("Date filtering will be available when InvoiceDate is added to transactions")

    st.markdown("### 🔌 Connection Status")
    if (tc := get_connection()) and tc.is_connected():
        st.success("✅ Database Connected")
        st.session_state.connection_status = "Connected"
        tc.close()
    else:
        st.error("❌ Database Disconnected")
        st.info("Ensure MySQL server is running on localhost:3306")

    st.markdown(f"**Last Refresh:** {st.session_state.last_refresh.strftime('%H:%M:%S')}")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear(); st.cache_resource.clear()
        st.session_state.last_refresh = datetime.now()
        st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Helpers to build SQL filters
# ──────────────────────────────────────────────────────────────────────────────
def build_filters_and_params(sel_countries, sel_products):
    country_filter, product_filter, params = "", "", []
    if sel_countries:
        placeholders = ",".join(["%s"]*len(sel_countries))
        country_filter = f"AND c.Country IN ({placeholders})"
        params.extend(sel_countries)
    if sel_products:
        placeholders = ",".join(["%s"]*len(sel_products))
        product_filter = f"AND p.Description IN ({placeholders})"
        params.extend(sel_products)
    return country_filter, product_filter, params

country_filter, product_filter, params = build_filters_and_params(
    selected_countries, selected_products
)

# ──────────────────────────────────────────────────────────────────────────────
# KPI query & load
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_kpis(country_filter, product_filter, ptuple):
    params = list(ptuple) if ptuple else []
    q = f"""
    SELECT 
        COALESCE(ROUND(SUM(t.Quantity*t.UnitPrice),2),0) AS Total_Revenue,
        COALESCE(COUNT(DISTINCT t.InvoiceNo),0)           AS Total_Orders,
        COALESCE(COUNT(DISTINCT t.CustomerID),0)         AS Total_Customers,
        COALESCE(ROUND(SUM(t.Quantity*t.UnitPrice)/NULLIF(COUNT(DISTINCT t.InvoiceNo),0),2),0) AS Avg_Order_Value,
        COALESCE(SUM(t.Quantity),0)                      AS Total_Units_Sold
    FROM transactions t
    JOIN customers c ON t.CustomerID=c.CustomerID
    JOIN products  p ON t.StockCode=p.StockCode
    WHERE 1=1 {country_filter} {product_filter}
    """
    return run_query(q, params)

df_kpis = load_kpis(country_filter, product_filter, tuple(params))

# ──────────────────────────────────────────────────────────────────────────────
# KPI display
# ──────────────────────────────────────────────────────────────────────────────
if not df_kpis.empty:
    st.markdown("### 📊 Key Performance Indicators")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("💰 Total Revenue", f"€{df_kpis.at[0,'Total_Revenue']:,.2f}")
    with c2:
        st.metric("🛒 Total Orders",   f"{int(df_kpis.at[0,'Total_Orders']):,}")
    with c3:
        st.metric("👥 Unique Customers", f"{int(df_kpis.at[0,'Total_Customers']):,}")
    with c4:
        st.metric("🧾 Avg Order Value", f"€{df_kpis.at[0,'Avg_Order_Value']:,.2f}")
    with c5:
        st.metric("📦 Units Sold", f"{int(df_kpis.at[0,'Total_Units_Sold']):,}")
else:
    st.warning("⚠️ No data available for the current selection.")

# ──────────────────────────────────────────────────────────────────────────────
# Revenue by country
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_country_revenue(country_filter, product_filter, ptuple):
    params = list(ptuple) if ptuple else []
    q = f"""
    SELECT c.Country,
           COALESCE(ROUND(SUM(t.Quantity*t.UnitPrice),2),0) AS Revenue,
           COUNT(DISTINCT t.InvoiceNo)  AS Orders,
           COUNT(DISTINCT t.CustomerID) AS Customers
    FROM transactions t
    JOIN customers c ON t.CustomerID=c.CustomerID
    JOIN products  p ON t.StockCode=p.StockCode
    WHERE 1=1 {country_filter} {product_filter}
    GROUP BY c.Country
    HAVING Revenue>0
    ORDER BY Revenue DESC
    LIMIT 15
    """
    return run_query(q, params)

# ──────────────────────────────────────────────────────────────────────────────
# Return analysis helpers (updated logic)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_return_analysis(country_filter, product_filter, ptuple):
    params = list(ptuple) if ptuple else []
    q = f"""
    SELECT c.Country,
           ABS(SUM(r.Quantity)) AS Units_Returned,
           SUM(t.Quantity)      AS Units_Sold,
           ROUND(ABS(SUM(r.Quantity))/NULLIF(SUM(t.Quantity),0)*100,2) AS Return_Rate_Percent
    FROM returns r
    JOIN customers c   ON r.CustomerID=c.CustomerID
    JOIN transactions t ON r.CustomerID=t.CustomerID AND r.StockCode=t.StockCode
    WHERE t.Quantity>0
    {country_filter} {product_filter}
    GROUP BY c.Country
    HAVING SUM(t.Quantity)>0
    ORDER BY Return_Rate_Percent DESC
    LIMIT 15
    """
    return run_query(q, params)

def debug_returns_table():
    st.write("Returns Table Structure:")
    st.dataframe(run_query("DESCRIBE returns"))
    st.write("Sample Returns Data:")
    st.dataframe(run_query("SELECT * FROM returns LIMIT 10"))
    st.write("Returns by Country:")
    st.dataframe(run_query("""
        SELECT c.Country,COUNT(*) AS Return_Records,ABS(SUM(r.Quantity)) AS Total_Units_Returned
        FROM returns r JOIN customers c ON r.CustomerID=c.CustomerID
        GROUP BY c.Country ORDER BY Total_Units_Returned DESC
    """))

def render_returns_section():
    st.markdown("### 📊 Return Rate Analysis")
    if st.checkbox("Show Debug Info for Returns"):
        debug_returns_table()
    df_ret = load_return_analysis(country_filter, product_filter, tuple(params))
    c1, c2 = st.columns(2)
    with c1:
        if not df_ret.empty and (df_ret['Return_Rate_Percent']>0).any():
            fig = px.bar(
                df_ret[df_ret['Return_Rate_Percent']>0],
                x='Country', y='Return_Rate_Percent', text='Return_Rate_Percent',
                color='Return_Rate_Percent', color_continuous_scale='reds',
                labels={'Return_Rate_Percent':'Return Rate (%)','Country':'Country'},
                title="Return Rates by Country"
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig.update_layout(showlegend=False,xaxis_tickangle=-45,height=400,
                              plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No return data available.")
    with c2:
        if not df_ret.empty:
            df_summary = df_ret[df_ret['Units_Returned']>0].copy()
            if df_summary.empty:
                st.info("No returns data to display.")
            else:
                df_summary['Units_Returned'] = df_summary['Units_Returned'].astype(int)
                df_summary['Units_Sold']     = df_summary['Units_Sold'].astype(int)
                st.markdown("#### Returns Summary")
                st.dataframe(
                    df_summary[['Country','Units_Returned','Units_Sold','Return_Rate_Percent']].head(10),
                    use_container_width=True
                )
        else:
            st.info("No return data available for summary")

# ──────────────────────────────────────────────────────────────────────────────
# Revenue by country visualisation
# ──────────────────────────────────────────────────────────────────────────────
c1, c2 = st.columns([2,1])
with c1:
    st.markdown("### 🌍 Revenue Performance by Country")
    df_country = load_country_revenue(country_filter, product_filter, tuple(params))
    if not df_country.empty:
        fig = px.bar(
            df_country, x='Country', y='Revenue', text='Revenue',
            color='Revenue', color_continuous_scale='viridis',
            labels={'Revenue':'Revenue (€)','Country':'Country'},
            title="Revenue Distribution Across Markets"
        )
        fig.update_traces(texttemplate='€%{text:,.0f}', textposition='outside',
                          marker_line_color='rgb(8,48,107)', marker_line_width=1.5)
        fig.update_layout(showlegend=False,xaxis_tickangle=-45,height=500,
                          plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)',
                          font=dict(family="Arial",size=12),title_font_size=16)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No country revenue data available.")
with c2:
    st.markdown("### 🏆 Top Markets Summary")
    if not df_country.empty:
        for _, row in df_country.head(5).iterrows():
            st.markdown(f"""
            <div style='background:linear-gradient(90deg,#667eea 0%,#764ba2 100%);
                        padding:10px;margin:5px 0;border-radius:8px;color:white;'>
                <strong>{row['Country']}</strong><br>
                💰 €{row['Revenue']:,.0f}<br>
                📦 {row['Orders']} orders
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No market data available.")

# ──────────────────────────────────────────────────────────────────────────────
# World map
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### 🗺️ Global Revenue Heatmap")
if not df_country.empty:
    iso_map = {"United Kingdom":"GBR","France":"FRA","Germany":"DEU","Netherlands":"NLD",
               "EIRE":"IRL","Spain":"ESP","Switzerland":"CHE","Portugal":"PRT",
               "Belgium":"BEL","Australia":"AUS","USA":"USA","Poland":"POL","Japan":"JPN",
               "Norway":"NOR","Channel Islands":"JEY","Italy":"ITA","Austria":"AUT",
               "Denmark":"DNK","Finland":"FIN","Sweden":"SWE","Canada":"CAN","Israel":"ISR",
               "Cyprus":"CYP","Greece":"GRC","Iceland":"ISL","Malta":"MLT",
               "Lithuania":"LTU","Brazil":"BRA","Czech Republic":"CZE","Bahrain":"BHR",
               "Saudi Arabia":"SAU","United Arab Emirates":"ARE","Lebanon":"LBN",
               "Singapore":"SGP","RSA":"ZAF"}
    df_country['ISO_Code'] = df_country['Country'].map(iso_map)
    fig_map = px.choropleth(
        df_country, locations="ISO_Code", color="Revenue",
        hover_name="Country", hover_data={'Revenue':':,.2f','Orders':':,','Customers':':,'},
        color_continuous_scale='plasma', title="Global Revenue Distribution"
    )
    fig_map.update_layout(geo=dict(showframe=False,showcoastlines=True,projection_type='equirectangular'),
                          height=600,plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_map, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# Product & customer analysis
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_product_analysis(country_filter, product_filter, ptuple):
    params = list(ptuple) if ptuple else []
    q = f"""
    SELECT p.Description,
           COALESCE(ROUND(SUM(t.Quantity*t.UnitPrice),2),0) AS Revenue,
           SUM(t.Quantity) AS Units_Sold
    FROM transactions t
    JOIN products p ON t.StockCode=p.StockCode
    JOIN customers c ON t.CustomerID=c.CustomerID
    WHERE 1=1 {country_filter} {product_filter}
    GROUP BY p.Description
    HAVING Revenue>0
    ORDER BY Revenue DESC
    LIMIT 10
    """
    return run_query(q, params)

@st.cache_data(ttl=300)
def load_customer_analysis(country_filter, product_filter, ptuple):
    params = list(ptuple) if ptuple else []
    q = f"""
    SELECT t.CustomerID,c.Country,
           COALESCE(ROUND(SUM(t.Quantity*t.UnitPrice),2),0) AS Total_Spent,
           COUNT(DISTINCT t.InvoiceNo) AS Orders
    FROM transactions t
    JOIN customers c ON t.CustomerID=c.CustomerID
    JOIN products  p ON t.StockCode=p.StockCode
    WHERE 1=1 {country_filter} {product_filter}
    GROUP BY t.CustomerID,c.Country
    HAVING Total_Spent>0
    ORDER BY Total_Spent DESC
    LIMIT 10
    """
    return run_query(q, params)

pc1, pc2 = st.columns(2)
with pc1:
    st.markdown("### 📈 Top Performing Products")
    df_prod = load_product_analysis(country_filter, product_filter, tuple(params))
    if not df_prod.empty:
        fig_prod = px.bar(
            df_prod, x='Revenue', y='Description', orientation='h',
            text='Revenue', color='Revenue', color_continuous_scale='viridis',
            labels={'Revenue':'Revenue (€)','Description':'Product'},
            title="Revenue Leaders"
        )
        fig_prod.update_traces(texttemplate='€%{text:,.0f}', textposition='outside')
        fig_prod.update_layout(showlegend=False,height=500,
                               plot_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)',
                               yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_prod, use_container_width=True)
    else:
        st.info("No product data available.")
with pc2:
    st.markdown("### 🎯 Customer Analysis")
    df_cust = load_customer_analysis(country_filter, product_filter, tuple(params))
    if not df_cust.empty:
        df_clean = (df_cust.assign(
            Orders=pd.to_numeric(df_cust['Orders'],errors='coerce'),
            Total_Spent=pd.to_numeric(df_cust['Total_Spent'],errors='coerce'),
            CustomerID=df_cust['CustomerID'].astype(str),
            Country=df_cust['Country'].astype(str)
        ).dropna())
        if not df_clean.empty:
            fig_cust = px.scatter(
                df_clean, x='Orders', y='Total_Spent', color='Country',
                size='Total_Spent', hover_data={'CustomerID':True},
                labels={'Total_Spent':'Total Spent (€)','Orders':'Number of Orders'},
                title="Customer Value Distribution"
            )
            fig_cust.update_layout(height=500,plot_bgcolor='rgba(0,0,0,0)',
                                   paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_cust, use_container_width=True)
        else:
            st.info("No valid customer data after cleaning.")
    else:
        st.info("No customer data available.")

# ──────────────────────────────────────────────────────────────────────────────
# Return analysis section
# ──────────────────────────────────────────────────────────────────────────────
render_returns_section()

# ──────────────────────────────────────────────────────────────────────────────
# Advanced analytics: insights & recommendations
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### 📈 Advanced Analytics")
df_returns = load_return_analysis(country_filter, product_filter, tuple(params))
ia1, ia2 = st.columns(2)
with ia1:
    st.markdown("#### 💡 Business Insights")
    ins = []
    if not df_kpis.empty:
        rev, ords, custs = float(df_kpis.at[0,'Total_Revenue']), int(df_kpis.at[0,'Total_Orders']), int(df_kpis.at[0,'Total_Customers'])
        aov = float(df_kpis.at[0,'Avg_Order_Value'])
        if rev:
            ins.append(f"💰 Total revenue of €{rev:,.2f} across {ords:,} orders")
            ins.append(f"👥 {custs:,} unique customers with average order value of €{aov:.2f}")
            if not df_country.empty:
                ins.append(f"🏆 {df_country.at[0,'Country']} leads with €{df_country.at[0,'Revenue']:,.2f} revenue")
            if not df_returns.empty and df_returns['Units_Returned'].sum()>0:
                ins.append(f"📊 Avg return rate: {df_returns['Return_Rate_Percent'].mean():.1f}%")
    if not ins:
        ins=["📊 No data for current selection","🔄 Adjust filters or check DB connection"]
    for i in ins: st.markdown(f"• {i}")
with ia2:
    st.markdown("#### 🎯 Recommendations")
    recs=[]
    if not df_country.empty:
        recs.append(f"Focus marketing on {df_country.at[0,'Country']} for growth")
    if not df_returns.empty and (hr:=df_returns[df_returns['Return_Rate_Percent']>10]).any(axis=None):
        recs.append(f"Investigate high return rate in {hr.iloc[0]['Country']} ({hr.iloc[0]['Return_Rate_Percent']:.1f}%)")
    if not df_prod.empty:
        recs.append("Increase inventory of top-performing products")
    if not recs:
        recs=["📈 Analyse purchase patterns","🔍 Monitor return rates","🎯 Retain high-value customers"]
    for r in recs: st.markdown(f"• {r}")

# ──────────────────────────────────────────────────────────────────────────────
# Data quality & system status
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### 🔍 Data Quality Check")
dq1, dq2, dq3 = st.columns(3)
with dq1:
    st.markdown("#### 📊 Data Coverage")
    for tbl in ['transactions','customers','products']:
        cnt = run_query(f"SELECT COUNT(*) as c FROM {tbl}")
        st.metric(tbl.capitalize(), f"{cnt.at[0,'c']:,}" if not cnt.empty else "0")
with dq2:
    st.markdown("#### ⚠️ Data Issues")
    miss = run_query("""
        SELECT SUM(CustomerID IS NULL) missing_customers,
               SUM(StockCode  IS NULL) missing_products,
               SUM(Quantity   IS NULL) missing_quantities
        FROM transactions
    """)
    issues=[]
    if not miss.empty:
        if miss.at[0,'missing_customers']: issues.append(f"❌ {miss.at[0,'missing_customers']} transactions missing customer IDs")
        if miss.at[0,'missing_products']: issues.append(f"❌ {miss.at[0,'missing_products']} transactions missing product codes")
        if miss.at[0,'missing_quantities']: issues.append(f"❌ {miss.at[0,'missing_quantities']} transactions missing quantities")
    if not issues: issues=["✅ No major data issues detected"]
    for isue in issues: st.markdown(isue)
with dq3:
    st.markdown("#### 🔧 System Status")
    for name,val in [
        ("Database Connection","✅ Connected" if st.session_state.connection_status=="Connected" else "❌ Disconnected"),
        ("Last Data Refresh",st.session_state.last_refresh.strftime("%H:%M:%S")),
        ("Cache Status","✅ Active"),
        ("Query Performance","✅ Optimized")
    ]:
        st.markdown(f"**{name}:** {val}")

# ──────────────────────────────────────────────────────────────────────────────
# Export buttons
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("### 📁 Data Export")
ex1, ex2, ex3 = st.columns(3)
with ex1:
    if st.button("📊 Export KPIs", use_container_width=True) and not df_kpis.empty:
        st.download_button("Download KPIs CSV", df_kpis.to_csv(index=False),
                           f"kpis_{datetime.now():%Y%m%d_%H%M%S}.csv", "text/csv")
with ex2:
    if st.button("🌍 Export Country Data", use_container_width=True) and not df_country.empty:
        st.download_button("Download Country CSV", df_country.to_csv(index=False),
                           f"country_data_{datetime.now():%Y%m%d_%H%M%S}.csv", "text/csv")
with ex3:
    if st.button("📦 Export Product Data", use_container_width=True) and not df_prod.empty:
        st.download_button("Download Products CSV", df_prod.to_csv(index=False),
                           f"products_{datetime.now():%Y%m%d_%H%M%S}.csv", "text/csv")

# ──────────────────────────────────────────────────────────────────────────────
# Debug section
# ──────────────────────────────────────────────────────────────────────────────
with st.expander("🔧 Debug Information", expanded=False):
    st.json({
        "Connection Status": st.session_state.connection_status,
        "Selected Countries": selected_countries,
        "Selected Products": len(selected_products),
        "Total Countries": len(countries),
        "Total Products": len(products),
        "Last Refresh": st.session_state.last_refresh.isoformat()
    })
    if st.button("🧪 Test Database Connection"):
        if (t := get_connection()):
            try:
                cur=t.cursor();cur.execute("SELECT VERSION()");ver=cur.fetchone()[0]
                st.success(f"✅ DB connection OK – MySQL {ver}");cur.close()
            finally:
                if t.is_connected(): t.close()
        else:
            st.error("❌ Could not connect to database")

# ──────────────────────────────────────────────────────────────────────────────
# Footer
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(f"""
<div style='text-align:center;padding:20px;color:#7f8c8d;'>
    <p>🚀 Powered by Advanced Analytics | Last Updated: {datetime.now():%Y-%m-%d %H:%M:%S}</p>
    <p>📊 Retail Analytics Hub v2.0 | Built with Streamlit & MySQL</p>
</div>
""", unsafe_allow_html=True)
