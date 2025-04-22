import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from cryptography.hazmat.primitives import serialization

st.set_page_config(page_title="Kaiser Permanente Cost Analysis Dashboard", layout="wide")

# Sidebar navigation
st.sidebar.title("🔎 Navigation")
section = st.sidebar.radio("Go to:", ["Home", "Heatmap Overview", "Category Analytics", "Negotiated Type Breakdown"])

# Map full state names → 2-letter codes
us_state_abbr = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY'
}

# Load and convert private key
private_key_pem = st.secrets["private_key"].encode()
private_key = serialization.load_pem_private_key(private_key_pem, password=None)
private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# DB connection function
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["user"],
        private_key=private_key_der,
        account=st.secrets["account"],
        warehouse=st.secrets["warehouse"],
        database=st.secrets["database"],
        schema="ALL_STATES"
    )

# --- HOME PAGE ---
if section == "Home":
    st.title("🏥 Kaiser Permanente Cost Analysis")
    st.success("✅ Data successfully imported and analyzed.")
    st.markdown("""
        This app provides an interactive breakdown of testosterone-related negotiated rates by:

        - 📍 **State**  
        - 📦 **Drug Category** (Gel, Injection, Patch, etc.)  
        - 💰 **Negotiated Rate Type** (Fixed, Percentage, etc.)

        
        👉 Use the sidebar to explore the full analytics.
    """)
    st.markdown("---")
    

# --- HEATMAP OVERVIEW ---
elif section == "Heatmap Overview":
    st.title("🧾 Heatmap Overview")
    st.markdown("""
    This section gives an overview of how many testosterone-related medical entries were processed across different states. 

    Use the navigation to view more detailed analytics by category or negotiated rate types.
    """)

# --- CATEGORY ANALYTICS ---
elif section == "Category Analytics":
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT STATE, CATEGORY, COUNT(*) AS CATEGORY_COUNT, ROUND(AVG(NEGOTIATED_RATE), 2) AS AVG_NEGOTIATED_RATE
        FROM ALL_STATE_COMBINED
        WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL
        GROUP BY STATE, CATEGORY
    """)
    cat_data = pd.DataFrame(cur.fetchall(), columns=["STATE", "CATEGORY", "CATEGORY_COUNT", "AVG_NEGOTIATED_RATE"])
    cat_data["STATE_CODE"] = cat_data["STATE"].map(us_state_abbr)
    cat_data = cat_data.dropna(subset=["STATE_CODE"])

    hover_text = cat_data.apply(lambda r: f"{r['CATEGORY']}<br>Avg Rate: ${r['AVG_NEGOTIATED_RATE']:.2f}<br>Count: {r['CATEGORY_COUNT']:,}", axis=1)
    cat_data["HOVER"] = hover_text

    st.title("📦 Category Analytics - Nationwide View")
    fig = px.choropleth(
        cat_data,
        locations="STATE_CODE",
        locationmode="USA-states",
        color="CATEGORY_COUNT",
        color_continuous_scale="Blues",
        hover_name="STATE",
        hover_data={"HOVER": True, "STATE_CODE": False, "CATEGORY_COUNT": False},
        hover_name="STATE",
        hover_data={"HOVER": True, "STATE_CODE": False, "CATEGORY_COUNT": False},
        scope="usa",
        color_continuous_scale="Blues",
        title="📍 Category Counts by State"
    )
    st.plotly_chart(fig, use_container_width=True)

    states = pd.read_sql("SELECT DISTINCT STATE FROM ALL_STATE_COMBINED WHERE STATE IS NOT NULL ORDER BY STATE", conn)
    selected_state = st.selectbox("👇 Select a state to view detailed CATEGORY breakdown:", states["STATE"])

    cur.execute(f"""
        SELECT CATEGORY, COUNT(*) AS CATEGORY_COUNT, ROUND(AVG(NEGOTIATED_RATE), 2) AS AVG_RATE
        FROM ALL_STATE_COMBINED
        WHERE STATE = '{selected_state}' AND CATEGORY IS NOT NULL
        GROUP BY CATEGORY
        ORDER BY CATEGORY_COUNT DESC
    """)
    category_data = pd.DataFrame(cur.fetchall(), columns=["CATEGORY", "CATEGORY_COUNT", "AVG_NEGOTIATED_RATE"])
    category_data["AVG_NEGOTIATED_RATE"] = category_data["AVG_NEGOTIATED_RATE"].apply(lambda x: f"${x:,.2f}")

    cur.execute(f"""
        SELECT ROUND(AVG(NEGOTIATED_RATE), 2)
        FROM ALL_STATE_COMBINED
        WHERE STATE = '{selected_state}'
    """)
    avg_rate = cur.fetchone()[0]

    st.markdown(f"📌 **Detailed breakdown for `{selected_state}`** ✨ *Statewide Avg. Negotiated Rate:* `${avg_rate}`")
    st.dataframe(category_data, use_container_width=True)

# --- NEGOTIATED TYPE BREAKDOWN ---
elif section == "Negotiated Type Breakdown":
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT STATE, CATEGORY, NEGOTIATED_TYPE, COUNT(*) AS TYPE_COUNT
        FROM ALL_STATE_COMBINED
        WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL AND NEGOTIATED_TYPE IS NOT NULL
        GROUP BY STATE, CATEGORY, NEGOTIATED_TYPE
    """)
    type_df = pd.DataFrame(cur.fetchall(), columns=["STATE", "CATEGORY", "NEGOTIATED_TYPE", "TYPE_COUNT"])

    summary = type_df.groupby("STATE")["TYPE_COUNT"].sum().reset_index(name="TOTAL_NEGOTIATED_TYPE")
    summary["STATE_CODE"] = summary["STATE"].map(us_state_abbr)
    summary = summary.dropna(subset=["STATE_CODE"])

    st.title("💰 Negotiated Type Breakdown")
    fig = px.choropleth(
        summary,
        locations="STATE_CODE",
        locationmode="USA-states",
        color="TOTAL_NEGOTIATED_TYPE",
        hover_name="STATE",
        scope="usa",
        color_continuous_scale="Purples",
        title="📍 Total NEGOTIATED_TYPE Entries by State"
    )
    st.plotly_chart(fig, use_container_width=True)

    states = pd.read_sql("SELECT DISTINCT STATE FROM ALL_STATE_COMBINED WHERE STATE IS NOT NULL ORDER BY STATE", conn)
    selected_state = st.selectbox("Select a state:", states["STATE"])

    cur.execute(f"""
        SELECT CATEGORY, NEGOTIATED_TYPE, COUNT(*) AS TYPE_COUNT
        FROM ALL_STATE_COMBINED
        WHERE STATE = '{selected_state}' AND CATEGORY IS NOT NULL AND NEGOTIATED_TYPE IS NOT NULL
        GROUP BY CATEGORY, NEGOTIATED_TYPE
        ORDER BY CATEGORY, NEGOTIATED_TYPE
    """)
    type_breakdown = pd.DataFrame(cur.fetchall(), columns=["CATEGORY", "NEGOTIATED_TYPE", "TYPE_COUNT"])
    type_pivot = type_breakdown.pivot(index="CATEGORY", columns="NEGOTIATED_TYPE", values="TYPE_COUNT").fillna(0).astype(int)

    st.markdown(f"### 🔍 Negotiated Type Breakdown for `{selected_state}`")
    st.dataframe(type_pivot.reset_index(), use_container_width=True)
    st.markdown("""
    - **negotiated**: A fixed, direct amount agreed upon (e.g., $53.25)
    - **percentage**: A percentage of billed charges (e.g., 80%)
    - **per diem**: A daily rate (e.g., $500 per day)
    - **derived**: Estimated from other values
    """)
