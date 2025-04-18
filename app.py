import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

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

# Correctly convert PEM private key → DER bytes
def connect_snowflake():
    pem_str = st.secrets["private_key"]
    pem_bytes = pem_str.encode("utf-8")

    private_key = serialization.load_pem_private_key(
        pem_bytes,
        password=None,
        backend=default_backend()
    )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=st.secrets["user"],
        account=st.secrets["account"],
        private_key=private_key_bytes,
        warehouse=st.secrets["warehouse"],
        database=st.secrets["database"],
        schema=st.secrets["schema"]
    )

# 🔍 Connection test block
try:
    conn = connect_snowflake()
    st.success("✅ Successfully connected to Snowflake using key-pair authentication!")

    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_REGION();")
    result = cur.fetchone()
    st.code(result)
    cur.close()
except Exception as e:
    st.error(f"❌ Connection failed: {e}")
    st.stop()  # Stop execution if connection fails

# --- Main App Logic Below ---

# Reconnect after test
conn = connect_snowflake()
cur = conn.cursor()

# Query state + category data
cur.execute("""
SELECT STATE, CATEGORY, COUNT(*) AS CATEGORY_COUNT
FROM MEDFAIR_DATABASE.PUBLIC.PROCESSED_MASTER_FILE_CATEGORY
WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL
GROUP BY STATE, CATEGORY
""")
df = pd.DataFrame(cur.fetchall(), columns=["STATE", "CATEGORY", "CATEGORY_COUNT"])

# Total per state
totals = df.groupby("STATE")["CATEGORY_COUNT"].sum().reset_index(name="TOTAL_COUNT")

# Hover text
hover = df.groupby("STATE").apply(
    lambda x: "<br>".join(f"{r['CATEGORY']}: {r['CATEGORY_COUNT']}" for _, r in x.iterrows())
).reset_index(name="HOVER_TEXT")

# Merge + map
data = totals.merge(hover, on="STATE")
data["STATE_CODE"] = data["STATE"].map(us_state_abbr)
data = data.dropna(subset=["STATE_CODE"])

# Map plot
fig = px.choropleth(
    data,
    locations="STATE_CODE",
    locationmode="USA-states",
    color="TOTAL_COUNT",
    hover_name="STATE",
    hover_data={"HOVER_TEXT": True, "STATE_CODE": False, "TOTAL_COUNT": False},
    scope="usa",
    color_continuous_scale="Turbo",
    title="📍 Hover on a State to See CATEGORY Counts"
)

st.plotly_chart(fig, use_container_width=True)

# --- Select state and show avg NEGOTIATED_RATE ---
selected_state = st.selectbox(
    "👇 Select a state to view average NEGOTIATED_RATE:",
    options=data["STATE"].sort_values().unique()
)

# Reconnect and run second query
conn = connect_snowflake()
cur = conn.cursor()

cur.execute(f"""
SELECT ROUND(AVG(NEGOTIATED_RATE), 2) AS AVG_NEGOTIATED_RATE
FROM MEDFAIR_DATABASE.PUBLIC.PROCESSED_MASTER_FILE_CATEGORY
WHERE STATE = '{selected_state}'
""")

avg_rate = cur.fetchone()[0]
cur.close()
conn.close()

# Display result
st.markdown(f"📌 **Average Negotiated Rate for `{selected_state}`:** `${avg_rate}`")
