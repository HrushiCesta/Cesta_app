import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from cryptography.hazmat.primitives import serialization

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

# Connect to Snowflake using key-pair auth
conn = snowflake.connector.connect(
    user=st.secrets["user"],
    private_key=private_key_der,
    account=st.secrets["account"],
    warehouse=st.secrets["warehouse"],
    database=st.secrets["database"],
    schema="ALL_STATES"
)

cur = conn.cursor()

# Query for category counts
cur.execute("""
SELECT STATE, CATEGORY, COUNT(*) AS CATEGORY_COUNT
FROM ALL_STATE_COMBINED
WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL
GROUP BY STATE, CATEGORY
""")
df = pd.DataFrame(cur.fetchall(), columns=["STATE", "CATEGORY", "CATEGORY_COUNT"])

# Query for negotiated type counts
cur.execute("""
SELECT STATE, CATEGORY, NEGOTIATED_TYPE, COUNT(*) AS TYPE_COUNT
FROM ALL_STATE_COMBINED
WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL AND NEGOTIATED_TYPE IS NOT NULL
GROUP BY STATE, CATEGORY, NEGOTIATED_TYPE
""")
type_df = pd.DataFrame(cur.fetchall(), columns=["STATE", "CATEGORY", "NEGOTIATED_TYPE", "TYPE_COUNT"])

# Pivot for hover text
hover_texts = {}
for (state, category), group in type_df.groupby(["STATE", "CATEGORY"]):
    lines = [f"{category}:"] + [f"- {r['NEGOTIATED_TYPE']}: {r['TYPE_COUNT']}" for _, r in group.iterrows()]
    hover_texts.setdefault(state, []).append("<br>".join(lines))

# Total per state
totals = df.groupby("STATE")["CATEGORY_COUNT"].sum().reset_index(name="TOTAL_SUBSCRIPTIONS")
hover = pd.DataFrame.from_dict({k: "<br><br>".join(v) for k, v in hover_texts.items()}, orient='index', columns=["HOVER_TEXT"]).reset_index().rename(columns={"index": "STATE"})

# Merge + map
data = totals.merge(hover, on="STATE")
data["STATE_CODE"] = data["STATE"].map(us_state_abbr)
data = data.dropna(subset=["STATE_CODE"])

# Map plot
fig = px.choropleth(
    data,
    locations="STATE_CODE",
    locationmode="USA-states",
    color="TOTAL_SUBSCRIPTIONS",
    hover_name="STATE",
    hover_data={"HOVER_TEXT": True, "STATE_CODE": False, "TOTAL_SUBSCRIPTIONS": False},
    scope="usa",
    color_continuous_scale="Turbo",
    title="📍 Hover on a State to See CATEGORY Counts"
)

st.plotly_chart(fig, use_container_width=True)

# Dropdown selection
selected_state = st.selectbox(
    "👇 Select a state to view CATEGORY breakdown:",
    options=data["STATE"].sort_values().unique()
)

# Reconnect
conn = snowflake.connector.connect(
    user=st.secrets["user"],
    private_key=private_key_der,
    account=st.secrets["account"],
    warehouse=st.secrets["warehouse"],
    database=st.secrets["database"],
    schema="ALL_STATES"
)
cur = conn.cursor()

# Get category breakdown
cur.execute(f"""
SELECT CATEGORY, COUNT(*) AS CATEGORY_COUNT
FROM ALL_STATE_COMBINED
WHERE STATE = '{selected_state}' AND CATEGORY IS NOT NULL
GROUP BY CATEGORY
ORDER BY CATEGORY_COUNT DESC
""")
category_data = pd.DataFrame(cur.fetchall(), columns=["CATEGORY", "CATEGORY_COUNT"])

# Get average negotiated rate
cur.execute(f"""
SELECT ROUND(AVG(NEGOTIATED_RATE), 2)
FROM ALL_STATE_COMBINED
WHERE STATE = '{selected_state}'
""")
avg_rate = cur.fetchone()[0]

# Get negotiated type breakdown for the selected state
cur.execute(f"""
SELECT CATEGORY, NEGOTIATED_TYPE, COUNT(*) AS TYPE_COUNT
FROM ALL_STATE_COMBINED
WHERE STATE = '{selected_state}' AND CATEGORY IS NOT NULL AND NEGOTIATED_TYPE IS NOT NULL
GROUP BY CATEGORY, NEGOTIATED_TYPE
ORDER BY CATEGORY, NEGOTIATED_TYPE
""")
type_breakdown = pd.DataFrame(cur.fetchall(), columns=["CATEGORY", "NEGOTIATED_TYPE", "TYPE_COUNT"])
cur.close()
conn.close()

# Display
st.markdown(f"📌 **Category breakdown for `{selected_state}`**  ✨ *Average Negotiated Rate:* `${avg_rate}`")
st.dataframe(category_data, use_container_width=True)

# Show negotiated type breakdown (formatted better)
st.markdown("### 🔍 Negotiated Type Breakdown by Category")
type_pivot = type_breakdown.pivot(index="CATEGORY", columns="NEGOTIATED_TYPE", values="TYPE_COUNT").fillna(0).astype(int)
st.dataframe(type_pivot.reset_index(), use_container_width=True)

# Legend for negotiated type
st.markdown("### 📘 Negotiated Type Legend")
st.markdown("""
- **negotiated**: A fixed, direct amount agreed upon (e.g., $53.25)
- **percentage**: A percentage of billed charges (e.g., 80%)
- **per diem**: A daily rate (e.g., $500 per day)
- **derived**: Estimated from other values
""")
