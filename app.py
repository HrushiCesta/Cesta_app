import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector

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

# Connect to Snowflake using secrets
conn = snowflake.connector.connect(
    user=st.secrets["user"],
    password=st.secrets["password"],
    account=st.secrets["account"],
    warehouse=st.secrets["warehouse"],
    database=st.secrets["database"],
    schema="ALL_STATES"  # 👈 updated schema for All States
)

cur = conn.cursor()

# Query state + total negotiated rate counts
cur.execute("""
SELECT STATE, COUNT(*) AS ENTRY_COUNT
FROM ALL_STATE_COMBINED
WHERE STATE IS NOT NULL
GROUP BY STATE
""")
df = pd.DataFrame(cur.fetchall(), columns=["STATE", "ENTRY_COUNT"])

# Merge with state codes
df["STATE_CODE"] = df["STATE"].map(us_state_abbr)
df = df.dropna(subset=["STATE_CODE"])

# Choropleth map
fig = px.choropleth(
    df,
    locations="STATE_CODE",
    locationmode="USA-states",
    color="ENTRY_COUNT",
    hover_name="STATE",
    hover_data={"STATE_CODE": False, "ENTRY_COUNT": True},
    scope="usa",
    color_continuous_scale="Turbo",
    title="📍 Hover on a State to See Entry Counts in All States Dataset"
)

st.plotly_chart(fig, use_container_width=True)

# --- Dropdown for AVG NEGOTIATED_RATE ---
selected_state = st.selectbox(
    "👇 Select a state to view average NEGOTIATED_RATE:",
    options=df["STATE"].sort_values().unique()
)

# Reconnect for second query
conn = snowflake.connector.connect(
    user=st.secrets["user"],
    password=st.secrets["password"],
    account=st.secrets["account"],
    warehouse=st.secrets["warehouse"],
    database=st.secrets["database"],
    schema="ALL_STATES"
)
cur = conn.cursor()

cur.execute(f"""
SELECT ROUND(AVG(NEGOTIATED_RATE), 2) AS AVG_NEGOTIATED_RATE
FROM ALL_STATE_COMBINED
WHERE STATE = '{selected_state}'
""")
avg_rate = cur.fetchone()[0]
cur.close()
conn.close()

st.markdown(f"📌 **Average Negotiated Rate for `{selected_state}`:** `${avg_rate}`")
