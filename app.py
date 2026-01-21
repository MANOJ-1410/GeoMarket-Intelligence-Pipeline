import streamlit as st
import pandas as pd
import pydeck as pdk
from sqlalchemy import create_engine

# 1. DATABASE CONNECTION
# Use the same credentials as your main.py
if "DB_URL" in st.secrets:
    DB_URL = st.secrets["DB_URL"]
else:
    DB_URL = "postgresql://myuser:mypassword@127.0.0.1:5432/real_estate_db"

engine = create_engine(DB_URL)

# 2. CONFIGURATION & STYLING
st.set_page_config(layout="wide", page_title="Real Estate Hot-Zones")

st.title("📍 Real Estate Market Intelligence")
st.markdown("Analyzing price density and hot/cold market segments.")

# 3. DATA FETCHING (With Caching - Very important for 2.5 YOE)
@st.cache_data
def load_dashboard_data():
    query = """
    SELECT 
        p.latitude, p.longitude, p.city, p.zip_code, p.sqft, p.beds,
        h.list_price, h.price_per_sqft, h.market_segment, h.is_new_listing
    FROM properties p
    JOIN price_history h ON p.property_id = h.property_id
    """
    return pd.read_sql(query, engine)

df = load_dashboard_data()

# 4. SIDEBAR FILTERS
st.sidebar.header("Filter Market Data")

# Segment Filter
selected_segments = st.sidebar.multiselect(
    "Market Segments", 
    options=df['market_segment'].unique(),
    default=df['market_segment'].unique()
)

# Price Slider
min_p, max_p = int(df['list_price'].min()), int(df['list_price'].max())
price_range = st.sidebar.slider("Price Range ($)", min_p, max_p, (min_p, max_p))

# Filter Dataframe
mask = (df['market_segment'].isin(selected_segments)) & \
       (df['list_price'].between(price_range[0], price_range[1]))
filtered_df = df[mask]

# 5. KEY PERFORMANCE INDICATORS (KPIs)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Listings", len(filtered_df))
col2.metric("Avg Price", f"${filtered_df['list_price'].mean():,.0f}")
col3.metric("Avg Price/Sqft", f"${filtered_df['price_per_sqft'].mean():,.2f}")
col4.metric("Luxury Count", len(filtered_df[filtered_df['market_segment'] == 'Luxury']))

# 6. GEOSPATIAL MAP (PyDeck)
st.subheader("Market Heat Map (Hot/Cold Zones)")

# Logic: Color code dots by segment
# Luxury = Red, Premium = Orange, Standard = Green
color_map = {
    "Luxury": [255, 0, 0, 150],
    "Premium": [255, 165, 0, 150],
    "Standard": [0, 255, 0, 150]
}
filtered_df['color'] = filtered_df['market_segment'].map(color_map)

# Map text names to segments
text_color_map = {
    "Luxury": "🔴 Red (High Priority)",
    "Premium": "🟡 Yellow (Mid-Range)",
    "Standard": "🟢 Green (Value)"
}
filtered_df['color name'] = filtered_df['market_segment'].map(text_color_map)

# Define the Map Layer
layer = pdk.Layer(
    "ScatterplotLayer",
    filtered_df,
    get_position="[longitude, latitude]",
    get_color="color",
    get_radius=150,
    pickable=True,
)

# View State
view_state = pdk.ViewState(
    latitude=filtered_df['latitude'].mean(),
    longitude=filtered_df['longitude'].mean(),
    zoom=11,
    pitch=45
)

# Render Map
st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={"text": "City: {city}\nPrice: ${list_price}\nSegment: {market_segment}"}
))

# 7. DATA TABLE
st.subheader("Raw Market Data")
st.dataframe(filtered_df.sort_values("list_price", ascending=False), use_container_width=True)

