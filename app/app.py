"""
StatsBomb Football Analytics Dashboard
Multi-page Streamlit app — WC 2022 + PL 2015/16
"""
import streamlit as st

st.set_page_config(
    page_title="Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("⚽ Football Analytics Dashboard")
st.markdown("""
Welcome to the StatsBomb analytics dashboard.

**Available data:**
- FIFA World Cup 2022 — 64 matches
- Premier League 2015/16 — 380 matches

**Use the sidebar to navigate between pages.**
""")

st.markdown("---")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Competitions", "2")
with col2:
    st.metric("Matches", "444")
with col3:
    st.metric("Events", "1.5M+")
with col4:
    st.metric("Players", "1,415")

st.markdown("---")
st.markdown("""
**Pages:**
- **Match Report** — xG timeline, pass network, shot map, formation, pressure heatmap
- **Team Overview** — season xG performance, PPDA pressing rankings, pass completion trends
- **Player Analysis** — top performers by xG, xT-added, key passes, radar charts
- **Head to Head** — compare any two teams across all metrics
""")