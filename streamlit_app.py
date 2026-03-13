import streamlit as st


st.set_page_config(
    page_title="Chicago Crimes Overview",
    page_icon=":satellite_antenna:",
    layout="wide",
)

navigation = st.navigation(
    [
        st.Page("pages/0_Overview.py", title="Overview", icon=":material/space_dashboard:"),
        st.Page("pages/1_Quality.py", title="Quality Monitor", icon=":material/fact_check:"),
        st.Page("pages/2_Data_Health.py", title="Data Health", icon=":material/monitoring:"),
        st.Page("pages/3_Aggregations.py", title="Insights", icon=":material/insights:"),
    ],
    position="sidebar",
)

navigation.run()
