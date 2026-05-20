from __future__ import annotations

import streamlit as st


home_page = st.Page(
    "views/home.py",
    title="Főoldal",
)

downloads_page = st.Page(
    "views/downloads.py",
    title="Downloads",
)

charts_page = st.Page(
    "views/charts.py",
    title="Charts",
)

navigation = st.navigation(
    [
        home_page,
        downloads_page,
        charts_page,
    ]
)

navigation.run()
