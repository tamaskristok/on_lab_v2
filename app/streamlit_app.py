from __future__ import annotations

import streamlit as st


st.markdown(
    """
    <style>
    [data-testid="stSidebarNav"] a {
        font-size: 1.15rem;
        font-weight: 600;
        padding-top: 0.6rem;
        padding-bottom: 0.6rem;
    }

    [data-testid="stSidebarNav"] span {
        font-size: 1.15rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


home_page = st.Page(
    "views/home.py",
    title="Főoldal",
)

downloads_page = st.Page(
    "views/downloads.py",
    title="Adat letöltés",
)

silver_upload_page = st.Page(
    "views/silver_upload.py",
    title="Silver feltöltés",
)

charts_page = st.Page(
    "views/charts.py",
    title="Elemzés",
)

navigation = st.navigation(
    [
        home_page,
        downloads_page,
        silver_upload_page,
        charts_page,
    ]
)

navigation.run()
