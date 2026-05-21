from __future__ import annotations

from pathlib import Path

import streamlit as st


ASSET_DIR = Path(__file__).parents[1] / "assets"
BME_LOGO_PATH = ASSET_DIR / "bme_logo.png"


st.set_page_config(
    page_title="Főoldal",
    layout="wide",
)

logo_left_col, logo_center_col, logo_right_col = st.columns([1.5, 1, 1.5])

with logo_center_col:
    if BME_LOGO_PATH.exists():
        st.image(
            BME_LOGO_PATH,
            width="stretch",
        )
    else:
        st.warning("A BME logó nem található: app/assets/bme_logo.png")

st.markdown(
    """
    <div style="line-height: 1.6; margin-top: 2rem; margin-bottom: 2rem;">
        <div style="font-size: 1.1rem; font-weight: 600;">Kristók Tamás</div>
        <div style="font-size: 1.1rem;">SYSPXR</div>
        <div style="font-size: 1.8rem; font-weight: 800; margin-top: 1rem;">
            Önálló laboratórium
        </div>
        <div style="font-size: 1.45rem; font-weight: 700;">
            BMEVITMAL05
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.divider()

st.header("Idősor-alapmodellek és generatív MI alkalmazása pénzügyi elemzésekben")

st.write(
    """
    A hagyományos pénzügyi elemzések gyakran feltételezik a folyamatok állandóságát, azonban a valós világ - különösen a gazdaság - folyamatosan változik, hirtelen "rezsimváltásokkal" és váratlan eseményekkel. Hogyan integrálhatók a száraz numerikus adatok mellé a hírekben rejlő szöveges információk egy közös modellbe? Képesek-e az új generációs mélytanuló architektúrák (pl. Transzformerek, State Space Modellek) nemcsak előrejelezni egy árfolyamot vagy a GDP-t, hanem megérteni a mögöttes okokat is ("miért" történt)? Építhető-e olyan "alapmodell" (foundation model), amely kevés tanítóadatból is képes alkalmazkodni új piaci helyzetekhez? Jelen téma célja a legkorszerűbb deep learning megoldások, különösen az idősoros alapmodellek és a multimodális adatfúzió (szöveg és numerikus adat együttes kezelése) vizsgálata. A hallgató feladata a releváns, heterogén adatforrások feltárása, begyűjtése és tisztítása, kiemelt figyelmet fordítva a jogi és etikai aspektusokra, valamint az EU AI Act előírásainak való megfelelésre. Ezt követően a tisztított adatokon olyan architektúrák (pl. RAG - Retrieval-Augmented Generation, domén-specifikus LLM-ek) kutatása és implementálása a cél, amelyek a gazdasági híreket és a idősorokat egy közös látens térben kezelik a pontosabb nowcast és forecast érdekében. A feladat jellege kifejezetten kutatás-orientált, a téma elméleti kihívásokat tartalmaz, ezért a jelentkezésnél elvárás a tudományos ambíció. A munka célja nem csupán szoftverfejlesztés, hanem publikálható eredmények elérése és TDK dolgozat készítése. A téma diplomáig, illetve akár PhD témaként is folytatható.
    """
)

st.divider()

st.markdown(
    "[Link](https://iw.tmit.bme.hu/education/studenttopic/TMIT2026-009)"
)

st.divider()

st.markdown(
    "GitHub: [Link](https://github.com/tamaskristok/on_lab_v2)"
)