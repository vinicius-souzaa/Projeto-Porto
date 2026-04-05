import streamlit as st
from util.constants import C

def sidebar(summary: dict = None):
    with st.sidebar:
        st.markdown(
            f'<div style="padding:1rem 0 0.5rem;">'
            f'<div style="font-size:1.1rem;font-weight:700;'
            f'color:{C["orange"]};letter-spacing:0.05em;">⚓ Portos do Brasil</div>'
            f'<div style="font-size:0.68rem;color:#64748B;letter-spacing:0.14em;'
            f'text-transform:uppercase;margin-top:3px;">ANTAQ · 2010–2026</div>'
            f'</div>',
            unsafe_allow_html=True
        )

        if summary:
            st.markdown(
                f'<div style="background:{C["bg"]};border:1px solid {C["border"]};'
                f'border-radius:8px;padding:0.85rem;margin:0.5rem 0 1rem;">'

                f'<div style="font-size:0.65rem;color:#64748B;text-transform:uppercase;'
                f'letter-spacing:0.1em;margin-bottom:0.6rem;">Snapshot · 2010–{summary["last_year"]}</div>'

                f'<div style="margin-bottom:0.55rem;">'
                f'<div style="font-size:0.68rem;color:#64748B;">Total de atracações</div>'
                f'<div style="font-size:1.4rem;font-weight:700;color:{C["orange"]};">'
                f'{summary["n_atracacoes"]:,}</div></div>'

                f'<div style="border-top:1px solid {C["border"]};padding-top:0.5rem;'
                f'display:grid;grid-template-columns:1fr 1fr;gap:0.4rem;">'

                f'<div><div style="font-size:0.63rem;color:#64748B;">Portos</div>'
                f'<div style="font-size:0.9rem;color:{C["blue"]};font-weight:600;">'
                f'{summary["n_portos"]}</div></div>'

                f'<div><div style="font-size:0.63rem;color:#64748B;">Último ano</div>'
                f'<div style="font-size:0.9rem;color:{C["green"]};font-weight:600;">'
                f'{summary["last_year"]}</div></div>'

                f'<div><div style="font-size:0.63rem;color:#64748B;">Estadia média</div>'
                f'<div style="font-size:0.9rem;color:{C["yellow"]};font-weight:600;">'
                f'{summary["estadia_media"]:.0f}h</div></div>'

                f'<div><div style="font-size:0.63rem;color:#64748B;">Espera média</div>'
                f'<div style="font-size:0.9rem;color:{C["yellow"]};font-weight:600;">'
                f'{summary["espera_media"]:.0f}h</div></div>'

                f'</div></div>',
                unsafe_allow_html=True
            )

        st.divider()
        st.caption("Vinicius Abreu Ernestino Souza")
        st.caption("Data Analytics · São Paulo, SP")
        st.caption("Fonte: ANTAQ — dados abertos")
