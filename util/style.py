import copy
from util.constants import C, HOVER

# ── PLOTLY BASE ───────────────────────────────────────────────
_BASE = dict(
    paper_bgcolor = C["surface"],
    plot_bgcolor  = C["surface"],
    font          = dict(family="Space Grotesk, Inter, sans-serif",
                         color=C["muted"], size=12),
    legend        = dict(bgcolor=C["bg"], bordercolor=C["border"],
                         borderwidth=1, font=dict(color=C["muted"], size=11)),
    xaxis = dict(gridcolor=C["border"], linecolor=C["border"],
                 tickfont=dict(color=C["muted"]),
                 title_font=dict(color=C["muted"]), zeroline=False),
    yaxis = dict(gridcolor=C["border"], linecolor=C["border"],
                 tickfont=dict(color=C["muted"]),
                 title_font=dict(color=C["muted"]), zeroline=False),
    margin    = dict(l=16, r=16, t=44, b=16),
    hoverlabel= HOVER,
)

def get_layout(**kw):
    lay = copy.deepcopy(_BASE)
    lay.update(kw)
    lay["hoverlabel"] = copy.deepcopy(HOVER)
    return lay

def hex_to_rgba(hex_color: str, alpha: float = 0.15) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"rgba({r},{g},{b},{alpha})"

# ── CSS ───────────────────────────────────────────────────────
_CSS = [
    "@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&display=swap');",
    f"html,body,[class*='css']{{font-family:'Space Grotesk',sans-serif !important;}}",
    f"html,body{{background-color:{C['bg']} !important;}}",
    f"[data-testid='stAppViewContainer']{{background-color:{C['bg']} !important;}}",
    f"[data-testid='stMain']{{background-color:{C['bg']} !important;}}",
    f".main .block-container{{background-color:{C['bg']} !important;}}",
    ".block-container{padding-top:1.2rem !important;padding-bottom:2rem !important;}",
    f"[data-testid='stHeader']{{background:{C['bg']} !important;}}",
    "[data-testid='stHeaderActionElements']{display:none !important;}",
    f"[data-testid='stSidebar']{{background-color:{C['surface']} !important;border-right:1px solid {C['border']} !important;}}",
    f"[data-testid='stSidebar']>div{{background-color:{C['surface']} !important;}}",
    f"[data-testid='stSidebar'] *{{color:#94A3B8 !important;}}",
    # Tabs
    f"[data-baseweb='tab-list']{{background-color:{C['bg']} !important;border-bottom:1px solid {C['border']} !important;gap:4px !important;padding:0 !important;}}",
    f"[data-baseweb='tab']{{background-color:transparent !important;color:{C['muted']} !important;border-bottom:2px solid transparent !important;padding:0.5rem 1rem !important;}}",
    f"[aria-selected='true'][data-baseweb='tab']{{color:{C['orange']} !important;border-bottom-color:{C['orange']} !important;}}",
    f"[data-baseweb='tab-panel']{{background-color:{C['bg']} !important;padding-top:1.2rem !important;}}",
    f"[data-baseweb='select'] *{{color:{C['text']} !important;background:{C['surface']} !important;}}",
    f".stSlider *{{color:{C['text']} !important;}}",
    f".stRadio *{{color:{C['text']} !important;}}",
    f".stMarkdown p{{color:{C['muted']} !important;}}",
    f"hr{{border-color:{C['border']} !important;}}",
    # KPI card
    f".kpi{{background:{C['surface']};border:1px solid {C['border']};border-radius:8px;padding:1rem 1.2rem;}}",
    f".kpi:hover{{border-color:{C['orange']};}}",
    f".kpi-num{{font-size:2rem;font-weight:700;line-height:1;margin-bottom:0.2rem;}}",
    f".kpi-label{{font-size:0.65rem;font-weight:600;letter-spacing:0.14em;text-transform:uppercase;color:{C['dim']};}}",
    f".kpi-delta{{font-size:0.75rem;font-weight:500;margin-top:0.2rem;}}",
    # Insight
    f".insight{{border-radius:8px;padding:0.85rem 1.1rem;margin:0.5rem 0;border-left:3px solid;}}",
    f".insight p{{color:#94A3B8;font-size:0.84rem;line-height:1.65;margin:0;}}",
    f".insight strong{{color:{C['text']};}}",
    # Section tag
    f".sec-tag{{display:inline-block;font-size:0.6rem;font-weight:600;letter-spacing:0.2em;text-transform:uppercase;color:{C['orange']};border:1px solid {C['border']};border-left:2px solid {C['orange']};padding:0.15rem 0.6rem;border-radius:0 3px 3px 0;margin-bottom:0.6rem;}}",
    f".sec-title{{font-size:1.35rem;font-weight:700;color:{C['text']};margin-bottom:0.6rem;}}",
    # Badge
    f".badge{{display:inline-block;font-size:0.65rem;font-weight:700;letter-spacing:0.08em;padding:0.15rem 0.55rem;border-radius:4px;}}",
    # Data row
    f".data-row{{display:flex;justify-content:space-between;padding:0.4rem 0;border-bottom:1px solid {C['border']};font-size:0.8rem;}}",
    f".data-row .dk{{color:{C['muted']};}}",
    f".data-row .dv{{color:{C['text']};font-weight:500;}}",
]

CSS = "<style>\n" + "\n".join(_CSS) + "\n</style>"

def inject_css():
    import streamlit as st
    st.markdown(CSS, unsafe_allow_html=True)

# ── COMPONENTS ────────────────────────────────────────────────

def kpi(number: str, label: str, delta: str = None,
        color: str = None, icon: str = "⚓") -> str:
    color = color or C["orange"]
    d = f'<div class="kpi-delta" style="color:{color};">{delta}</div>' if delta else ""
    return (
        f'<div class="kpi">'
        f'<div style="font-size:1rem;margin-bottom:0.1rem;">{icon}</div>'
        f'<div class="kpi-num" style="color:{color};">{number}</div>'
        f'<div class="kpi-label">{label}</div>{d}</div>'
    )

def insight(text: str, color: str = None) -> str:
    color = color or C["orange"]
    bg    = hex_to_rgba(color, 0.07)
    return (
        f'<div class="insight" style="background:{bg};border-color:{color};">'
        f'<p>{text}</p></div>'
    )

def sec_tag(label: str, note: str = None) -> str:
    extra = (f' <span style="color:{C["dim"]};font-weight:400;">· {note}</span>'
             if note else "")
    return f'<div class="sec-tag">{label}{extra}</div>'

def badge(text: str, color: str = None) -> str:
    color = color or C["orange"]
    return (
        f'<span class="badge" '
        f'style="background:{hex_to_rgba(color,0.15)};'
        f'color:{color};border:1px solid {hex_to_rgba(color,0.3)};">'
        f'{text}</span>'
    )

def data_row(key: str, value: str, vc: str = None) -> str:
    vstyle = f'color:{vc};' if vc else ""
    return (
        f'<div class="data-row">'
        f'<span class="dk">{key}</span>'
        f'<span class="dv" style="{vstyle}">{value}</span>'
        f'</div>'
    )
