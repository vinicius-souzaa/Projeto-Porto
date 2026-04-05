# ── PORTOS DO BRASIL · Constantes ─────────────────────────────

# Paleta — Slate industrial + Laranja porto
C = {
    "bg":       "#0F1923",   # slate quase preto — fundo
    "surface":  "#162130",   # slate escuro — cards
    "surface2": "#1C2D3F",   # slate médio — hover
    "border":   "#1E3A4F",   # slate borda
    "border2":  "#2A4F6B",   # slate borda ativa
    "orange":   "#FF6B35",   # laranja porto — accent principal
    "blue":     "#1E90FF",   # azul oceano — accent secundário
    "green":    "#00C896",   # verde — positivo / crescimento
    "red":      "#EF4444",   # vermelho — alerta
    "yellow":   "#F59E0B",   # amarelo — atenção
    "teal":     "#14B8A6",   # teal — neutro positivo
    "text":     "#E2E8F0",   # texto principal
    "muted":    "#64748B",   # texto secundário
    "dim":      "#334155",   # texto terciário
}

# Plotly hover
HOVER = dict(
    bgcolor  = "#162130",
    bordercolor = "#1E3A4F",
    font     = dict(color="#E2E8F0", size=12),
    namelength = -1,
)

# Tipos de navegação com cores
NAV_COLORS = {
    "Longo Curso":    C["blue"],
    "Cabotagem":      C["orange"],
    "Interior":       C["green"],
    "Apoio Portuário":C["muted"],
    "Apoio Marítimo": C["dim"],
}

# Regiões
REGIAO_COLORS = {
    "Sudeste":      C["blue"],
    "Norte":        C["green"],
    "Sul":          C["teal"],
    "Nordeste":     C["orange"],
    "Centro-Oeste": C["yellow"],
}

# Natureza de carga
CARGA_COLORS = {
    "Granel Sólido":           "#F59E0B",
    "Granel Líquido e Gasoso": "#1E90FF",
    "Carga Conteinerizada":    "#FF6B35",
    "Carga Geral":             "#00C896",
    "Sem Carga":               "#334155",
}

# Meses em ordem
MESES_ORDER = ["jan","fev","mar","abr","mai","jun",
               "jul","ago","set","out","nov","dez"]

MESES_LABEL = {
    "jan":"Jan","fev":"Fev","mar":"Mar","abr":"Abr",
    "mai":"Mai","jun":"Jun","jul":"Jul","ago":"Ago",
    "set":"Set","out":"Out","nov":"Nov","dez":"Dez",
}

# Eventos históricos do setor portuário
EVENTS = [
    (2013, "Lei dos Portos",       C["blue"],   "Nova regulação do setor portuário"),
    (2015, "Crise econômica",      C["red"],    "Recessão reduz movimentação"),
    (2016, "Retomada",             C["green"],  "Início da recuperação"),
    (2020, "COVID-19",             C["red"],    "Pandemia impacta operações"),
    (2021, "BR do Mar",            C["blue"],   "Política de incentivo à cabotagem"),
    (2022, "Recorde Arco Norte",   C["orange"], "Norte supera Sul/Sudeste em grãos"),
    (2023, "Recorde histórico",    C["green"],  "1,3 bilhão de toneladas movimentadas"),
]

# Principais portos para filtros
TOP_PORTOS = [
    "Santos", "Paranaguá", "Rio Grande", "Itaguaí",
    "Belém", "São Francisco do Sul", "Vitória",
    "Rio de Janeiro", "Aratu", "Manaus",
]

# Coordenadas aproximadas dos principais portos para o mapa
PORTO_COORDS = {
    "Santos":               (-23.960, -46.333),
    "Paranaguá":            (-25.521, -48.523),
    "Rio Grande":           (-32.035, -52.098),
    "Itaguaí":              (-22.859, -43.789),
    "Belém":                (-1.455,  -48.502),
    "São Francisco do Sul": (-26.230, -48.636),
    "Vitória":              (-20.319, -40.337),
    "Rio de Janeiro":       (-22.891, -43.177),
    "Aratu":                (-12.760, -38.490),
    "Manaus":               (-3.136,  -60.020),
    "Salvador":             (-12.975, -38.502),
    "Fortaleza":            (-3.716,  -38.481),
    "Suape":                (-8.408,  -34.975),
    "Pecém":                (-3.533,  -38.820),
    "Porto Velho":          (-8.761,  -63.900),
    "Santarém":             (-2.443,  -54.708),
    "Ilhéus":               (-14.790, -39.034),
    "São Luís":             (-2.531,  -44.304),
    "Maceió":               (-9.665,  -35.735),
    "Recife":               (-8.362,  -34.877),
}
