# py_server.py — FastAPI con cachés, formato moneda ES/AR, normalización y tablas estilo Markdown
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import pandas as pd
import os, time, re, unicodedata

# (Opcional) .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = FastAPI(title="Ventas TELCO Demo (fast+md+norm)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

# ---------- Rutas/carga del Excel ----------
EXCEL_ENV = os.getenv("EXCEL_PATH", "").strip()
DEFAULT_EXCEL_NAME = "Analisis_Ventas_TELCO_Demo.xlsx"
HERE = Path(__file__).resolve().parent

def resolve_excel_path():
    candidates = []
    if EXCEL_ENV:
        candidates.append(Path(EXCEL_ENV))
    candidates += [
        HERE / DEFAULT_EXCEL_NAME,
        HERE / "data" / DEFAULT_EXCEL_NAME,
        Path.cwd() / DEFAULT_EXCEL_NAME,
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(
        "No se encontró el Excel.\nProbé:\n- " + "\n- ".join(map(str, candidates))
        + "\nSoluciones: setea EXCEL_PATH en .env o copiá el archivo junto al py_server.py o en ./data/"
    )

DATA_PATH = resolve_excel_path()

# ---------- Constantes ----------
SHEET_BASE = "Base Facturación"
COL_TOTAL   = "Total"
COL_PERIODO = "Periodo"
COL_TIPO    = "Tipo de Cliente"
COL_EMISORA = "Emisora"
COL_CLIENTE = "Cliente"
COL_MOVIL   = "Líneas móviles"
COL_HOGAR   = "Internet hogar"
COL_SERV    = "Servicios adicionales"
COL_AJUSTE  = "Notas de ajuste"
SERVICE_COLS = [COL_MOVIL, COL_HOGAR, COL_SERV, COL_AJUSTE]

# ---------- Estado/caches ----------
DF = None
G_BY_PERIOD = None
G_BY_EMISORA = None
G_SERVICES = None
TOP_CLIENTS = None
LAST_LOAD_TS = None

# ---------- Helpers ----------
def _to_numeric(s):
    return pd.to_numeric(s, errors="coerce").fillna(0)

def _fmt_currency(v, decimals=2):
    try:
        x = float(v)
    except Exception:
        return str(v)
    s = f"{x:,.{decimals}f}"            # 1,234,567.89
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"$ {s}"

def _fmt_percent(v, decimals=2):
    try:
        x = float(v) * 100.0
    except Exception:
        return str(v)
    s = f"{x:,.{decimals}f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s + " %"

# normalizar consulta: sin tildes, sin puntuación, minúsculas
def _norm(s: str) -> str:
    s = (s or "").lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # quitar acentos
    s = re.sub(r"[^\w\s]", " ", s)  # quitar puntuación
    s = re.sub(r"\s+", " ", s).strip()
    return s

MONTHS_ES = {"ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,"jul":7,"ago":8,"sep":9,"sept":9,"oct":10,"nov":11,"dic":12}
def _periodo_key(s: str):
    if not isinstance(s, str): return None
    s2 = s.strip().lower().replace("é","e").replace("á","a").replace("ó","o").replace("í","i").replace("ú","u")
    m = re.match(r"([a-z]{3,5})[\.]?\s*[-/]?\s*(\d{4})", s2)
    if not m: return None
    mon = m.group(1).replace(".", "")
    mon_num = MONTHS_ES.get(mon[:4] if mon.startswith("sept") else mon[:3])
    year = int(m.group(2))
    if not mon_num: return None
    return year*100 + mon_num  # ej 202507

def _add_period_key(df: pd.DataFrame):
    out = df.copy()
    out["_period_key"] = out[COL_PERIODO].map(_periodo_key)
    return out

def _df_to_md_table(df: pd.DataFrame, headers=None):
    """Convierte df a tabla markdown con columnas alineadas por ancho."""
    if headers:
        df = df.rename(columns=headers)
    cols = list(df.columns)
    str_df = df.astype(str).fillna("")
    widths = {c: max(len(str(c)), str_df[c].map(len).max()) for c in cols}
    header = "| " + " | ".join(f"{str(c):<{widths[c]}}" for c in cols) + " |"
    sep    = "| " + " | ".join("-"*widths[c] for c in cols) + " |"
    rows = ["| " + " | ".join(f"{str(row[c]):<{widths[c]}}" for c in cols) + " |"
            for _, row in str_df.iterrows()]
    return "\n".join([header, sep] + rows)

def _build_caches(df: pd.DataFrame):
    # Total por mes
    g1 = df[[COL_PERIODO, COL_TOTAL]].copy()
    g1[COL_TOTAL] = _to_numeric(g1[COL_TOTAL])
    g1 = g1.groupby(COL_PERIODO, as_index=False)[COL_TOTAL].sum()
    g1 = _add_period_key(g1).sort_values("_period_key").drop(columns=["_period_key"])

    # Total por emisora
    g2 = df[[COL_EMISORA, COL_TOTAL]].copy()
    g2[COL_TOTAL] = _to_numeric(g2[COL_TOTAL])
    g2 = g2.groupby(COL_EMISORA, as_index=False)[COL_TOTAL].sum().sort_values(COL_TOTAL, ascending=False)

    # Servicios
    g3 = df[SERVICE_COLS].apply(_to_numeric, axis=0).sum().reset_index()
    g3.columns = ["Servicio", "Importe"]

    # Top clientes (acumulado)
    g4 = df[[COL_CLIENTE, COL_TOTAL]].copy()
    g4[COL_TOTAL] = _to_numeric(g4[COL_TOTAL])
    g4 = g4.groupby(COL_CLIENTE, as_index=False)[COL_TOTAL].sum().sort_values(COL_TOTAL, ascending=False)

    return g1, g2, g3, g4

def load_data():
    global DF, G_BY_PERIOD, G_BY_EMISORA, G_SERVICES, TOP_CLIENTS, LAST_LOAD_TS
    t0 = time.time()
    df = pd.read_excel(DATA_PATH, sheet_name=SHEET_BASE, engine="openpyxl")
    for c in [COL_TOTAL] + SERVICE_COLS:
        if c in df.columns:
            df[c] = _to_numeric(df[c])
    DF = df
    G_BY_PERIOD, G_BY_EMISORA, G_SERVICES, TOP_CLIENTS = _build_caches(DF)
    LAST_LOAD_TS = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[py_server] Datos cargados en {time.time()-t0:.2f}s desde {DATA_PATH}")

@app.on_event("startup")
def _startup():
    load_data()

# ---------- Modelos ----------
class Ask(BaseModel):
    prompt: str

# ---------- Endpoints básicos ----------
@app.get("/ping")
def ping():
    return {"ok": True, "file": str(DATA_PATH), "rows": int(len(DF)), "loaded_at": LAST_LOAD_TS}

@app.get("/meta")
def meta():
    prev = DF.head(3).fillna("").to_dict(orient="records")
    return {"columns": list(DF.columns), "rows": int(len(DF)), "preview": prev, "loaded_at": LAST_LOAD_TS}

# ---------- Helpers de variación ----------
def _mom_simple(df_period_value: pd.DataFrame, value_col=COL_TOTAL):
    d = _add_period_key(df_period_value).sort_values("_period_key")
    if len(d) < 2:
        return None
    p = d.iloc[-1]; q = d.iloc[-2]
    cur, prev = p[value_col], q[value_col]
    return {
        "Periodo_Actual": p[COL_PERIODO],
        "Total_Actual": cur,
        "Periodo_Anterior": q[COL_PERIODO],
        "Total_Anterior": prev,
        "Var_Abs": cur - prev,
        "Var_%": (cur - prev) / prev if prev != 0 else 0
    }

# ---------- /ask ----------
@app.post("/ask")
def ask(q: Ask):
    raw = q.prompt or ""
    t = _norm(raw)  # <- normalizado

    # --- Top N clientes (acumulado)
    if "top" in t and "cliente" in t and ("ultimo" not in t):
        m = re.search(r"top\s*(\d+)", t)
        n = int(m.group(1)) if m else 5
        g = TOP_CLIENTS.head(n).copy()
        g[COL_TOTAL] = g[COL_TOTAL].map(_fmt_currency)
        md = _df_to_md_table(g.rename(columns={COL_CLIENTE:"Cliente", COL_TOTAL:"Facturado"}))
        return {"md": f"Top {n} clientes (acumulado):\n{md}"}

    # --- Top N clientes EN EL ÚLTIMO MES (con variación vs mes anterior)
    if "top" in t and "cliente" in t and ("ultimo" in t or "mes" in t):
        m = re.search(r"top\s*(\d+)", t)
        n = int(m.group(1)) if m else 5
        tdf = DF[[COL_CLIENTE, COL_PERIODO, COL_TOTAL]].groupby([COL_CLIENTE, COL_PERIODO], as_index=False)[COL_TOTAL].sum()
        tdf = _add_period_key(tdf).sort_values("_period_key")
        if tdf.empty: return {"md":"No hay datos suficientes."}
        last_key = tdf["_period_key"].max()
        uniq_keys = sorted(tdf["_period_key"].unique())
        prev_key = uniq_keys[-2] if len(uniq_keys) >= 2 else None
        cur_per = tdf[tdf["_period_key"]==last_key]
        top_cur = cur_per.sort_values(COL_TOTAL, ascending=False).head(n)[[COL_CLIENTE, COL_TOTAL, COL_PERIODO]]
        if prev_key:
            prev = tdf[tdf["_period_key"]==prev_key][[COL_CLIENTE, COL_TOTAL]].rename(columns={COL_TOTAL:"Prev"})
            merged = top_cur.merge(prev, on=COL_CLIENTE, how="left").fillna({"Prev":0})
        else:
            merged = top_cur.assign(Prev=0)
        merged["Var_%"] = merged.apply(lambda r: (r[COL_TOTAL]-r["Prev"])/r["Prev"] if r["Prev"]!=0 else 0, axis=1)
        periodo_actual = merged[COL_PERIODO].iloc[0]
        periodo_anterior = tdf[tdf["_period_key"]==prev_key][COL_PERIODO].iloc[0] if prev_key else "—"
        out = merged[[COL_CLIENTE, COL_TOTAL, "Var_%"]].rename(columns={COL_CLIENTE:"Cliente", COL_TOTAL:"Facturado", "Var_%":"Variación %"}).copy()
        out["Facturado"]   = out["Facturado"].map(_fmt_currency)
        out["Variación %"] = out["Variación %"].map(_fmt_percent)
        md = _df_to_md_table(out)
        title = f"Top {n} clientes en {periodo_actual} (vs {periodo_anterior}):"
        return {"md": f"{title}\n{md}"}

    # --- Total por mes
    if ("por mes" in t) or ("total por mes" in t) or ("mensual" in t):
        g = G_BY_PERIOD.copy()
        g[COL_TOTAL] = g[COL_TOTAL].map(_fmt_currency)
        md = _df_to_md_table(g.rename(columns={COL_PERIODO:"Periodo", COL_TOTAL:"Total"}))
        return {"md": f"Total mensual:\n{md}"}

    # --- Emisora
    if ("emisora" in t) or ("canal" in t):
        if ("variac" in t) or ("variacion" in t):
            dd = DF[[COL_EMISORA, COL_PERIODO, COL_TOTAL]].groupby([COL_EMISORA, COL_PERIODO], as_index=False)[COL_TOTAL].sum()
            rows = []
            for em, sub in dd.groupby(COL_EMISORA):
                mrow = _mom_simple(sub[[COL_PERIODO, COL_TOTAL]])
                if mrow:
                    mrow[COL_EMISORA] = em
                    rows.append(mrow)
            if not rows: return {"md":"No hay suficientes meses para variación por emisora."}
            mom = pd.DataFrame(rows).sort_values("Var_%", ascending=False)
            view = mom[[COL_EMISORA,"Periodo_Actual","Total_Actual","Periodo_Anterior","Total_Anterior","Var_Abs","Var_%"]].copy()
            for c in ["Total_Actual","Total_Anterior","Var_Abs"]:
                view[c] = view[c].map(_fmt_currency)
            view["Var_%"] = view["Var_%"].map(_fmt_percent)
            md = _df_to_md_table(view.rename(columns={COL_EMISORA:"Emisora"}))
            return {"md": f"Variación mensual por emisora (últimos 2 meses)\n{md}"}
        else:
            g = G_BY_EMISORA.copy()
            g[COL_TOTAL] = g[COL_TOTAL].map(_fmt_currency)
            md = _df_to_md_table(g.rename(columns={COL_EMISORA:"Emisora", COL_TOTAL:"Total"}))
            return {"md": f"Ventas por emisora:\n{md}"}

    # --- Servicios
    if ("servicio" in t) or ("tipo" in t):
        if ("variac" in t) or ("variacion" in t):
            tmp = DF[[COL_PERIODO] + SERVICE_COLS].groupby(COL_PERIODO, as_index=False).sum()
            d = _add_period_key(tmp).sort_values("_period_key")
            if len(d) < 2: return {"md":"No hay suficientes meses para variación por servicio."}
            p, q = d.iloc[-1], d.iloc[-2]
            rows = []
            for col in SERVICE_COLS:
                cur, prev = p[col], q[col]
                rows.append({
                    "Servicio": col,
                    "Periodo_Actual": p[COL_PERIODO],
                    "Total_Actual": _fmt_currency(cur),
                    "Periodo_Anterior": q[COL_PERIODO],
                    "Total_Anterior": _fmt_currency(prev),
                    "Var_Abs": _fmt_currency(cur-prev),
                    "Var_%": _fmt_percent((cur-prev)/prev if prev!=0 else 0)
                })
            md = _df_to_md_table(pd.DataFrame(rows))
            return {"md": f"Variación mensual por servicio (últimos 2 meses)\n{md}"}
        else:
            subset = G_SERVICES.rename(columns={"Importe":"Importe"}).copy()
            subset["Importe"] = subset["Importe"].map(_fmt_currency)
            md = _df_to_md_table(subset)
            return {"md": f"Ventas por tipo de servicio:\n{md}"}

    # --- Cliente específico
    if "cliente" in t:
        m = re.search(r"cliente\s*([\w\s\-\#]+)", raw, flags=re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            filt = DF[DF[COL_CLIENTE].str.contains(name, case=False, na=False)]
            if len(filt) == 0:
                return {"md": f"No se encontró el cliente: {name}"}
            byp = filt.groupby(COL_PERIODO, as_index=False)[COL_TOTAL].sum()
            if ("variac" in t) or ("variacion" in t):
                mom = _mom_simple(byp[[COL_PERIODO, COL_TOTAL]])
                if not mom: return {"md": f"No hay suficientes meses para calcular variación de {name}."}
                view = pd.DataFrame([{
                    "Cliente": name,
                    "Periodo_Actual": mom["Periodo_Actual"],
                    "Total_Actual": _fmt_currency(mom["Total_Actual"]),
                    "Periodo_Anterior": mom["Periodo_Anterior"],
                    "Total_Anterior": _fmt_currency(mom["Total_Anterior"]),
                    "Var_Abs": _fmt_currency(mom["Var_Abs"]),
                    "Var_%": _fmt_percent(mom["Var_%"])
                }])
                md = _df_to_md_table(view)
                return {"md": f"Variación mensual — {name}\n{md}"}
            byp = _add_period_key(byp).sort_values("_period_key").drop(columns=["_period_key"])
            byp[COL_TOTAL] = byp[COL_TOTAL].map(_fmt_currency)
            md = _df_to_md_table(byp.rename(columns={COL_PERIODO:"Periodo", COL_TOTAL:"Total"}))
            return {"md": f"Cliente: {name}\n{md}"}

    # --- Variación mensual total
    if ("variac" in t) or ("variacion" in t) or ("mom" in t):
        dd = DF[[COL_PERIODO, COL_TOTAL]].groupby(COL_PERIODO, as_index=False)[COL_TOTAL].sum()
        mom = _mom_simple(dd)
        if not mom: return {"md":"No hay suficientes meses para calcular la variación."}
        view = pd.DataFrame([{
            "Periodo_Actual": mom["Periodo_Actual"],
            "Total_Actual": _fmt_currency(mom["Total_Actual"]),
            "Periodo_Anterior": mom["Periodo_Anterior"],
            "Total_Anterior": _fmt_currency(mom["Total_Anterior"]),
            "Var_Abs": _fmt_currency(mom["Var_Abs"]),
            "Var_%": _fmt_percent(mom["Var_%"])
        }])
        md = _df_to_md_table(view)
        return {"md": f"Variación mensual total (últimos 2 meses)\n{md}"}

    # Default: resumen
    total = float(DF[COL_TOTAL].sum())
    resumen = {
        "filas": int(len(DF)),
        "total": _fmt_currency(total),
        "clientes_unicos": int(DF[COL_CLIENTE].nunique()),
        "loaded_at": LAST_LOAD_TS,
    }
    return {"answer": resumen}

# Aliases / reload
@app.post("/")
def ask_root(q: Ask): return ask(q)

@app.get("/")
def home():
    return {"ok": True, "hint": "Ej.: 'Top 5 clientes en último mes', 'variacion por emisora', 'variacion servicio', 'variacion cliente telco 015'"}

@app.post("/reload")
def reload_data():
    load_data()
    return {"ok": True, "reloaded_at": LAST_LOAD_TS}
