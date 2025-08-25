# -*- coding: utf-8 -*-
# OLX x FIPE — v6.0
# - Orçamento + margem alvo (com tolerâncias)
# - Modelo/Estado/Cidade opcionais
# - 2 modos: Buscar online (com provedores via st.secrets) e Importar HTML (sem 403)
# - Importar HTML lê __NEXT_DATA__.props.pageProps.ads (formato que veio nos seus arquivos)
# - Ranking por proximidade da margem e depois do preço

import re, json, time, math, unicodedata, urllib.parse as up
from functools import lru_cache
import requests, pandas as pd, streamlit as st

# ---------------------------
# Utils
# ---------------------------
def norm_txt(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()

def parse_brl_to_int(txt):
    if txt is None: return None
    if isinstance(txt, (int, float)):
        try: return int(float(txt))
        except Exception: return None
    t = str(txt).strip().replace("R$", "").replace("r$", "")
    t = t.replace(".", "").replace(" ", "").replace("\u00A0", "").replace(",", ".")
    m = re.findall(r"\d+\.?\d*", t)
    if not m: return None
    try: return int(float(m[0]))
    except Exception: return None

def fmt_brl(v):
    if v is None or (isinstance(v, float) and math.isnan(v)): return "-"
    return f"R$ {int(v):,}".replace(",", ".")

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
]

def http_get(url, timeout=40):
    headers = {"User-Agent": UA_POOL[int(time.time()) % len(UA_POOL)]}
    if "SCRAPERAPI_KEY" in st.secrets:  # opcional
        key = st.secrets["SCRAPERAPI_KEY"]
        proxy = f"https://api.scraperapi.com?api_key={key}&keep_headers=true&url={up.quote(url)}"
        return requests.get(proxy, timeout=timeout, headers=headers)
    if "SCRAPINGBEE_KEY" in st.secrets:  # opcional
        key = st.secrets["SCRAPINGBEE_KEY"]
        proxy = f"https://app.scrapingbee.com/api/v1/?api_key={key}&render_js=false&url={up.quote(url)}"
        return requests.get(proxy, timeout=timeout, headers=headers)
    return requests.get(url, timeout=timeout, headers=headers)

# ---------------------------
# FIPE (Parallelum)
# ---------------------------
FIPE_BASE = "https://parallelum.com.br/fipe/api/v1"

@lru_cache(maxsize=256)
def fipe_marcas():
    r = requests.get(f"{FIPE_BASE}/carros/marcas", timeout=30); r.raise_for_status(); return r.json()

@lru_cache(maxsize=256)
def fipe_modelos(cod_marca):
    r = requests.get(f"{FIPE_BASE}/carros/marcas/{cod_marca}/modelos", timeout=30); r.raise_for_status()
    return r.json().get("modelos", [])

@lru_cache(maxsize=256)
def fipe_anos(cod_marca, cod_modelo):
    r = requests.get(f"{FIPE_BASE}/carros/marcas/{cod_marca}/modelos/{cod_modelo}/anos", timeout=30); r.raise_for_status()
    return r.json()

def jaccard(a: str, b: str) -> float:
    sa, sb = set(a.split()), set(b.split())
    return (len(sa & sb) / len(sa | sb)) if sa and sb else 0.0

def pick_best(items, key_txt, target):
    tgt = norm_txt(target); best, best_score = None, -1
    for it in items:
        score = jaccard(norm_txt(it.get(key_txt, "")), tgt)
        if score > best_score: best, best_score = it, score
    return best, best_score

def extract_year_code(year_list, year_str):
    y = re.sub(r"[^0-9]", "", str(year_str))[:4]
    for it in year_list:
        if str(it.get("nome", "")).startswith(y): return it.get("codigo")
    return year_list[0]["codigo"] if year_list else None

def get_fipe_price_guess(brand, model, year):
    if not brand or not model or not year: return None
    try:
        marcas = fipe_marcas()
        marca, s1 = pick_best(marcas, "nome", brand)
        if not marca or s1 < 0.3: return None
        modelos = fipe_modelos(marca["codigo"])
        modelo_it, s2 = pick_best(modelos, "nome", model)
        if not modelo_it or s2 < 0.25: return None
        anos = fipe_anos(marca["codigo"], modelo_it["codigo"])
        ycode = extract_year_code(anos, str(year))
        if not ycode: return None
        r = requests.get(f"{FIPE_BASE}/carros/marcas/{marca['codigo']}/modelos/{modelo_it['codigo']}/anos/{ycode}", timeout=30)
        r.raise_for_status()
        return parse_brl_to_int(r.json().get("Valor"))
    except Exception:
        return None

# ---------------------------
# OLX URLs
# ---------------------------
def olx_base_url(valor, tol_preco, estado=None, cidade=None, modelo=None):
    ps, pe = max(0, int(valor - tol_preco)), int(valor + tol_preco)
    path = "/autos-e-pecas/carros-vans-e-utilitarios"
    if estado:
        path += f"/{norm_txt(estado).replace(' ', '-')}"
        if cidade: path += f"/{norm_txt(cidade).replace(' ', '-')}"
    params = {"sf": "1", "ps": str(ps), "pe": str(pe)}
    if modelo: params["q"] = modelo
    return f"https://www.olx.com.br{path}?{up.urlencode(params)}"

def list_search_pages(base_url, pages=1):
    urls = [base_url]
    for i in range(2, pages + 1):
        sep = "&" if "?" in base_url else "?"
        urls.append(f"{base_url}{sep}o={i}")
    return urls

# ---------------------------
# Importadores
# ---------------------------
def parse_next_data_from_html(html_text):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, flags=re.DOTALL)
    if not m: return None
    try: return json.loads(m.group(1))
    except Exception: return None

def ads_from_next_data(nd):
    return (nd or {}).get("props", {}).get("pageProps", {}).get("ads", []) or []

def collect_ads_online(base_url, pages):
    ads, errs = [], []
    for i, url in enumerate(list_search_pages(base_url, pages), 1):
        st.write(f"Buscando página {i}/{pages}: {url}")
        try:
            r = http_get(url, timeout=40); r.raise_for_status()
            nd = parse_next_data_from_html(r.text)
            page_ads = ads_from_next_data(nd)
            if not page_ads:
                errs.append(f"Página {i}: nenhum anúncio no __NEXT_DATA__ (pode ser bloqueio/403 render-side).")
            ads.extend(page_ads)
        except Exception as e:
            errs.append(f"Página {i}: {e}")
    return ads, errs

def collect_ads_from_uploaded(files):
    ads, details = [], []
    for f in files:
        try:
            txt = f.read().decode("utf-8", errors="ignore")
            nd = parse_next_data_from_html(txt)
            page_ads = ads_from_next_data(nd)
            details.append(f"{f.name}: {len(page_ads)} anúncios do __NEXT_DATA__.")
            ads.extend(page_ads)
        except Exception:
            details.append(f"{f.name}: erro ao ler.")
    return ads, details

def ad_to_row(ad):
    title = ad.get("subject") or ad.get("title") or ""
    link  = ad.get("friendlyUrl") or ad.get("url") or ""
    price_txt = ad.get("priceValue") or ad.get("price") or ""
    price_num = parse_brl_to_int(price_txt)

    props = {p.get("name"): p.get("value") for p in (ad.get("properties") or [])}
    brand = props.get("vehicle_brand") or props.get("brand") or ""
    model = props.get("vehicle_model") or props.get("model") or ""
    year  = props.get("regdate") or props.get("year") or ""
    km    = props.get("mileage") or props.get("km") or ""
    city  = ad.get("location") or props.get("municipality") or ""

    return {
        "titulo": title,
        "preco_txt": price_txt,
        "preco_num": price_num,
        "marca": brand,
        "modelo": model,
        "ano": str(year)[:4] if year else "",
        "km": km,
        "cidade": city,
        "link": link,
    }

def enrich_with_fipe(rows, want=True):
    if not want:
        for r in rows: r["fipe"], r["margem"] = None, None
        return rows
    for r in rows:
        fipe_val = get_fipe_price_guess(r["marca"], r["modelo"], r["ano"])
        r["fipe"] = fipe_val
        r["margem"] = (fipe_val - r["preco_num"]) if (fipe_val and r["preco_num"]) else None
    return rows

def filter_rank(rows, valor, margem, tol_preco, tol_margem, only_price=True):
    df = pd.DataFrame(rows)
    if df.empty: return df
    if only_price: df = df[df["preco_num"].notna()]
    lo, hi = max(0, int(valor - tol_preco)), int(valor + tol_preco)
    df = df[(df["preco_num"] >= lo) & (df["preco_num"] <= hi)]
    if df.empty: return df

    alvo_lo, alvo_hi = int(margem - tol_margem), int(margem + tol_margem)
    if "margem" in df.columns:
        ok, na = df[df["margem"].notna()], df[df["margem"].isna()]
        ok = ok[(ok["margem"] >= alvo_lo) & (ok["margem"] <= alvo_hi)]
        df = pd.concat([ok, na], ignore_index=True)
        df["score_margem"] = df["margem"].apply(lambda x: abs(x - margem) if pd.notna(x) else 10**9)
    else:
        df["score_margem"] = 10**9
    df["score_preco"] = (df["preco_num"] - valor).abs()
    return df.sort_values(["score_margem", "score_preco"]).reset_index(drop=True)

def show_results(df: pd.DataFrame):
    out = df.copy()
    out["Preço"] = out["preco_num"].apply(fmt_brl)
    out["FIPE"]  = out["fipe"].apply(fmt_brl)
    out["Margem (FIPE − preço)"] = out["margem"].apply(fmt_brl)
    out = out[["titulo","marca","modelo","ano","km","cidade","Preço","FIPE","Margem (FIPE − preço)","link"]]
    out = out.rename(columns={"titulo":"Título","marca":"Marca","modelo":"Modelo","ano":"Ano","km":"KM","cidade":"Local","link":"Link"})
    st.success(f"Encontrados {len(out)} anúncios (ordenado por proximidade da margem e do preço).")
    st.dataframe(out, use_container_width=True)

# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="Busca OLX por Valor + Margem FIPE", layout="wide")
st.title("🚗 Busca OLX por Valor a Investir + Margem FIPE")
st.write("Informe seu **orçamento** e a **margem desejada**. Buscamos na OLX pela faixa de preço (ps/pe), "
         "estimamos a FIPE (API pública) e calculamos a margem (FIPE − preço).")

with st.sidebar:
    st.header("Parâmetros")
    valor = st.number_input("Valor a investir (R$)", min_value=0, value=30000, step=500)
    margem_desejada = st.number_input("Margem desejada (R$)", min_value=0, value=5000, step=500)
    tol_preco = st.number_input("Tolerância de preço (± R$)", min_value=0, value=3000, step=500)
    tol_margem = st.number_input("Tolerância de margem (± R$)", min_value=0, value=2000, step=500)
    modelo = st.text_input("Modelo (opcional, ex.: Gol 2014)", "")
    estado = st.text_input("Estado (opcional, ex.: minas-gerais)", "")
    cidade = st.text_input("Cidade (opcional, ex.: montes-claros)", "")
    pages = st.slider("Páginas a varrer", 1, 5, 2, help="Quantidade de páginas (&o=2, &o=3…).")
    only_with_price = st.checkbox("Apenas anúncios com preço numérico", True)

tab1, tab2 = st.tabs(["Buscar online (automático)", "Importar HTML (manual, sem 403)"])

# -------- Online
with tab1:
    base = olx_base_url(valor, tol_preco, estado or None, cidade or None, modelo or None)
    st.markdown(f"🔗 **Página base da OLX (página 1):** {base}")
    if st.button("🔎 Buscar online"):
        ads, errs = collect_ads_online(base, pages)
        for e in errs: st.warning(e)
        rows = [ad_to_row(a) for a in ads]
        rows = enrich_with_fipe(rows, want=True)
        df = filter_rank(rows, valor, margem_desejada, tol_preco, tol_margem, only_price=only_with_price)
        if df.empty: st.error("Nenhum anúncio encontrado.")
        else: show_results(df)
        st.caption("Se der **403/Forbidden**, use a aba Importar HTML ou configure um provedor em **Settings → Secrets** "
                   "(`SCRAPERAPI_KEY` ou `SCRAPINGBEE_KEY`).")

# -------- Import
with tab2:
    st.write("Abra a busca no navegador, **role até o fim**, salve como **“Página da Web, somente HTML (.html)”**. "
             "Faça isso para 1+ páginas (&o=2, &o=3…). Envie os arquivos.")
    files = st.file_uploader("Envie os .html da OLX", type=["html","htm"], accept_multiple_files=True)
    if st.button("📥 Importar anúncios dos HTMLs") and files:
        ads, details = collect_ads_from_uploaded(files)
        for d in details: st.write(d)
        rows = [ad_to_row(a) for a in ads]
        rows = enrich_with_fipe(rows, want=True)
        df = filter_rank(rows, valor, margem_desejada, tol_preco, tol_margem, only_price=only_with_price)
        if df.empty: st.error("Não foi possível extrair anúncios (verifique se rolou até o fim e salvou como somente HTML).")
        else: show_results(df)
