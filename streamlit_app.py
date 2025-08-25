import re
import os
import json
import time
import math
import unicodedata
import urllib.parse as up
from functools import lru_cache

import pandas as pd
import requests
import streamlit as st

# =========================
# Utilidades
# =========================
def norm_txt(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()

def parse_brl_to_int(txt):
    if not txt or not isinstance(txt, str):
        return None
    # Aceita formatos: "R$ 32.000", "R$ 32.000,00", "32000", "32.000"
    t = txt.strip()
    t = t.replace("R$", "").replace("r$", "")
    t = t.replace(".", "").replace(" ", "")
    t = t.replace("\u00A0", "")  # non-breaking
    t = t.replace(",", ".")
    try:
        val = float(t)
        return int(round(val))
    except:
        return None

def fmt_brl(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "-"
    return f"R$ {int(v):,}".replace(",", ".")  # formatação simples R$ 12.345

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
]

def http_get(url, timeout=30):
    """GET com opção de provedor (ScraperAPI / ScrapingBee) via st.secrets para contornar 403."""
    headers = {"User-Agent": UA_POOL[int(time.time()) % len(UA_POOL)]}
    # Provedor opcional
    if "SCRAPERAPI_KEY" in st.secrets:
        key = st.secrets["SCRAPERAPI_KEY"]
        proxy = f"https://api.scraperapi.com?api_key={key}&keep_headers=true&url={up.quote(url)}"
        return requests.get(proxy, timeout=timeout, headers=headers)
    if "SCRAPINGBEE_KEY" in st.secrets:
        key = st.secrets["SCRAPINGBEE_KEY"]
        proxy = f"https://app.scrapingbee.com/api/v1/?api_key={key}&render_js=false&url={up.quote(url)}"
        return requests.get(proxy, timeout=timeout, headers=headers)
    return requests.get(url, timeout=timeout, headers=headers)

# =========================
# FIPE (Parallelum)
# =========================
FIPE_BASE = "https://parallelum.com.br/fipe/api/v1"

@lru_cache(maxsize=256)
def fipe_marcas():
    r = requests.get(f"{FIPE_BASE}/carros/marcas", timeout=30)
    r.raise_for_status()
    return r.json()

@lru_cache(maxsize=256)
def fipe_modelos(cod_marca):
    r = requests.get(f"{FIPE_BASE}/carros/marcas/{cod_marca}/modelos", timeout=30)
    r.raise_for_status()
    return r.json().get("modelos", [])

@lru_cache(maxsize=256)
def fipe_anos(cod_marca, cod_modelo):
    r = requests.get(f"{FIPE_BASE}/carros/marcas/{cod_marca}/modelos/{cod_modelo}/anos", timeout=30)
    r.raise_for_status()
    return r.json()

def pick_best(items, key_txt, target):
    """Escolhe o item com melhor similaridade do texto alvo."""
    target_n = norm_txt(target)
    best, best_score = None, -1
    for it in items:
        txt = it.get(key_txt, "")
        score = jaccard(norm_txt(txt), target_n)
        if score > best_score:
            best, best_score = it, score
    return best, best_score

def jaccard(a: str, b: str) -> float:
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)

def extract_year_code(year_list, year_str):
    """Procura pelo ano no formato '2013 Gasolina' etc."""
    y = re.sub(r"[^0-9]", "", str(year_str))[:4]
    for it in year_list:
        if str(it.get("nome", "")).startswith(y):
            return it.get("codigo")
    # fallback: primeiro
    return year_list[0]["codigo"] if year_list else None

def get_fipe_price_guess(brand, model, year):
    """Heurística rápida: casa Marca -> Modelo -> Ano e retorna valor FIPE int."""
    if not brand or not model or not year:
        return None
    try:
        marcas = fipe_marcas()
        marca, score_marca = pick_best(marcas, "nome", brand)
        if not marca or score_marca < 0.3:
            return None
        modelos = fipe_modelos(marca["codigo"])
        modelo_item, score_modelo = pick_best(modelos, "nome", model)
        if not modelo_item or score_modelo < 0.25:
            return None
        anos = fipe_anos(marca["codigo"], modelo_item["codigo"])
        ycode = extract_year_code(anos, str(year))
        if not ycode:
            return None
        url = f"{FIPE_BASE}/carros/marcas/{marca['codigo']}/modelos/{modelo_item['codigo']}/anos/{ycode}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        return parse_brl_to_int(data.get("Valor"))
    except Exception:
        return None

# =========================
# OLX – montar URL e coletar
# =========================
def olx_base_url(valor, tol_preco, estado=None, cidade=None, modelo=None):
    # Faixa: [valor - tol, valor + tol]
    ps = max(0, int(valor - tol_preco))
    pe = int(valor + tol_preco)
    path = "/autos-e-pecas/carros-vans-e-utilitarios"
    if estado:
        path += f"/{norm_txt(estado).replace(' ', '-')}"
        if cidade:
            path += f"/{norm_txt(cidade).replace(' ', '-')}"
    params = {"sf": "1", "ps": str(ps), "pe": str(pe)}
    if modelo:
        params["q"] = modelo
    return f"https://www.olx.com.br{path}?{up.urlencode(params)}"

def list_search_pages(base_url, pages=1):
    urls = [base_url]
    for i in range(2, pages + 1):
        # paginação da OLX usa &o=2, &o=3, ...
        sep = "&" if "?" in base_url else "?"
        urls.append(f"{base_url}{sep}o={i}")
    return urls

def parse_next_data_from_html(html_text):
    """Extrai objeto JSON do <script id="__NEXT_DATA__">..."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, flags=re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def ads_from_next_data(nd):
    """Lê pageProps.ads"""
    try:
        return nd.get("props", {}).get("pageProps", {}).get("ads", []) or []
    except Exception:
        return []

def collect_ads_online(base_url, pages):
    ads = []
    errs = []
    for i, url in enumerate(list_search_pages(base_url, pages), 1):
        st.write(f"Buscando página {i}/{pages}: {url}")
        try:
            r = http_get(url, timeout=40)
            r.raise_for_status()
            nd = parse_next_data_from_html(r.text)
            page_ads = ads_from_next_data(nd)
            if not page_ads:
                errs.append(f"Nenhum anúncio extraído da página {i} (talvez a página seja SPA carregada via JS).")
            ads.extend(page_ads)
        except Exception as e:
            errs.append(f"Falha ao buscar página {i}: {e}")
    return ads, errs

def collect_ads_from_uploaded(files):
    ads = []
    details = []
    for f in files:
        try:
            txt = f.read().decode("utf-8", errors="ignore")
            nd = parse_next_data_from_html(txt)
            page_ads = ads_from_next_data(nd)
            details.append(f"{f.name}: {len(page_ads)} anúncios extraídos.")
            ads.extend(page_ads)
        except Exception:
            details.append(f"{f.name}: erro ao ler.")
    return ads, details

def ad_to_row(ad):
    # Campos principais vindos do __NEXT_DATA__
    title = ad.get("subject") or ad.get("title") or ""
    link = ad.get("friendlyUrl") or ""
    price_txt = ad.get("priceValue") or ad.get("price") or ""
    price_num = parse_brl_to_int(price_txt)

    # Propriedades (marca, modelo, ano, km, cidade etc.)
    props = {p.get("name"): p.get("value") for p in (ad.get("properties") or [])}
    brand = props.get("vehicle_brand") or props.get("brand") or ""
    model = props.get("vehicle_model") or props.get("model") or ""
    year = props.get("regdate") or props.get("year") or ""
    km = props.get("mileage") or props.get("km") or ""
    location = ad.get("location") or props.get("municipality") or ""

    return {
        "titulo": title,
        "preco_txt": price_txt,
        "preco_num": price_num,
        "marca": brand,
        "modelo": model,
        "ano": str(year)[:4] if year else "",
        "km": km,
        "cidade": location,
        "link": link,
    }

def enrich_with_fipe(rows, want_fipe=True):
    if not want_fipe:
        for r in rows:
            r["fipe"] = None
            r["margem"] = None
        return rows

    for r in rows:
        fipe_val = get_fipe_price_guess(r["marca"], r["modelo"], r["ano"])
        r["fipe"] = fipe_val
        r["margem"] = (fipe_val - r["preco_num"]) if (fipe_val and r["preco_num"]) else None
    return rows

def filter_rank(rows, valor, margem, tol_preco, tol_margem, only_numeric_price=True):
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    if only_numeric_price:
        df = df[df["preco_num"].notna()]

    # filtro por preço dentro de [valor - tol_preco, valor + tol_preco]
    lo, hi = max(0, int(valor - tol_preco)), int(valor + tol_preco)
    df = df[(df["preco_num"] >= lo) & (df["preco_num"] <= hi)]

    # margem desejada com tolerância (se tivermos FIPE)
    if "margem" in df.columns:
        alvo_lo, alvo_hi = int(margem - tol_margem), int(margem + tol_margem)
        # mantenha anúncios sem FIPE (NaN) como “indefinidos” (não filtra), mas ranqueia depois
        df_ok = df[df["margem"].notna()]
        df_na = df[df["margem"].isna()]
        df_ok = df_ok[(df_ok["margem"] >= alvo_lo) & (df_ok["margem"] <= alvo_hi)]
        df = pd.concat([df_ok, df_na], ignore_index=True)

        # ranqueia por proximidade da margem e depois do preço
        df["score_margem"] = df["margem"].apply(lambda x: abs(x - margem) if pd.notna(x) else 10**9)
    else:
        df["score_margem"] = 10**9

    df["score_preco"] = (df["preco_num"] - valor).abs()
    df = df.sort_values(by=["score_margem", "score_preco"], ascending=[True, True])
    return df

# =========================
# UI
# =========================
st.set_page_config(page_title="Busca OLX por Valor + Margem FIPE", layout="wide")
st.title("🚗 Busca OLX por Valor a Investir + Margem FIPE")

st.write(
    "Coletamos anúncios da OLX na faixa do seu orçamento (filtros **ps/pe**), estimamos a **FIPE** por heurística (título/ano, API pública) e calculamos a **margem** (**FIPE – preço**). "
    "Ranqueamos pela **proximidade da margem desejada** e, em seguida, do **preço** ao seu orçamento."
)

with st.sidebar:
    st.header("Parâmetros")

    valor = st.number_input("Valor a investir (R$)", min_value=0, value=30000, step=500)
    margem_desejada = st.number_input("Margem desejada (± R$)", min_value=0, value=5000, step=500)
    tol_preco = st.number_input("Tolerância de preço (± R$)", min_value=0, value=3000, step=500)
    tol_margem = st.number_input("Tolerância de margem (± R$)", min_value=0, value=2000, step=500)

    modelo = st.text_input("Modelo (opcional, ex.: Gol 2014)", "")
    estado = st.text_input("Estado (opcional, ex.: minas-gerais)", "")
    cidade = st.text_input("Cidade (opcional, ex.: montes-claros)", "")

    pages = st.slider("Páginas a varrer", min_value=1, max_value=5, value=2, help="Quantas páginas da busca da OLX abrir (cada uma tem ~50 anúncios). A paginação usa o parâmetro &o=2, &o=3, ...")

    only_with_price = st.checkbox("Apenas anúncios com preço numérico", value=True)

tabs = st.tabs(["Buscar online (automático)", "Importar HTML (manual, sem 403)"])

# --------- ONLINE
with tabs[0]:
    base = olx_base_url(valor, tol_preco, estado=estado or None, cidade=cidade or None, modelo=modelo or None)
    st.write("🔗 **Página base da OLX (página 1):**", base)

    if st.button("🔎 Buscar online"):
        st.info("Coletando páginas...")
        ads, errs = collect_ads_online(base, pages)
        for e in errs:
            st.warning(e)

        rows = [ad_to_row(a) for a in ads]
        rows = enrich_with_fipe(rows, want_fipe=True)
        df = filter_rank(rows, valor, margem_desejada, tol_preco, tol_margem, only_numeric_price=only_with_price)

        if df.empty:
            st.error("Nenhum anúncio encontrado.")
        else:
            show_results(df, valor, margem_desejada)

        st.caption("Se aparecer **403/Forbidden**, a OLX está limitando acessos do servidor. "
                   "Abra os links acima no seu navegador (funciona) ou use a aba **Importar HTML**. "
                   "Opcionalmente, no Streamlit Cloud adicione **Settings → Secrets**: `SCRAPERAPI_KEY` ou `SCRAPINGBEE_KEY` para evitar 403.")

# --------- IMPORT
with tabs[1]:
    st.write("Abra a busca da OLX no seu navegador (com os filtros), **role até o fim** para carregar tudo, depois **Ctrl+S → “Página da Web, somente HTML (.html)”**. Faça isso para 1 ou mais páginas (paginação `&o=2`, `&o=3`, ...). Envie os arquivos abaixo.")
    upl = st.file_uploader("Envie um ou mais arquivos .html das páginas da OLX", type=["html", "htm"], accept_multiple_files=True)

    if st.button("📥 Importar anúncios dos HTMLs") and upl:
        ads, details = collect_ads_from_uploaded(upl)
        for d in details:
            st.write(d)

        rows = [ad_to_row(a) for a in ads]
        rows = enrich_with_fipe(rows, want_fipe=True)
        df = filter_rank(rows, valor, margem_desejada, tol_preco, tol_margem, only_numeric_price=only_with_price)

        if df.empty:
            st.error("Não foi possível extrair anúncios dos arquivos enviados. Dicas: 1) role a página até o fim; 2) salve como “somente HTML”; 3) tente páginas `&o=2`, `&o=3` etc.")
        else:
            show_results(df, valor, margem_desejada)

# =========================
# Exibição dos resultados
# =========================
def show_results(df: pd.DataFrame, valor_alvo: int, margem_alvo: int):
    out = df.copy()
    # colunas bonitas
    out["Preço"] = out["preco_num"].apply(fmt_brl)
    out["FIPE"] = out["fipe"].apply(fmt_brl)
    out["Margem (FIPE − preço)"] = out["margem"].apply(fmt_brl)
    out = out[[
        "titulo", "marca", "modelo", "ano", "km", "cidade",
        "Preço", "FIPE", "Margem (FIPE − preço)", "link"
    ]].rename(columns={
        "titulo": "Título",
        "marca": "Marca",
        "modelo": "Modelo",
        "ano": "Ano",
        "km": "KM",
        "cidade": "Local",
        "link": "Link"
    })

    st.success(
        f"Encontrados {len(out)} anúncios. Ordenado por proximidade da **margem desejada** (± tolerância) e depois pela proximidade do **preço** ao seu orçamento."
    )
    st.dataframe(out, use_container_width=True)
