# -*- coding: utf-8 -*-
# OLX x FIPE — v5.1 (orçamento + margem; modelo/estado/cidade opcionais; robusto)

import re
import csv
import time
import difflib
from io import StringIO
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
import streamlit as st
import pandas as pd
from functools import lru_cache

# ---------------------------
# Configuração da página
# ---------------------------
st.set_page_config(page_title='Busca OLX x FIPE — Orcamento + Margem', page_icon='🚗', layout='wide')
st.title('🚗 Busca OLX por Valor a Investir + Margem FIPE')

# ---------------------------
# Sidebar: parâmetros do usuário
# ---------------------------
with st.sidebar:
    st.header('Parâmetros')
    budget = st.number_input('Valor a Investir (R$)', min_value=0.0, value=30000.0, step=500.0, format='%.2f')
    margem_alvo = st.number_input('Margem desejada (R$)', min_value=0.0, value=5000.0, step=500.0, format='%.2f')
    tol_preco = st.number_input('Tolerância de preço (± R$)', min_value=0.0, value=3000.0, step=500.0, format='%.0f',
                                help='Faixa de preço buscada: [Investir - tol, Investir + tol]')
    tol_margem = st.number_input('Tolerância de margem (± R$)', min_value=0.0, value=2000.0, step=500.0, format='%.0f',
                                 help='Aceita margens em [Margem - tol, Margem + tol]')
    modelo = st.text_input('Modelo (opcional, ex.: Gol 2014)', value='')
    estado = st.text_input('Estado (opcional, ex.: minas-gerais)', value='')
    cidade = st.text_input('Cidade (opcional, ex.: montes-claros)', value='')
    max_pages = st.slider('Páginas a varrer', min_value=1, max_value=5, value=3,
                          help='Quantidade de páginas de resultados da OLX a coletar (parâmetro &o=)')
    only_with_price = st.checkbox('Apenas anúncios com preço', value=True)
    buscar = st.button('🔎 Buscar anúncios', type='primary')

st.markdown(
    'Coletamos anúncios da OLX na faixa do seu orçamento (filtros `ps`/`pe`), '
    'estimamos a **FIPE** por heurística (título/ano, API pública) e calculamos a **margem (FIPE − preço)**. '
    'Ranqueamos pela proximidade da margem desejada e, em seguida, do preço ao seu orçamento.'
)

# ---------------------------
# Utilidades de scraping
# ---------------------------
UA_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
]
BASE_HDRS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
}

def parse_preco(texto: str):
    if not texto:
        return None
    t = texto.replace('.', '').replace(',', '')
    nums = re.findall(r'\d+', t)
    if not nums:
        return None
    try:
        return int(''.join(nums))
    except Exception:
        return None

def extrair_anuncios(html: str):
    """Extrai {titulo, preco_txt, preco_num, url} com tolerância a pequenas mudanças de HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    results = []

    # Estratégia principal: card DS-AdCard
    for a in soup.find_all('a', {'data-ds-component': 'DS-AdCard'}):
        titulo = a.get('title') or (a.find('h2').get_text(strip=True) if a.find('h2') else None) or a.get_text(strip=True)[:80]
        link = a.get('href')
        preco_txt = None
        for el in [
            a.find(attrs={'data-ds-component': 'DS-Price'}),
            a.find('h3'),
            a.find('span', string=re.compile(r'R\$\s*[\d\.\,]+')),
            a.find('p', string=re.compile(r'R\$\s*[\d\.\,]+')),
        ]:
            if el:
                preco_txt = el.get_text(strip=True)
                break
        results.append({'titulo': titulo, 'preco_txt': preco_txt, 'preco_num': parse_preco(preco_txt), 'url': link})

    # Fallback: âncoras genéricas com /d/ no href
    if not results:
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/d/' in href and 'olx.com.br' in href:
                titulo = a.get('title') or a.get_text(strip=True)[:80]
                preco_el = a.find('h3') or a.find('span', string=re.compile(r'R\$\s*[\d\.\,]+'))
                preco_txt = preco_el.get_text(strip=True) if preco_el else None
                results.append({'titulo': titulo, 'preco_txt': preco_txt, 'preco_num': parse_preco(preco_txt), 'url': href})

    # Dedup por URL
    uniq, seen = [], set()
    for r in results:
        u = r.get('url')
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(r)
    return uniq

def montar_url(budget: float, tol_preco_val: float, modelo: str = '', estado: str = None, cidade: str = None, page: int = 1):
    """Monta URL com filtros de preço ps (min) e pe (max). Se modelo preenchido, usa q=..."""
    base = 'https://www.olx.com.br/autos-e-pecas/carros-vans-e-utilitarios'
    path = ''
    if estado:
        e = estado.strip().strip('/').lower().replace(' ', '-')
        path += f'/{e}'
        if cidade:
            c = cidade.strip().strip('/').lower().replace(' ', '-')
            path += f'/{c}'
    ps = max(0, int(budget - tol_preco_val))
    pe = int(budget + tol_preco_val)
    params = f'?sf=1&ps={ps}&pe={pe}'
    if modelo and modelo.strip():
        params += f'&q={quote_plus(modelo.strip())}'
    if page and page > 1:
        params += f'&o={page}'
    return base + path + params

def fetch(url: str, retries: int = 2, backoff: float = 1.2):
    last_err = None
    for i in range(retries + 1):
        try:
            hdrs = BASE_HDRS.copy()
            hdrs['User-Agent'] = UA_LIST[i % len(UA_LIST)]
            res = requests.get(url, headers=hdrs, timeout=25)
            res.raise_for_status()
            return res.text
        except Exception as e:
            last_err = e
            time.sleep(backoff * (i + 1))
    raise last_err

# ---------------------------
# Estimativa de FIPE (API Parallelum) — heurística por título/ano
# ---------------------------
FIPE_BASE = 'https://parallelum.com.br/fipe/api/v1/carros'
COMMON_BRAND_ALIASES = {
    'vw': 'Volkswagen',
    'gm': 'Chevrolet',
    'chevy': 'Chevrolet',
    'mercedes': 'Mercedes-Benz',
    'merce': 'Mercedes-Benz',
    'bmw': 'BMW',
}

@lru_cache(maxsize=1000)
def get_brands():
    r = requests.get(f'{FIPE_BASE}/marcas', timeout=20)
    r.raise_for_status()
    return r.json()

@lru_cache(maxsize=1000)
def get_models(brand_code: str):
    r = requests.get(f'{FIPE_BASE}/marcas/{brand_code}/modelos', timeout=20)
    r.raise_for_status()
    return r.json().get('modelos', [])

@lru_cache(maxsize=2000)
def get_years(brand_code: str, model_code: str):
    r = requests.get(f'{FIPE_BASE}/marcas/{brand_code}/modelos/{model_code}/anos', timeout=20)
    r.raise_for_status()
    return r.json()

@lru_cache(maxsize=5000)
def get_price(brand_code: str, model_code: str, year_code: str):
    r = requests.get(f'{FIPE_BASE}/marcas/{brand_code}/modelos/{model_code}/anos/{year_code}', timeout=20)
    r.raise_for_status()
    return r.json().get('Valor')

def parse_money_br(s: str):
    if not s:
        return None
    s = s.replace('R$', '').replace('.', '').replace(',', '.').strip()
    try:
        return float(s)
    except Exception:
        return None

def try_extract_year(text: str):
    if not text:
        return None
    m = re.search(r'(19|20)\d{2}', text)
    return int(m.group(0)) if m else None

def find_brand_in_title(title_low: str, brands):
    # 1) ocorrência direta
    for b in brands:
        if b['nome'].lower() in title_low:
            return b
    # 2) aliases (vw -> Volkswagen, etc.)
    for alias, proper in COMMON_BRAND_ALIASES.items():
        if f' {alias} ' in f' {title_low} ':
            for b in brands:
                if b['nome'].lower() == proper.lower():
                    return b
    # 3) fallback: similaridade com primeira palavra
    parts = title_low.split()
    first = parts[0].capitalize() if parts else ''
    names = [b['nome'] for b in brands]
    guess = difflib.get_close_matches(first, names, n=1, cutoff=0.85)
    if guess:
        for b in brands:
            if b['nome'] == guess[0]:
                return b
    return None

def estimate_fipe_from_title(title: str):
    """Retorna FIPE estimada (float) ou None."""
    if not title:
        return None
    title_low = title.lower()

    try:
        brands = get_brands()
    except Exception:
        return None

    brand = find_brand_in_title(title_low, brands)
    if not brand:
        return None

    try:
        models = get_models(brand['codigo'])
    except Exception:
        return None

    model_names = [m['nome'] for m in models]
    candidates = difflib.get_close_matches(title, model_names, n=4, cutoff=0.5)
    year_num = try_extract_year(title)

    for cand in candidates:
        mdl = next((m for m in models if m['nome'] == cand), None)
        if not mdl:
            continue
        try:
            years = get_years(brand['codigo'], mdl['codigo'])
        except Exception:
            continue

        # escolhe ano mais próximo (se foi detectado)
        year_choice = None
        if year_num:
            def year_of(code_name: str):
                m = re.search(r'(19|20)\d{2}', code_name)
                return int(m.group(0)) if m else None
            best_delta = 10**9
            for y in years:
                ynum = year_of(y.get('nome', ''))
                if ynum is None:
                    continue
                d = abs(ynum - year_num)
                if d < best_delta:
                    best_delta = d
                    year_choice = y.get('codigo')
            if not year_choice and years:
                year_choice = years[0].get('codigo')
        else:
            year_choice = years[0].get('codigo') if years else None

        if not year_choice:
            continue

        try:
            price_txt = get_price(brand['codigo'], mdl['codigo'], year_choice)
            price = parse_money_br(price_txt)
            if price:
                return price
        except Exception:
            continue

    return None

def human_money(v):
    if v is None:
        return '—'
    return ('R$ {:,.2f}'.format(v)).replace(',', 'X').replace('.', ',').replace('X', '.')

# Link útil para verificar como a OLX está respondendo na página 1
base_url = montar_url(budget, tol_preco, modelo, estado or None, cidade or None, 1)
st.markdown(f'🔗 **Página base da OLX (página 1):** [{base_url}]({base_url})')

# ---------------------------
# Execução da busca
# ---------------------------
if buscar:
    all_rows = []
    progress = st.progress(0.0, text='Coletando páginas...')
    log = st.empty()

    for p in range(1, max_pages + 1):
        url = montar_url(budget, tol_preco, modelo, estado or None, cidade or None, p)
        log.write(f'Buscando página {p}/{max_pages}: {url}')
        try:
            html = fetch(url)
        except Exception as e:
            st.warning(f'Falha ao buscar página {p}: {e}')
            progress.progress(p / max_pages)
            continue
        rows = extrair_anuncios(html)
        all_rows.extend(rows)
        time.sleep(0.6)  # suaviza frequência para evitar bloqueios
        progress.progress(p / max_pages)

    if not all_rows:
        st.warning('Nenhum anúncio coletado. A OLX pode estar limitando acessos. '
                   'Tente reduzir páginas, mudar filtros ou rode localmente.')
        st.stop()

    df = pd.DataFrame(all_rows)
    for col in ['titulo', 'preco_txt', 'preco_num', 'url']:
        if col not in df.columns:
            df[col] = None

    # filtro de preço (reforça a faixa além do filtro da própria OLX)
    min_p = max(0, int(budget - tol_preco))
    max_p = int(budget + tol_preco)
    if only_with_price:
        df = df[df['preco_num'].notna()]
    df = df[(df['preco_num'].notna()) & (df['preco_num'].between(min_p, max_p))]

    if df.empty:
        st.info('Nenhum anúncio na faixa de preço após filtros.')
        st.stop()

    # estima FIPE para cada anúncio
    st.info('Estimando FIPE por título/ano (heurística, pode levar alguns segundos)...')
    fipe_vals = []
    for t in df['titulo'].fillna('').tolist():
        try:
            fipe_val = estimate_fipe_from_title(t)
        except Exception:
            fipe_val = None
        fipe_vals.append(fipe_val)
        time.sleep(0.2)  # evita rate limit da API pública
    df['fipe_estimado'] = fipe_vals

    # calcula margem
    df['margem_calc'] = df.apply(
        lambda r: (r['fipe_estimado'] - r['preco_num']) if pd.notna(r['fipe_estimado']) and pd.notna(r['preco_num']) else None,
        axis=1
    )

    # seleção por margem ~ desejada (com tolerância)
    alvo_min = margem_alvo - tol_margem
    alvo_max = margem_alvo + tol_margem
    mask_margem = df['margem_calc'].notna() & df['margem_calc'].between(alvo_min, alvo_max)
    df_sel = df[mask_margem].copy()

    if df_sel.empty:
        st.warning('Nenhum anúncio dentro da faixa de margem; exibindo anúncios com FIPE estimada para avaliação.')
        df_sel = df[df['fipe_estimado'].notna()].copy()

    if df_sel.empty:
        st.info('Sem FIPE estimada suficiente para cálculo de margem.')
        st.stop()

    # ranking: margem mais próxima do alvo, depois preço mais próximo do orçamento
    df_sel['delta_margem'] = (df_sel['margem_calc'] - margem_alvo).abs()
    df_sel['delta_preco'] = (df_sel['preco_num'] - budget).abs()
    df_sel = df_sel.sort_values(['delta_margem', 'delta_preco']).reset_index(drop=True)

    # exibição
    df_show = df_sel[['titulo', 'preco_num', 'fipe_estimado', 'margem_calc', 'url']].copy()
    df_show.rename(columns={
        'titulo': 'Título',
        'preco_num': 'Preço (R$)',
        'fipe_estimado': 'FIPE estimada (R$)',
        'margem_calc': 'Margem (FIPE − Preço)',
        'url': 'Anúncio',
    }, inplace=True)

    df_show['Preço (R$)'] = df_show['Preço (R$)'].apply(human_money)
    df_show['FIPE estimada (R$)'] = df_show['FIPE estimada (R$)'].apply(human_money)
    df_show['Margem (FIPE − Preço)'] = df_show['Margem (FIPE − Preço)'].apply(human_money)

    colcfg = {}
    try:
        colcfg['Anúncio'] = st.column_config.LinkColumn('Anúncio')
    except Exception:
        pass

    st.subheader(f'Resultados ({len(df_show)})')
    st.dataframe(df_show, use_container_width=True, column_config=colcfg, hide_index=True)

    # CSV com valores numéricos crus (separador ';' para Excel BR)
    out = df_sel[['titulo', 'preco_num', 'fipe_estimado', 'margem_calc', 'url']].copy()
    csv_io = StringIO()
    writer = csv.writer(csv_io, delimiter=';')
    writer.writerow(['titulo', 'preco_num', 'fipe_estimado', 'margem_calc', 'url'])
    for _, r in out.iterrows():
        writer.writerow([
            r.get('titulo', ''),
            r.get('preco_num', ''),
            r.get('fipe_estimado', ''),
            r.get('margem_calc', ''),
            r.get('url', ''),
        ])
    st.download_button('⬇️ Baixar CSV (valores numéricos)', data=csv_io.getvalue().encode('utf-8'),
                       file_name='olx_busca_orcamento_margem.csv', mime='text/csv')
