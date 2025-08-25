# -*- coding: utf-8 -*-
# OLX x FIPE — v5.6
# - Orçamento + margem alvo (com tolerâncias)
# - Modelo/Estado/Cidade opcionais
# - 3 modos contra 403:
#     * Provedores via st.secrets (SCRAPERAPI_KEY / SCRAPINGBEE_KEY)
#     * Importar HTML (agora robusto: JSON embutido, regex /d/, links relativos, múltiplos seletores)
#     * Rodar localmente
# - Estimativa de FIPE (API Parallelum) por heurística (título/ano)
# - Ranking por proximidade de margem e de preço
# - Expander de depuração com contagens/amostras

import re
import csv
import time
import json
import difflib
from io import StringIO
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup
import streamlit as st
import pandas as pd
from functools import lru_cache

# ---------------------------
# Config da página
# ---------------------------
st.set_page_config(page_title='Busca OLX x FIPE — Orcamento + Margem', page_icon='🚗', layout='wide')
st.title('🚗 Busca OLX por Valor a Investir + Margem FIPE')

# ---------------------------
# Sidebar
# ---------------------------
with st.sidebar:
    st.header('Parâmetros')
    budget = st.number_input('Valor a Investir (R$)', min_value=0.0, value=30000.0, step=500.0, format='%.2f')
    margem_alvo = st.number_input('Margem desejada (R$)', min_value=0.0, value=5000.0, step=500.0, format='%.2f')
    tol_preco = st.number_input('Tolerância de preço (± R$)', min_value=0.0, value=3000.0, step=500.0, format='%.0f',
                                help='Faixa: [Investir - tol, Investir + tol]')
    tol_margem = st.number_input('Tolerância de margem (± R$)', min_value=0.0, value=2000.0, step=500.0, format='%.0f',
                                 help='Aceita margens em [Margem - tol, Margem + tol]')
    modelo = st.text_input('Modelo (opcional, ex.: Gol 2014)', value='')
    estado = st.text_input('Estado (opcional, ex.: minas-gerais)', value='')
    cidade = st.text_input('Cidade (opcional, ex.: montes-claros)', value='')
    max_pages = st.slider('Páginas a varrer', min_value=1, max_value=5, value=3,
                          help='Quantidade de páginas (parâmetro &o=)')
    only_with_price = st.checkbox('Apenas anúncios com preço', value=True)

tab_busca, tab_import = st.tabs(['Buscar online (automático)', 'Importar HTML (manual, sem 403)'])

# ---------------------------
# Utilidades
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
    'Referer': 'https://www.olx.com.br/',
}
BASE_URL = 'https://www.olx.com.br'
PRICE_RE = re.compile(r'R\$\s*[\d\.\s]+(?:,\d{2})?')
D_LINK_RE = re.compile(r'(https?://[^\s"\'<>]*olx\.com\.br[^\s"\'<>]*/d/[^\s"\'<>]*|/d/[^\s"\'<>]*)')

def parse_preco(texto: str):
    if texto is None:
        return None
    if isinstance(texto, (int, float)):
        try:
            return int(float(texto))
        except Exception:
            return None
    t = texto.replace('R$', '').replace(' ', '').replace('.', '').replace(',', '')
    nums = re.findall(r'\d+', t)
    if not nums:
        return None
    try:
        return int(''.join(nums))
    except Exception:
        return None

def _text(node):
    try:
        return node.get_text(" ", strip=True)
    except Exception:
        return ""

def _first(*vals):
    for v in vals:
        if v:
            return v
    return None

def _fmt_money(v):
    if v is None:
        return '—'
    return ('R$ {:,.2f}'.format(v)).replace(',', 'X').replace('.', ',').replace('X', '.')

def montar_url(budget: float, tol_preco_val: float, modelo: str = '', estado: str = None, cidade: str = None, page: int = 1):
    base = f'{BASE_URL}/autos-e-pecas/carros-vans-e-utilitarios'
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

def _call_scraping_provider(url: str, headers: dict):
    providers = []
    key_scraperapi = st.secrets.get('SCRAPERAPI_KEY', None)
    key_scrapingbee = st.secrets.get('SCRAPINGBEE_KEY', None)
    if key_scraperapi:
        providers.append(('scraperapi', key_scraperapi))
    if key_scrapingbee:
        providers.append(('scrapingbee', key_scrapingbee))
    for name, key in providers:
        try:
            if name == 'scraperapi':
                api_url = 'http://api.scraperapi.com'
                params = {'api_key': key, 'url': url, 'keep_headers': 'true', 'country_code': 'br'}
                r = requests.get(api_url, params=params, headers=headers, timeout=35)
            else:
                api_url = 'https://app.scrapingbee.com/api/v1/'
                params = {'api_key': key, 'url': url, 'country_code': 'br', 'block_ads': 'true'}
                r = requests.get(api_url, params=params, headers=headers, timeout=35)
            r.raise_for_status()
            return r.text
        except Exception:
            continue
    return None

def fetch(url: str, retries: int = 1, backoff: float = 1.2):
    last_err = None
    for i in range(retries + 1):
        try:
            hdrs = BASE_HDRS.copy()
            hdrs['User-Agent'] = UA_LIST[i % len(UA_LIST)]
            res = requests.get(url, headers=hdrs, timeout=25)
            res.raise_for_status()
            return res.text
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, 'status_code', None)
            if status == 403:
                html = _call_scraping_provider(url, hdrs)
                if html:
                    return html
            last_err = e
        except Exception as e:
            last_err = e
        time.sleep(backoff * (i + 1))
    raise last_err

# ---------------------------
# Importadores (HTML → anúncios)
# ---------------------------
def _find_price_near(node):
    m = PRICE_RE.search(_text(node))
    if m:
        return m.group(0)
    # pais até 3 níveis
    p = node.parent
    for _ in range(3):
        if not p:
            break
        m = PRICE_RE.search(_text(p))
        if m:
            return m.group(0)
        p = p.parent
    # irmãos próximos
    sib = getattr(node, 'next_sibling', None)
    for _ in range(3):
        if not sib:
            break
        m = PRICE_RE.search(_text(sib))
        if m:
            return m.group(0)
        sib = getattr(sib, 'next_sibling', None)
    return None

def _extract_by_selectors(soup):
    """Extrai a partir de anchors/DOM."""
    results = []
    anchors = []
    selectors = [
        'a[data-ds-component="DS-AdCard"]',
        'a[data-testid*="ad-card"]',
        'a[href*="/d/"]'
    ]
    for sel in selectors:
        try:
            anchors.extend(soup.select(sel))
        except Exception:
            pass
    # dedup
    seen_href, uniq = set(), []
    for a in anchors:
        href = a.get('href')
        if not href or href in seen_href:
            continue
        seen_href.add(href)
        uniq.append(a)
    for a in uniq:
        url = urljoin(BASE_URL, a.get('href', ''))
        if 'olx.com.br' not in url:
            continue
        titulo = _first(a.get('title'), _text(a.find('h2')), _text(a.find('h3')), _text(a))
        if not titulo:
            p = a.parent
            for _ in range(2):
                if not p:
                    break
                titulo = _first(_text(p.find('h2')), _text(p.find('h3')))
                if titulo:
                    break
                p = p.parent
        preco_txt = None
        for el in [a.find(attrs={'data-ds-component': 'DS-Price'}),
                   a.find('span', string=PRICE_RE),
                   a.find('p', string=PRICE_RE),
                   a.find('h3', string=PRICE_RE)]:
            if el:
                preco_txt = _text(el); break
        if not preco_txt:
            preco_txt = _find_price_near(a)
        results.append({
            'titulo': (titulo or '').strip() or None,
            'preco_txt': preco_txt,
            'preco_num': parse_preco(preco_txt),
            'url': url
        })
    return results, len(uniq)

def _extract_by_regex(html):
    """Extrai por regex bruto de links /d/ no HTML."""
    results = []
    links = D_LINK_RE.findall(html)
    seen = set()
    for href in links:
        url = urljoin(BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        results.append({'titulo': None, 'preco_txt': None, 'preco_num': None, 'url': url})
    return results, len(seen)

def _walk_json(obj):
    """Gera todos os dicts dentro de um JSON arbitrário."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_json(v)
    elif isinstance(obj, list):
        for x in obj:
            yield from _walk_json(x)

def _json_pick_price(d):
    # tenta vários formatos: número, dict, string
    if 'price' in d:
        v = d['price']
        if isinstance(v, (int, float, str)):
            return v
        if isinstance(v, dict):
            for k in ('value', 'amount', 'display', 'formatted', 'label'):
                if k in v:
                    return v[k]
    for k in ('amount', 'value', 'formattedPrice', 'displayPrice'):
        if k in d:
            return d[k]
    return None

def _json_pick_title(d):
    for k in ('subject', 'title', 'name', 'headline'):
        if k in d and isinstance(d[k], str):
            return d[k]
    return None

def _json_pick_url(d):
    for k in ('url', 'href', 'link', 'permalink', 'shareUrl'):
        if k in d and isinstance(d[k], str) and ('/d/' in d[k] or 'olx.com.br' in d[k]):
            return d[k]
    return None

def _extract_from_json_scripts(soup, html_text):
    """Procura <script> com JSON (application/json, ld+json, __NEXT_DATA__, etc.)."""
    results = []
    json_blocks = []
    # 1) scripts com tipo JSON
    for s in soup.find_all('script', attrs={'type': re.compile('json', re.I)}):
        txt = s.string or s.get_text()
        if txt and len(txt) > 2:
            json_blocks.append(txt)
    # 2) também varre scripts comuns e tenta localizar objetos grandes
    for s in soup.find_all('script'):
        txt = s.string or s.get_text()
        if txt and ('{' in txt and '}' in txt and ('ads' in txt or '/d/' in txt or 'price' in txt or 'subject' in txt)):
            json_blocks.append(txt)

    # 3) fallback: procurar URLs /d/ diretas no script (sem parse)
    direct_urls = set()
    for txt in json_blocks:
        for m in D_LINK_RE.findall(txt):
            direct_urls.add(urljoin(BASE_URL, m))
    for u in direct_urls:
        results.append({'titulo': None, 'preco_txt': None, 'preco_num': None, 'url': u})

    # 4) tentar carregar JSONs válidos e caminhar neles
    loaded_count = 0
    for txt in json_blocks:
        # tenta isolar um JSON puro entre chaves
        candidate = None
        t = txt.strip()
        if t.startswith('{') and t.endswith('}'):
            candidate = t
        else:
            # extrai o maior bloco {...} plausível
            start = t.find('{')
            end = t.rfind('}')
            if start != -1 and end != -1 and end > start:
                candidate = t[start:end+1]
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            loaded_count += 1
        except Exception:
            continue

        for d in _walk_json(data):
            if not isinstance(d, dict):
                continue
            url = _json_pick_url(d)
            title = _json_pick_title(d)
            price_raw = _json_pick_price(d)

            if (url and ('/d/' in url or 'olx.com.br' in url)) and (title or price_raw is not None):
                preco_num = parse_preco(price_raw)
                preco_txt = None
                if isinstance(price_raw, (int, float)):
                    preco_txt = _fmt_money(price_raw)
                elif isinstance(price_raw, str):
                    preco_txt = price_raw
                results.append({
                    'titulo': title,
                    'preco_txt': preco_txt,
                    'preco_num': preco_num,
                    'url': urljoin(BASE_URL, url),
                })

    # dedup por URL
    clean, seen = [], set()
    for r in results:
        u = r.get('url')
        if not u or u in seen:
            continue
        seen.add(u)
        clean.append(r)

    return clean, loaded_count

def extrair_anuncios(html: str):
    """Orquestra: tenta DOM -> regex -> JSON. Retorna (rows, stats_dict)."""
    soup = BeautifulSoup(html, 'html.parser')

    dom_rows, anchor_count = _extract_by_selectors(soup)
    regex_rows, regex_count = _extract_by_regex(html)
    json_rows, json_loaded = _extract_from_json_scripts(soup, html)

    combined = []
    seen = set()
    for group in (dom_rows, regex_rows, json_rows):
        for r in group:
            u = r.get('url')
            if not u or u in seen:
                continue
            seen.add(u)
            combined.append(r)

    stats = {
        'anchors': anchor_count,
        'regex_links': regex_count,
        'json_blocks_loaded': json_loaded,
        'from_dom': len(dom_rows),
        'from_regex': len(regex_rows),
        'from_json': len(json_rows),
        'total': len(combined)
    }
    return combined, stats

# ---------------------------
# FIPE (Parallelum) — heurística
# ---------------------------
FIPE_BASE = 'https://parallelum.com.br/fipe/api/v1/carros'
COMMON_BRAND_ALIASES = {
    'vw': 'Volkswagen', 'gm': 'Chevrolet', 'chevy': 'Chevrolet',
    'mercedes': 'Mercedes-Benz', 'merce': 'Mercedes-Benz', 'bmw': 'BMW',
}

@lru_cache(maxsize=1000)
def get_brands():
    r = requests.get(f'{FIPE_BASE}/marcas', timeout=20); r.raise_for_status(); return r.json()

@lru_cache(maxsize=1000)
def get_models(brand_code: str):
    r = requests.get(f'{FIPE_BASE}/marcas/{brand_code}/modelos', timeout=20); r.raise_for_status()
    return r.json().get('modelos', [])

@lru_cache(maxsize=2000)
def get_years(brand_code: str, model_code: str):
    r = requests.get(f'{FIPE_BASE}/marcas/{brand_code}/modelos/{model_code}/anos', timeout=20); r.raise_for_status()
    return r.json()

@lru_cache(maxsize=5000)
def get_price(brand_code: str, model_code: str, year_code: str):
    r = requests.get(f'{FIPE_BASE}/marcas/{brand_code}/modelos/{model_code}/anos/{year_code}', timeout=20); r.raise_for_status()
    return r.json().get('Valor')

def parse_money_br(s: str):
    if not s: return None
    s = s.replace('R$', '').replace('.', '').replace(',', '.').strip()
    try: return float(s)
    except Exception: return None

def try_extract_year(text: str):
    if not text: return None
    m = re.search(r'(19|20)\d{2}', text); return int(m.group(0)) if m else None

def find_brand_in_title(title_low: str, brands):
    for b in brands:
        if b['nome'].lower() in title_low: return b
    for alias, proper in COMMON_BRAND_ALIASES.items():
        if f' {alias} ' in f' {title_low} ':
            for b in brands:
                if b['nome'].lower() == proper.lower(): return b
    parts = title_low.split(); first = parts[0].capitalize() if parts else ''
    names = [b['nome'] for b in brands]
    guess = difflib.get_close_matches(first, names, n=1, cutoff=0.85)
    if guess:
        for b in brands:
            if b['nome'] == guess[0]: return b
    return None

def estimate_fipe_from_title(title: str):
    if not title: return None
    title_low = title.lower()
    try: brands = get_brands()
    except Exception: return None
    brand = find_brand_in_title(title_low, brands)
    if not brand: return None
    try: models = get_models(brand['codigo'])
    except Exception: return None
    model_names = [m['nome'] for m in models]
    candidates = difflib.get_close_matches(title, model_names, n=4, cutoff=0.5)
    year_num = try_extract_year(title)
    for cand in candidates:
        mdl = next((m for m in models if m['nome'] == cand), None)
        if not mdl: continue
        try: years = get_years(brand['codigo'], mdl['codigo'])
        except Exception: continue
        if not years: continue
        year_choice = None
        if year_num:
            def year_of(code_name: str):
                m = re.search(r'(19|20)\d{2}', code_name); return int(m.group(0)) if m else None
            best_delta = 10**9
            for y in years:
                ynum = year_of(y.get('nome', ''))
                if ynum is None: continue
                d = abs(ynum - year_num)
                if d < best_delta: best_delta = d; year_choice = y.get('codigo')
            if not year_choice: year_choice = years[0].get('codigo')
        else:
            year_choice = years[0].get('codigo')
        try:
            price_txt = get_price(brand['codigo'], mdl['codigo'], year_choice)
            price = parse_money_br(price_txt)
            if price: return price
        except Exception: continue
    return None

def human_money(v):
    return _fmt_money(v)

# ---------------------------
# Ajuda/links
# ---------------------------
if cidade and not estado:
    st.warning('Para filtrar por cidade, preencha também o estado (ex.: estado=minas-gerais e cidade=montes-claros).')

base_url = montar_url(budget, tol_preco, modelo, estado or None, cidade or None, 1)
st.markdown(f'🔗 **Página base da OLX (página 1):** [{base_url}]({base_url})')

# ---------------------------
# TAB 1 — Buscar online
# ---------------------------
with tab_busca:
    if st.button('🔎 Buscar anúncios (online)'):
        st.session_state['rows_online'] = []
        all_rows = []
        progress = st.progress(0.0, text='Coletando páginas...')
        log = st.empty()
        search_links = []
        for p in range(1, max_pages + 1):
            url = montar_url(budget, tol_preco, modelo, estado or None, cidade or None, p)
            search_links.append(url)
            log.write(f'Buscando página {p}/{max_pages}: {url}')
            try:
                html = fetch(url)
            except Exception as e:
                st.warning(f'Falha ao buscar página {p}: {e}')
                progress.progress(p / max_pages); continue
            rows, stats = extrair_anuncios(html)
            all_rows.extend(rows)
            time.sleep(0.6)
            progress.progress(p / max_pages)

        with st.expander('Links diretos das páginas de busca geradas'):
            for u in search_links:
                st.markdown(f'- {u}')

        if not all_rows:
            st.error('Nenhum anúncio coletado. A OLX pode estar limitando acessos do servidor.\n'
                     'Abra os links acima no navegador **ou** use a aba "Importar HTML".')
        else:
            st.success(f'Coletados {len(all_rows)} anúncios.')
            st.session_state['rows_online'] = all_rows

# ---------------------------
# TAB 2 — Importar HTML
# ---------------------------
with tab_import:
    st.write('Abra a busca no seu **navegador**, role até o fim, e salve como **"Página da Web, somente HTML" (.html)**. '
             'Faça isso para **1+ páginas** (&o=2, &o=3...). Depois envie os arquivos.')
    files = st.file_uploader('Envie um ou mais arquivos .html da OLX', type=['html', 'htm'], accept_multiple_files=True)
    if files and st.button('📥 Importar anúncios dos HTMLs'):
        st.session_state['rows_online'] = []
        imported = []
        debug = []
        for f in files:
            try:
                data = f.read()
                try:
                    html = data.decode('utf-8', errors='ignore')
                except Exception:
                    html = data.decode('latin-1', errors='ignore')
                rows, stats = extrair_anuncios(html)
                imported.extend(rows)
                debug.append((f.name, stats))
            except Exception as e:
                st.warning(f'Falha ao ler {f.name}: {e}')

        if not imported:
            st.error('Não foi possível extrair anúncios.\n'
                     'Dicas: 1) role a página até o fim antes de salvar; 2) salve como "somente HTML"; '
                     '3) o arquivo deve ter > 500 KB; 4) tente também a página 2 (&o=2).')
        else:
            st.success(f'Importados {len(imported)} anúncios de {len(files)} arquivo(s).')
            st.session_state['rows_online'] = imported

        with st.expander('Depuração — estatísticas por arquivo'):
            for name, s in debug:
                st.write(f'**{name}** → anchors:{s["anchors"]} | regex_links:{s["regex_links"]} | '
                         f'json_blocks_loaded:{s["json_blocks_loaded"]} | '
                         f'from_dom:{s["from_dom"]} | from_regex:{s["from_regex"]} | from_json:{s["from_json"]} | total:{s["total"]}')

# ---------------------------
# PÓS-COLETA (comum)
# ---------------------------
rows = st.session_state.get('rows_online', [])
if rows:
    df = pd.DataFrame(rows).drop_duplicates(subset=['url'])
    for col in ['titulo', 'preco_txt', 'preco_num', 'url']:
        if col not in df.columns: df[col] = None

    # Reforça faixa de preço
    min_p = max(0, int(budget - tol_preco)); max_p = int(budget + tol_preco)
    if only_with_price: df = df[df['preco_num'].notna()]
    df = df[(df['preco_num'].notna()) & (df['preco_num'].between(min_p, max_p))]

    if df.empty:
        st.info('Nenhum anúncio na faixa de preço após filtros.')
    else:
        st.info('Estimando FIPE por título/ano (heurística, pode levar alguns segundos)...')
        fipe_vals = []
        for t in df['titulo'].fillna('').tolist():
            try:
                fipe_val = estimate_fipe_from_title(t)
            except Exception:
                fipe_val = None
            fipe_vals.append(fipe_val); time.sleep(0.12)
        df['fipe_estimado'] = fipe_vals

        df['margem_calc'] = df.apply(
            lambda r: (r['fipe_estimado'] - r['preco_num']) if pd.notna(r['fipe_estimado']) and pd.notna(r['preco_num']) else None,
            axis=1
        )

        alvo_min = margem_alvo - tol_margem; alvo_max = margem_alvo + tol_margem
        mask_margem = df['margem_calc'].notna() & df['margem_calc'].between(alvo_min, alvo_max)
        df_sel = df[mask_margem].copy()

        if df_sel.empty:
            st.warning('Nenhum anúncio dentro da faixa de margem; exibindo anúncios com FIPE estimada para avaliação.')
            df_sel = df[df['fipe_estimado'].notna()].copy()

        if df_sel.empty:
            st.info('Sem FIPE estimada suficiente para cálculo de margem.')
        else:
            df_sel['delta_margem'] = (df_sel['margem_calc'] - margem_alvo).abs()
            df_sel['delta_preco'] = (df_sel['preco_num'] - budget).abs()
            df_sel = df_sel.sort_values(['delta_margem', 'delta_preco']).reset_index(drop=True)

            df_show = df_sel[['titulo', 'preco_num', 'fipe_estimado', 'margem_calc', 'url']].copy()
            df_show.rename(columns={
                'titulo': 'Título', 'preco_num': 'Preço (R$)',
                'fipe_estimado': 'FIPE estimada (R$)', 'margem_calc': 'Margem (FIPE − Preço)', 'url': 'Anúncio',
            }, inplace=True)

            df_show['Preço (R$)'] = df_show['Preço (R$)'].apply(human_money)
            df_show['FIPE estimada (R$)'] = df_show['FIPE estimada (R$)'].apply(human_money)
            df_show['Margem (FIPE − Preço)'] = df_show['Margem (FIPE − Preço)'].apply(human_money)

            colcfg = {}
            try: colcfg['Anúncio'] = st.column_config.LinkColumn('Anúncio')
            except Exception: pass

            st.subheader(f'Resultados ({len(df_show)})')
            st.dataframe(df_show, use_container_width=True, column_config=colcfg, hide_index=True)

            out = df_sel[['titulo', 'preco_num', 'fipe_estimado', 'margem_calc', 'url']].copy()
            csv_io = StringIO(); writer = csv.writer(csv_io, delimiter=';')
            writer.writerow(['titulo', 'preco_num', 'fipe_estimado', 'margem_calc', 'url'])
            for _, r in out.iterrows():
                writer.writerow([
                    r.get('titulo', ''), r.get('preco_num', ''), r.get('fipe_estimado', ''), r.get('margem_calc', ''), r.get('url', '')
                ])
            st.download_button('⬇️ Baixar CSV (valores numéricos)', data=csv_io.getvalue().encode('utf-8'),
                               file_name='olx_busca_orcamento_margem.csv', mime='text/csv')
