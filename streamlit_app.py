# -*- coding: utf-8 -*-
# OLX x FIPE — v5.4
# - Orçamento + margem alvo (com tolerâncias)
# - Modelo/Estado/Cidade opcionais
# - 3 modos contra 403:
#     * Provedores via st.secrets (SCRAPERAPI_KEY / SCRAPINGBEE_KEY)
#     * Importar HTML (robusto: links relativos, múltiplos seletores, preço por regex no card)
#     * Rodar localmente
# - Estimativa de FIPE (API Parallelum) por heurística (título/ano)
# - Ranking por proximidade de margem e de preço

import re
import csv
import time
import difflib
from io import StringIO
from urllib.parse import quote_plus, urljoin

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
                                help='Faixa: [Investir - tol, Investir + tol]')
    tol_margem = st.number_input('Tolerância de margem (± R$)', min_value=0.0, value=2000.0, step=500.0, format='%.0f',
                                 help='Aceita margens em [Margem - tol, Margem + tol]')
    modelo = st.text_input('Modelo (opcional, ex.: Gol 2014)', value='')
    estado = st.text_input('Estado (opcional, ex.: minas-gerais)', value='')
    cidade = st.text_input('Cidade (opcional, ex.: montes-claros)', value='')
    max_pages = st.slider('Páginas a varrer', min_value=1, max_value=5, value=3,
                          help='Quantidade de páginas (parâmetro &o=)')
    only_with_price = st.checkbox('Apenas anúncios com preço', value=True)

# ---------------------------
# Modo de operação
# ---------------------------
tab_busca, tab_import = st.tabs(['Buscar online (automático)', 'Importar HTML (manual, sem 403)'])

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
    'Referer': 'https://www.olx.com.br/',
}
BASE_URL = 'https://www.olx.com.br'

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

def _
