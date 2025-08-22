# -*- coding: utf-8 -*-
"""
OLX x FIPE — Busca anúncios até (FIPE − margem)
Versão web (Streamlit) — Revisada

Melhorias:
- Varredura de múltiplas páginas (parâmetro `o=` da OLX)
- Alternativa para incluir/excluir anúncios sem preço numérico
- Retentativas simples em caso de falha de rede
- Coluna de link clicável na tabela
"""
import re
import csv
import time
from io import StringIO
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Busca OLX x FIPE", page_icon="🚗", layout="wide")
st.title("🚗 Busca OLX abaixo da FIPE")

st.markdown("Preencha **Valor FIPE**, **Margem** e **Modelo** (ex.: *Onix Premier 2022*). "
            "Opcionalmente especifique **Estado** e **Cidade** para refinar.")

with st.sidebar:
    st.header("Parâmetros")
    fipe_valor = st.number_input("Valor FIPE (R$)", min_value=0.0, value=89424.0, step=100.0, format="%.2f")
    margem = st.number_input("Margem abaixo (R$)", min_value=0.0, value=10000.0, step=500.0, format="%.2f")
    modelo = st.text_input("Modelo", value="Onix Premier 2022")
    estado = st.text_input("Estado (opcional, ex.: minas-gerais)", value="")
    cidade = st.text_input("Cidade (opcional, ex.: belo-horizonte)", value="")
    max_pages = st.slider("Páginas a varrer", min_value=1, max_value=5, value=2, help="Número de páginas de resultados a coletar")
    include_no_price = st.checkbox("Incluir anúncios sem preço numérico", value=True)
    buscar = st.button("🔎 Buscar anúncios", type="primary")

teto = max(0, fipe_valor - margem)
st.markdown(f"**Teto de preço (FIPE − margem):** R$ {teto:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

def parse_preco(texto: str):
    if not texto:
        return None
    # Remove pontos separadores de milhar
    texto = texto.replace(".", "")
    nums = re.findall(r"\d+", texto)
    if not nums:
        return None
    try:
        valor = int("".join(nums))
        return valor
    except Exception:
        return None

def extrair_anuncios(html: str):
    """Retorna lista de dicts {titulo, preco_txt, preco_num, url}"""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Estratégia principal: cards DS-AdCard
    cards = soup.find_all("a", {"data-ds-component": "DS-AdCard"})
    for a in cards:
        titulo = a.get("title") or (a.find("h2").get_text(strip=True) if a.find("h2") else None) or a.get_text(strip=True)[:80]
        link = a.get("href")
        preco_el = None
        # Tenta várias formas de localizar o preço
        for sel in [
            lambda node: node.find(attrs={"data-ds-component": "DS-Price"}),
            lambda node: node.find("h3"),
            lambda node: node.find("span", string=re.compile(r"R\$\s*[\d\.\,]+")),
            lambda node: node.find("p", string=re.compile(r"R\$\s*[\d\.\,]+"))
        ]:
            preco_el = sel(a)
            if preco_el:
                break
        preco_txt = preco_el.get_text(strip=True) if preco_el else None
        results.append({"titulo": titulo, "preco_txt": preco_txt, "preco_num": parse_preco(preco_txt), "url": link})

    # Fallback: âncoras genéricas para páginas de detalhe ("/d/")
    if not results:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/d/" in href and "olx.com.br" in href:
                titulo = a.get("title") or a.get_text(strip=True)[:80]
                preco_el = a.find("h3") or a.find("span", string=re.compile(r"R\$\s*[\d\.\,]+"))
                preco_txt = preco_el.get_text(strip=True) if preco_el else None
                results.append({"titulo": titulo, "preco_txt": preco_txt, "preco_num": parse_preco(preco_txt), "url": href})

    # Limpa e deduplica por URL
    clean = [r for r in results if (r.get("titulo") or r.get("preco_txt")) and r.get("url")]
    seen = set(); uniq = []
    for r in clean:
        if r["url"] in seen: 
            continue
        seen.add(r["url"])
        uniq.append(r)
    return uniq

def montar_url(modelo: str, teto: float, estado: str=None, cidade: str=None, page:int=1):
    base = "https://www.olx.com.br/autos-e-pecas/carros-vans-e-utilitarios"
    path = ""
    if estado:
        estado = estado.strip().strip("/").lower().replace(" ", "-")
        path += f"/{estado}"
        if cidade:
            cidade = cidade.strip().strip("/").lower().replace(" ", "-")
            path += f"/{cidade}"
    params = f"?q={quote_plus(modelo)}&sf=1&pe={int(teto)}"
    if page and page > 1:
        params += f"&o={page}"
    return base + path + params

def fetch(url: str, retries:int=2, backoff:float=1.5):
    last_err = None
    for i in range(retries+1):
        try:
            ua = UA_LIST[i % len(UA_LIST)]
            res = requests.get(url, headers={"User-Agent": ua}, timeout=25)
            res.raise_for_status()
            return res.text
        except Exception as e:
            last_err = e
            time.sleep(backoff*(i+1))
    raise last_err

st.markdown(f"🔗 **Página base da OLX (página 1):** [{montar_url(modelo, teto, estado or None, cidade or None, 1)}]({montar_url(modelo, teto, estado or None, cidade or None, 1)})")

if buscar:
    if not modelo.strip():
        st.error("Informe o **Modelo** (ex.: 'Onix Premier 2022').")
        st.stop()

    all_rows = []
    progress = st.progress(0.0, text="Coletando páginas...")
    status_text = st.empty()

    for p in range(1, max_pages+1):
        url = montar_url(modelo, teto, estado or None, cidade or None, p)
        status_text.write(f"Buscando página {p}/{max_pages}: {url}")
        try:
            html = fetch(url)
        except Exception as e:
            st.warning(f"Falha ao buscar página {p}: {e}")
            continue
        rows = extrair_anuncios(html)
        all_rows.extend(rows)
        progress.progress(p/max_pages)

    # Dedup final
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"])

    # Filtro por preço (<= teto) e inclusão opcional de sem preço
    mask_preco = (df["preco_num"].notna() & (df["preco_num"] <= int(teto)))
    if include_no_price:
        mask = mask_preco | (df["preco_num"].isna())
    else:
        mask = mask_preco
    df = df[mask].copy()

    # Ordena: preço numérico ascendente (None por último)
    df["ord"] = df["preco_num"].fillna(10**12)
    df = df.sort_values("ord").drop(columns=["ord"])

    st.subheader(f"Resultados ({len(df)})")
    if df.empty:
        st.info("Nenhum anúncio encontrado dentro desse valor.")
    else:
        # Ajuste visual e link clicável
        df_display = df[["titulo", "preco_txt", "preco_num", "url"]].rename(columns={
            "titulo": "Título", "preco_txt": "Preço (texto)", "preco_num": "Preço (numérico)", "url": "Anúncio"
        })
        st.dataframe(
            df_display,
            use_container_width=True,
            column_config={
                "Anúncio": st.column_config.LinkColumn("Anúncio"),
                "Preço (numérico)": st.column_config.NumberColumn(format="%d")
            }
        )

        # CSV download
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer, delimiter=";")
        writer.writerow(["titulo", "preco_txt", "preco_num", "url", "teto_aplicado"])
        for _, r in df.iterrows():
            writer.writerow([r.get("titulo",""), r.get("preco_txt",""), r.get("preco_num",""), r.get("url",""), int(teto)])
        st.download_button("⬇️ Baixar CSV", data=csv_buffer.getvalue().encode("utf-8"), file_name="olx_fipe_resultados.csv", mime="text/csv")

st.caption("Aviso: a OLX pode alterar o layout ou limitar acessos automatizados. "
           "Se a captura falhar, tente novamente depois, reduza a frequência ou ajuste os seletores no código. "
           "Verifique sempre o histórico do veículo (sinistro, leilão etc.).")
