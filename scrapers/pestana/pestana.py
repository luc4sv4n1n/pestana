#!/usr/bin/env python3
"""
pestana.py — Scraper Pestana Leilões → auctions.veiculos

Coleta veículos retomados via Playwright (site renderizado por JS),
enriquece com FIPE via DDG e sobe pro Supabase.

Uso local (debug):
    python pestana.py --no-upload
    python pestana.py --no-upload --output pestana_debug.json

GitHub Actions (produção):
    python pestana.py --output /tmp/pestana_coleta.json

Dependências:
    pip install playwright httpx
    playwright install chromium
"""

import asyncio
import json
import re
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fipe_client import buscar_valor_mercado, fmt_brl as _fmt_ddg, _detectar_categorias, _parse_titulo


CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"


# ─── Config ───────────────────────────────────────────────────────────────────

BASE_URL     = "https://www.pestanaleiloes.com.br"
LISTAGEM_URL = (
    BASE_URL + "/leilao-de-veiculos"
    "?anoMax=2025&anoMin=2014&lotePage={pagina}&loteQty=96&origem=Retomado"
)

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ─── Parsers ──────────────────────────────────────────────────────────────────

def parse_brl(v) -> float | None:
    if v is None:
        return None
    s = str(v).replace("R$", "").replace("\xa0", "").replace(" ", "").strip()
    # Formato BR: 1.234,56
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        val = float(s)
        return val if 500 <= val <= 10_000_000 else None
    except Exception:
        return None


def fmt_brl(v) -> str:
    val = parse_brl(v)
    if val is None:
        return "—"
    s = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def parse_ano(titulo: str) -> tuple[int | None, int | None]:
    """
    Pega os dois ÚLTIMOS anos do título — evita confundir nome de modelo
    com ano de fabricação (ex: "Peugeot 2008 ALLURE 2017 2018" → 2017/2018).
    """
    nums = re.findall(r"\b(19[5-9]\d|20[0-3]\d)\b", titulo or "")
    if len(nums) >= 2:
        return int(nums[-2]), int(nums[-1])
    if len(nums) == 1:
        return int(nums[-1]), int(nums[-1])
    return None, None


def parse_km(km_str: str) -> int | None:
    if not km_str:
        return None
    s = re.sub(r"[^\d]", "", str(km_str))
    try:
        val = int(s)
        return val if 0 <= val <= 2_000_000 else None
    except Exception:
        return None


def parse_combustivel(titulo: str) -> str | None:
    """
    Extrai combustível do título do Pestana.
    Padrões: "Gas/Alc" = Flex, "Diesel", "Elétrico", "Hibrido"
    """
    t = titulo.lower()
    if "gas/alc" in t or "gas / alc" in t or "flex" in t:
        return "Flex"
    if "diesel" in t:
        return "Diesel"
    if "eletrico" in t or "elétrico" in t or "electric" in t:
        return "Elétrico"
    if "hibrido" in t or "híbrido" in t or "hybrid" in t:
        return "Híbrido"
    if "etanol" in t:
        return "Etanol"
    if "gasolina" in t or "gasol" in t:
        return "Gasolina"
    if "gnv" in t:
        return "GNV"
    return None


def parse_marca_modelo(titulo: str) -> tuple[str | None, str | None]:
    """
    "Renault Logan AUTH 10 2018 2019 Gas/Alc"  → ("RENAULT", "Logan AUTH 10")
    "Peugeot 2008 ALLURE EAT6 2017 2018 Gas/Alc" → ("PEUGEOT", "2008 ALLURE EAT6")

    Remove apenas os anos de fab/mod do FINAL do título (últimos 2 anos consecutivos).
    """
    t = titulo or ""
    # Remove sufixo de combustível
    t = re.sub(r"\s+Gas/Alc.*$", "", t, flags=re.I).strip()
    t = re.sub(r"\s+(Flex|Diesel|Gasolina|Etanol|GNV|Elétrico|Híbrido).*$", "", t, flags=re.I).strip()
    # Remove os dois anos do final  ex: "... 2017 2018" ou "... 2019 2019"
    t = re.sub(r"\s+(19[5-9]\d|20[0-3]\d)\s+(19[5-9]\d|20[0-3]\d)\s*$", "", t).strip()
    # Remove ano solto no final
    t = re.sub(r"\s+(19[5-9]\d|20[0-3]\d)\s*$", "", t).strip()

    parts = t.split(" ", 1)
    marca  = parts[0].strip().upper() if parts else None
    modelo = parts[1].strip() if len(parts) > 1 else None
    return marca, modelo


def parse_data_iso(data_str: str, hora_str: str = "00:00") -> str | None:
    """
    "11/03/2026" + "10:00" → "2026-03-11T10:00:00"
    """
    if not data_str:
        return None
    try:
        dt_str = f"{data_str} {hora_str}"
        dt = datetime.strptime(dt_str, "%d/%m/%Y %H:%M")
        return dt.isoformat()
    except Exception:
        return None


def parse_estado(texto: str) -> str | None:
    """Extrai UF do padrão "Lote\n / RS\n" no texto da página."""
    m = re.search(r"Lote\s*\n\s*/\s*([A-Z]{2})\s*\n", texto)
    if m:
        return m.group(1)
    # Fallback: qualquer "/ XX" onde XX é UF
    m = re.search(r"/\s*([A-Z]{2})\b", texto)
    if m and m.group(1) in {
        "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
        "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
    }:
        return m.group(1)
    return None


def parse_lance(texto: str) -> float | None:
    """
    Extrai o lance do texto da página — funciona tanto para
    "Lance inicial R$ 18.800,00" quanto "Lance atual R$ 26.000,00".
    """
    m = re.search(r"Lance\s+(?:inicial|atual)\s*\n?\s*R\$\s*([\d.,]+)", texto, re.I)
    if m:
        return parse_brl(m.group(1))
    return None


def parse_cidade(localizacao: str) -> tuple[str | None, str | None]:
    """
    "BR 386, S/N - Km 431 - Nova Santa Rita RS"
    → cidade="Nova Santa Rita", estado="RS"
    """
    if not localizacao:
        return None, None
    # Último trecho após "-"
    parts = [p.strip() for p in localizacao.split("-")]
    last = parts[-1] if parts else ""
    # Remove CEP e UF do final
    m = re.match(r"^(.+?)\s+([A-Z]{2})\s*(?:\d{5})?$", last)
    if m:
        return m.group(1).strip(), m.group(2)
    return last or None, None


# ─── Playwright: coleta listagem ──────────────────────────────────────────────

async def coletar_cards_pagina(page) -> list[dict]:
    """Extrai todos os cards de lote de uma página de listagem."""
    try:
        await page.wait_for_selector("a[href*='agenda-de-leiloes']", timeout=20_000)
    except Exception:
        pass

    cards = await page.evaluate("""
        () => {
            const results = [];
            const vistos = new Set();

            const links = [...document.querySelectorAll('a[href]')].filter(a =>
                a.href && a.href.match(/agenda-de-leiloes\\/\\d+\\/\\d+/)
            );

            links.forEach(link => {
                if (vistos.has(link.href)) return;
                vistos.add(link.href);

                // Sobe na DOM até achar o card com preço
                let card = link;
                for (let i = 0; i < 8; i++) {
                    const parent = card.parentElement;
                    if (!parent) break;
                    if (parent.innerText && parent.innerText.match(/R\\$/)) {
                        card = parent;
                        break;
                    }
                    card = parent;
                }

                const texto = card.innerText.trim();
                if (!texto.match(/R\\$/) && !texto.match(/Lance/i)) return;

                const img = card.querySelector('img');
                const tituloEl = card.querySelector('h1,h2,h3,h4,strong');

                results.push({
                    link:   link.href,
                    titulo: tituloEl ? tituloEl.innerText.trim() : texto.split('\\n')[0].trim(),
                    imagem: img ? (img.src || img.getAttribute('data-src') || '') : '',
                    texto:  texto.substring(0, 600),
                });
            });

            return results;
        }
    """)
    return cards or []


# ─── Playwright: detalhe do lote ──────────────────────────────────────────────

async def coletar_detalhe(page, url: str) -> dict:
    """Acessa a página do lote e extrai texto + imagens."""
    try:
        await page.goto(url, wait_until="networkidle", timeout=60_000)
        await asyncio.sleep(1.5)

        raw = await page.evaluate("""
            (url) => {
                const imagens = [...document.querySelectorAll('img')]
                    .map(i => i.src || i.getAttribute('data-src') || '')
                    .filter(src =>
                        src &&
                        src.includes('ged.pestana') &&
                        !src.includes('tarja') &&
                        !src.includes('logo')
                    );

                // Remove duplicatas e thumbs (query string de resize)
                const seen = new Set();
                const imagens_unicas = [];
                for (const src of imagens) {
                    const base = src.split('?')[0];
                    if (!seen.has(base)) {
                        seen.add(base);
                        imagens_unicas.push(base);  // URL limpa sem query string
                    }
                }

                const main = document.querySelector(
                    'main, article, [class*="detalhe"], [class*="lote"], [class*="content"]'
                );
                const texto_pagina = (main || document.body).innerText.trim();

                return { url, imagens: imagens_unicas, texto_pagina };
            }
        """, url)

        return raw

    except Exception as e:
        return {"url": url, "imagens": [], "texto_pagina": "", "erro": str(e)}


# ─── Extração estruturada ─────────────────────────────────────────────────────

def extract(card: dict, detalhe: dict) -> dict | None:
    """
    Combina card da listagem + detalhe da página individual
    em um dict estruturado pronto para enriquecimento FIPE.
    """
    titulo = (card.get("titulo") or "").strip()
    link   = card.get("link", "")
    if not titulo or not link:
        return None

    texto_det = detalhe.get("texto_pagina", "")

    # ── Valores ──────────────────────────────────────────────────────────
    # Tenta pegar da listagem primeiro, senão do detalhe
    lance_raw = parse_brl(card.get("lance_inicial")) or parse_lance(texto_det)

    # ── Datas ─────────────────────────────────────────────────────────────
    data_str = card.get("data_leilao") or _extrair_data_texto(texto_det)
    hora_str = card.get("hora_leilao") or _extrair_hora_texto(texto_det) or "00:00"
    data_leilao = parse_data_iso(data_str, hora_str)

    # ── Localização ───────────────────────────────────────────────────────
    loc_str = _extrair_localizacao(texto_det)
    cidade_det, estado_det = parse_cidade(loc_str)
    estado = parse_estado(texto_det) or estado_det

    # ── Campos do detalhe ─────────────────────────────────────────────────
    km          = parse_km(_campo(texto_det, r"KM[:\s]+([\d.,]+)"))
    cambio      = _campo(texto_det, r"Câmbio[:\s]+([^\n]+)", max_len=20)
    ar_cond     = _campo(texto_det, r"Ar Condicionado[:\s]+([^\n]+)", max_len=10)
    direcao     = _campo(texto_det, r"Dire[çc][aã]o[:\s]+([^\n]+)", max_len=20)
    chaves      = _campo(texto_det, r"Chaves[:\s]+([^\n]+)", max_len=20)
    estepe      = _campo(texto_det, r"Estepe[:\s]+([^\n]+)", max_len=10)
    tipo_bem    = _campo(texto_det, r"Tipo[:\s]+([^\n]+)", max_len=20)
    placa       = _campo(texto_det, r"Placa[:\s]+([^\n]+)", max_len=10)

    # ── Título → campos ───────────────────────────────────────────────────
    ano_fab, ano_mod = parse_ano(titulo)
    marca, modelo    = parse_marca_modelo(titulo)
    combustivel      = parse_combustivel(titulo)

    # ── Imagens ───────────────────────────────────────────────────────────
    imagens = detalhe.get("imagens") or []
    # Normaliza e deduplica imagens (remove query string de resize)
    seen_imgs = set()
    imagens_unicas = []
    for img in imagens:
        base = img.split("?")[0]
        if base not in seen_imgs and "8573860" not in base:  # remove logo Pestana
            seen_imgs.add(base)
            imagens_unicas.append(base)
    imagens = imagens_unicas[:10]

    return {
        # Identificação
        "titulo":           titulo,
        "marca":            marca,
        "modelo":           modelo,
        "link":             link,

        # Ano
        "ano_fab":          ano_fab,
        "ano_mod":          ano_mod,

        # Valores
        "lance_raw":        lance_raw,
        "lance":            fmt_brl(lance_raw),

        # FIPE (preenchido depois)
        "fipe_raw":         None,
        "fipe":             None,
        "fipe_min":         None,
        "fipe_max":         None,
        "fipe_fonte":       None,
        "desconto_pct":     None,
        "margem_bruta":     None,
        "margem_bruta_fmt": None,
        "margem_liquida":   None,
        "margem_liquida_fmt": None,

        # Características
        "km":               km,
        "combustivel":      combustivel,
        "cambio":           cambio,
        "ar_cond":          ar_cond,
        "direcao":          direcao,
        "chaves":           chaves,
        "estepe":           estepe,
        "tipo_bem":         tipo_bem,
        "placa":            placa,

        # Localização
        "estado":           estado,
        "cidade":           cidade_det,
        "localizacao":      loc_str,

        # Leilão
        "data_leilao":      data_leilao,
        "nome_leilao":      _extrair_nome_leilao(texto_det),
        "visitacao":        _extrair_visitacao(texto_det),
        "origem":           "Retomado",

        # Imagens
        "imagens":          imagens,
    }


# ─── Helpers regex ────────────────────────────────────────────────────────────

def _campo(texto: str, padrao: str, max_len: int = 100) -> str | None:
    m = re.search(padrao, texto, re.IGNORECASE)
    if not m:
        return None
    val = m.group(1).strip()
    return val[:max_len] if len(val) <= max_len else None


def _extrair_data_texto(texto: str) -> str | None:
    m = re.search(r"(\d{2}/\d{2}/\d{4})", texto)
    return m.group(1) if m else None


def _extrair_hora_texto(texto: str) -> str | None:
    m = re.search(r"(\d{2}:\d{2})", texto)
    return m.group(1) if m else None


def _extrair_localizacao(texto: str) -> str | None:
    m = re.search(r"Localiza[çc][aã]o\s*\n+(.+)", texto)
    return m.group(1).strip() if m else None


def _extrair_visitacao(texto: str) -> str | None:
    m = re.search(r"Visita[çc][aã]o\s*\n+(.+)", texto)
    return m.group(1).strip() if m else None


def _extrair_nome_leilao(texto: str) -> str | None:
    m = re.search(r"Pertence ao\s*\n+(.+)", texto)
    if m:
        return m.group(1).strip()
    m = re.search(r"(Leil[aã]o de [^\n\-]{3,60}?)(?:\s*-\s*\d{2}/\d{2}|\n)", texto)
    if m:
        return m.group(1).strip()
    return None


# ─── Enriquecimento FIPE ──────────────────────────────────────────────────────

def _titulo_para_query(titulo: str) -> str:
    """Remove separadores do título para busca DDG."""
    s = re.sub(r"\s*[-/]\s*", " ", titulo)
    return re.sub(r"\s+", " ", s).strip()


async def enriquecer_fipe(lotes: list[dict]) -> list[dict]:
    total  = len(lotes)
    ok     = 0
    falhou = 0

    for i, lote in enumerate(lotes, 1):
        titulo = lote.get("titulo", "")
        print(f"  {DIM}[{i}/{total}]{RESET} {titulo[:55]}", end=" ", flush=True)

        r = await buscar_valor_mercado(_titulo_para_query(titulo))

        if r["valor"]:
            lote["fipe_raw"]   = r["valor"]
            lote["fipe"]       = _fmt_ddg(r["valor"])
            lote["fipe_min"]   = r["valor_min"]
            lote["fipe_max"]   = r["valor_max"]
            lote["fipe_fonte"] = r["fonte"]

            lance = lote.get("lance_raw")
            fipe  = r["valor_min"]  # conservador: menor versão do modelo

            if lance and fipe and fipe > 0:
                desc_pct     = round((1 - lance / fipe) * 100, 1)
                margem_bruta = round(fipe - lance, 2)

                CUSTO_REPARO = 1_500 if lote.get("tipo") == "moto" else 5_000
                margem_liq   = round(margem_bruta - CUSTO_REPARO, 2)

                if desc_pct > 0 and margem_liq >= 10_000:
                    lote["desconto_pct"]       = desc_pct
                    lote["margem_bruta"]       = margem_bruta
                    lote["margem_bruta_fmt"]   = _fmt_ddg(margem_bruta)
                    lote["margem_liquida"]     = margem_liq
                    lote["margem_liquida_fmt"] = _fmt_ddg(margem_liq)

            label = (
                f"{GREEN}({lote['desconto_pct']}% desc · liq {lote['margem_liquida_fmt']}){RESET}"
                if lote.get("desconto_pct")
                else f"{YELLOW}sem margem{RESET}"
            )
            print(f"→ fipe_min {lote['fipe']}  {label}")
            ok += 1
        else:
            print(f"→ {RED}não encontrado{RESET}")
            falhou += 1

        await asyncio.sleep(1.2)

    print(f"\n  {GREEN}DDG OK: {ok}{RESET}  ·  {RED}não encontrado: {falhou}{RESET}")
    return lotes


# ─── Playwright: coleta completa ─────────────────────────────────────────────

async def coletar_playwright(headless: bool = True, limit: int = 0) -> tuple[list[dict], list[dict]]:
    """
    Etapa 1: coleta todos os cards + detalhes via Playwright.
    Retorna (cards_brutos, detalhes_brutos).
    Se limit > 0, para após N lotes (útil pra debug).
    """
    cards_todos   = []
    detalhes_dict = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent=BROWSER_UA, locale="pt-BR")
        page    = await context.new_page()

        # ── Listagem ─────────────────────────────────────────────────────
        for pagina in range(1, 50):
            url = LISTAGEM_URL.format(pagina=pagina)
            print(f"  {DIM}página {pagina}:{RESET} {url}")

            await page.goto(url, wait_until="networkidle", timeout=60_000)
            await asyncio.sleep(1.5)

            cards = await coletar_cards_pagina(page)
            if not cards:
                print(f"  {YELLOW}Sem lotes — fim da listagem.{RESET}")
                break

            # Parseia dados básicos do card
            for c in cards:
                texto = c.get("texto", "")
                if not c.get("lance_inicial"):
                    m = re.search(r"Lance\s+(?:inicial|atual)[:\s]+R\$\s*([\d.,]+)", texto, re.I)
                    if m:
                        c["lance_inicial"] = "R$ " + m.group(1)
                if not c.get("data_leilao"):
                    m = re.search(r"(\d{2}/\d{2}/\d{4})", texto)
                    if m:
                        c["data_leilao"] = m.group(1)
                if not c.get("hora_leilao"):
                    m = re.search(r"(\d{2}:\d{2})", texto)
                    if m:
                        c["hora_leilao"] = m.group(1)

            print(f"  {GREEN}✓  {len(cards)} lotes{RESET}")
            cards_todos.extend(cards)

            if limit > 0 and len(cards_todos) >= limit:
                cards_todos = cards_todos[:limit]
                print(f"  {YELLOW}--limit {limit} atingido.{RESET}")
                break

            if len(cards) < 96:
                print(f"  {DIM}Última página detectada.{RESET}")
                break

            await asyncio.sleep(1.0)

        total_cards = len(cards_todos)
        print(f"\n  Total para detalhar: {total_cards}\n")

        # ── Detalhe de cada lote ─────────────────────────────────────────
        for i, card in enumerate(cards_todos, 1):
            link   = card.get("link", "")
            titulo = card.get("titulo", "?")[:55]
            print(f"  [{i:03d}/{total_cards}] {titulo}", end=" ", flush=True)

            if not link:
                print(f"{YELLOW}sem link{RESET}")
                detalhes_dict[i] = {"url": "", "imagens": [], "texto_pagina": ""}
                continue

            det = await coletar_detalhe(page, link)
            detalhes_dict[i] = det
            imgs = len(det.get("imagens", []))
            erro = det.get("erro")
            print(f"{GREEN}✓ {imgs} imgs{RESET}" if not erro else f"{RED}erro: {erro[:60]}{RESET}")
            await asyncio.sleep(0.8)

        await browser.close()

    return cards_todos, detalhes_dict


# ─── Normalização → schema auctions.veiculos ──────────────────────────────────

def normalize_to_db(lote: dict) -> dict | None:
    if not lote.get("link") or not lote.get("titulo"):
        return None
    if not lote.get("ano_fab"):
        return None
    if not lote.get("lance_raw"):
        return None
    if not lote.get("fipe_raw"):
        return None

    imagens = lote.get("imagens") or []

    # Tipo (moto vs carro vs truck) via fipe_client
    _marca_n, _modelo_n, *_ = _parse_titulo(lote.get("titulo", ""))
    _cats = _detectar_categorias(_marca_n or "", _modelo_n or "")
    _tipo = {"motorcycles": "moto", "trucks": "truck"}.get(_cats[0], "carro")

    return {
        "titulo":                 lote["titulo"],
        "descricao":              lote.get("localizacao"),
        "tipo":                   _tipo,
        "marca":                  lote.get("marca"),
        "modelo":                 lote.get("modelo"),
        "estado":                 lote.get("estado"),
        "cidade":                 lote.get("cidade"),
        "ano_fabricacao":         lote.get("ano_fab"),
        "ano_modelo":             lote.get("ano_mod"),
        "modalidade":             "leilao",
        "valor_inicial":          lote["lance_raw"],
        "valor_atual":            lote["lance_raw"],
        "data_encerramento":      lote.get("data_leilao"),
        "link":                   lote["link"],
        "imagem_1":               imagens[0] if len(imagens) > 0 else None,
        "imagem_2":               imagens[1] if len(imagens) > 1 else None,
        "imagem_3":               imagens[2] if len(imagens) > 2 else None,
        "percentual_abaixo_fipe": lote.get("desconto_pct"),
        "margem_revenda":         lote.get("margem_liquida"),
        "km":                     lote.get("km"),
        "origem":                 "Retomado",
        "ativo":                  True,
    }


# ─── Print ────────────────────────────────────────────────────────────────────

def print_lote(lote: dict, i: int, total: int):
    titulo  = lote["titulo"][:58]
    km_str  = f"{lote['km']:,} km".replace(",", ".") if lote["km"] else "km ?"
    ano_fab = lote.get("ano_fab")
    ano_mod = lote.get("ano_mod")
    ano_str = (
        f"{ano_fab}/{ano_mod}" if ano_fab != ano_mod
        else str(ano_fab or "?")
    )
    desc_str = (
        f"  {GREEN}{lote['desconto_pct']}% abaixo FIPE{RESET}"
        if lote.get("desconto_pct") else ""
    )

    print(f"\n{'─'*68}")
    print(f"{BOLD}{YELLOW}[{i}/{total}] {titulo}{RESET}")
    print(f"{'─'*68}")
    print(f"  {DIM}marca:{RESET}        {lote['marca']}  ·  {lote['modelo']}")
    print(f"  {DIM}ano:{RESET}          {ano_str}  ·  {km_str}  ·  {lote['combustivel'] or '?'}")
    print(f"  {DIM}local:{RESET}        {lote.get('cidade') or '?'} / {lote.get('estado') or '?'}")
    print(f"  {DIM}câmbio:{RESET}       {lote.get('cambio') or '?'}  ·  ar={lote.get('ar_cond') or '?'}  ·  chaves={lote.get('chaves') or '?'}")
    print(f"  {DIM}lance:{RESET}        {lote['lance']}{desc_str}")
    print(f"  {DIM}fipe:{RESET}         {lote['fipe'] or '—'}  [{lote['fipe_fonte'] or '—'}]")
    print(f"  {DIM}margem líquida:{RESET} {lote.get('margem_liquida_fmt') or '—'}  "
          f"(bruta {lote.get('margem_bruta_fmt') or '—'} - R$5.000 reparo)")
    print(f"  {DIM}data:{RESET}         {lote['data_leilao']}")
    print(f"  {DIM}imagens:{RESET}      {len(lote['imagens'])}x")
    print(f"  {DIM}link:{RESET}         {lote['link']}")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Pestana Leilões → auctions.veiculos")
    parser.add_argument("--no-upload",   action="store_true", help="Não sobe pro Supabase (debug local)")
    parser.add_argument("--no-fipe",     action="store_true", help="Pula busca DDG (mais rápido, sem margem)")
    parser.add_argument("--show-browser", action="store_true", help="Abre browser visível (debug)")
    parser.add_argument("--limit",       type=int, default=0,  help="Limita a N lotes (debug, ex: --limit 1)")
    parser.add_argument("--output",      default="pestana_coleta.json")
    args = parser.parse_args()

    headless = not args.show_browser

    print(f"\n{BOLD}{'='*68}{RESET}")
    print(f"{BOLD}  🏠  PESTANA LEILÕES — COLETA COMPLETA{RESET}")
    print(f"{BOLD}{'='*68}{RESET}")
    print(f"  {DIM}URL base: {LISTAGEM_URL.format(pagina=1)}{RESET}")
    print(f"  {DIM}upload:   {'nao (debug)' if args.no_upload else 'sim → auctions.veiculos'}{RESET}\n")

    # ── 1. Coleta Playwright ─────────────────────────────────────────────
    print(f"{BOLD}  🌐  Coletando com Playwright...{RESET}\n")
    cards_brutos, detalhes_dict = await coletar_playwright(headless=headless, limit=args.limit)

    # ── 2. Extração estruturada ──────────────────────────────────────────
    print(f"\n{BOLD}  🔧  Extraindo campos...{RESET}\n")
    lotes  = []
    falhos = []
    for i, card in enumerate(cards_brutos, 1):
        detalhe = detalhes_dict.get(i, {})
        lote = extract(card, detalhe)
        if lote:
            lotes.append(lote)
        else:
            falhos.append(card.get("titulo", "?"))

    print(f"  {GREEN}✓  {len(lotes)} extraídos{RESET}  ·  {RED}{len(falhos)} falhos{RESET}")

    # ── 3. FIPE via DDG ──────────────────────────────────────────────────
    if not args.no_fipe:
        print(f"\n{BOLD}  🔍  Buscando FIPE via DDG ({len(lotes)} lotes)...{RESET}\n")
        lotes = await enriquecer_fipe(lotes)

    # ── 4. Ordena por margem ─────────────────────────────────────────────
    lotes.sort(key=lambda x: x.get("margem_liquida") or 0, reverse=True)

    # ── 5. Print ─────────────────────────────────────────────────────────
    for i, lote in enumerate(lotes, 1):
        print_lote(lote, i, len(lotes))

    com_fipe   = sum(1 for l in lotes if l.get("fipe_raw"))
    com_margem = sum(1 for l in lotes if l.get("margem_liquida"))
    com_imagem = sum(1 for l in lotes if l.get("imagens"))
    com_lance  = sum(1 for l in lotes if l.get("lance_raw"))

    print(f"\n\n{'='*68}")
    print(f"{BOLD}  📊  RESUMO{RESET}")
    print(f"{'='*68}")
    print(f"  Total coletados:  {len(lotes)}")
    print(f"  Com lance:        {com_lance}")
    print(f"  Com FIPE DDG:     {com_fipe}")
    print(f"  Com margem:       {com_margem}")
    print(f"  Com imagem:       {com_imagem}")
    if com_margem:
        top = [l for l in lotes if l.get("margem_liquida")]
        print(f"  Melhor margem:    {top[0]['margem_liquida_fmt']}  ({top[0]['titulo'][:45]})")
        print(f"  Maior desconto:   {max(l['desconto_pct'] for l in top if l.get('desconto_pct'))}%")

    # ── 6. Salva JSON ────────────────────────────────────────────────────
    output_data = {
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "total_lotes":  len(lotes),
        "com_fipe":     com_fipe,
        "com_margem":   com_margem,
        "lotes":        lotes,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"\n  JSON salvo em: {args.output}")

    # ── 7. Upload Supabase ───────────────────────────────────────────────
    if not args.no_upload:
        from supabase_client import SupabaseClient

        registros = [normalize_to_db(l) for l in lotes]
        registros = [r for r in registros if r]

        print(f"\n{BOLD}{'='*68}{RESET}")
        print(f"{BOLD}  ☁️   UPLOAD → auctions.veiculos  ({len(registros)} registros){RESET}")
        print(f"{BOLD}{'='*68}{RESET}\n")

        db    = SupabaseClient()
        stats = db.upsert_veiculos(registros)
        total_s = stats.get("inserted", 0) + stats.get("updated", 0)
        print(f"\n  ✅  Enviados:        {total_s}  "
              f"({stats.get('inserted',0)} novos + {stats.get('updated',0)} atualizados)")
        print(f"  🔄  Dupes removidas: {stats.get('duplicates_removed', 0)}")
        print(f"  ❌  Erros:           {stats.get('errors', 0)}\n")


if __name__ == "__main__":
    asyncio.run(main())