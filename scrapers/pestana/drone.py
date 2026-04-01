#!/usr/bin/env python3
"""
drone.py — Scraper Pestana Leilões (busca "drone") → auctions.tecnologia

Coleta drones via Playwright (site renderizado por JS),
enriquece com preço de mercado via Mercado Livre e sobe pro Supabase.

Regra premium: percentual_abaixo_mercado >= 40 → premium = True

Uso local (debug):
    python drone.py --no-upload
    python drone.py --no-upload --output drone_debug.json
    python drone.py --no-upload --no-market          # pula ML, mais rápido
    python drone.py --show-browser                   # browser visível
    python drone.py --limit 5                        # testa com 5 lotes

GitHub Actions (produção):
    python drone.py --output /tmp/drone_coleta.json

Dependências:
    pip install playwright httpx
    playwright install chromium
"""

import asyncio
import json
import re
import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from supabase_client import SupabaseClient


# ─── Cores ────────────────────────────────────────────────────────────────────

CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

# ─── Config ───────────────────────────────────────────────────────────────────

BASE_URL   = "https://www.pestanaleiloes.com.br"
SEARCH_URL = (
    BASE_URL
    + "/procurar-bens?texto=drone"
    + "&lotePage={pagina}&loteQty=96"
)

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

PREMIUM_DESCONTO_MIN = 40.0   # % abaixo do mercado para marcar como premium


# ─── Detecção de Drone ────────────────────────────────────────────────────────

# Exclui falsos positivos (itens que podem conter a palavra "drone" mas não são)
DRONE_EXCLUDE = re.compile(
    r"""
      \bcamera\s+de\s+seguranca\b
    | \bcftv\b
    | \bchapeu\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Confirma que é um drone de fato
DRONE_CONFIRM = re.compile(
    r"""
      \bdrone\b
    | \bdji\b
    | \bphantom\s*\d
    | \bmavic\b
    | \bmini\s*\d
    | \bair\s*\d
    | \bpro\s*\d          # Phantom Pro 4, etc
    | \bmatrice\b
    | \bfpv\b
    | \bautel\b
    | \bevo\s*\d
    | \bparrot\b
    | \banafi\b
    | \bskydio\b
    | \bwingtra\b
    | \bquadricoptero\b
    | \bquadricóptero\b
    | \bmultirotor\b
    | \bhexacoptero\b
    | \bvant\b              # Veículo Aéreo Não Tripulado
    | \buav\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


# Detecta lotes de sucata/peças soltas — exclui da coleta
SUCATA_PATTERNS = re.compile(
    r"""
      \bsucata[s]?\b
    | \bpe[cç]as?\s+adul[st]as?\b
    | \bpe[cç]as?\s+soltas?\b
    | \bpara\s+retirada\b
    | \blote\s+contendo\s+sucata
    | \bcontrole[s]?\s+avul[st]o[s]?\b
    | \bpropulsor[es]?\s+avul[st]o[s]?\b
    | \bsobressalente[s]?\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


def is_sucata(titulo: str) -> bool:
    return bool(SUCATA_PATTERNS.search(titulo))


def is_drone(titulo: str) -> bool:
    if DRONE_EXCLUDE.search(titulo):
        return False
    if is_sucata(titulo):
        return False
    return bool(DRONE_CONFIRM.search(titulo))


# Detecta lotes bundle/kit — drone + acessórios incluídos
BUNDLE_PATTERNS = re.compile(
    r"""
      \bacompanha\b           # "acompanha 3 baterias"
    | \binclui\b              # "inclui controle e carregador"
    | \bcom\s+\d+\s+bateria   # "com 2 baterias"
    | \bkit\b                 # "kit drone"
    | \bcombo\b               # "combo DJI"
    | \bc/\s*controle\b       # "c/ controle"
    | \bc/\s*\d+\s*bat        # "c/ 3 bat"
    | \bconjunto\b            # "conjunto completo"
    | \bcompleto\b            # "drone completo com"
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Extrai lista de extras do título ("3 baterias", "controle", "carregador", etc.)
_EXTRAS_RE = re.compile(
    r"""
      (\d+)\s*baterias?
    | \bcontrole[s]?\b
    | \bcarregador[es]?\b
    | \bgoggle[s]?\b
    | \b\d+\s*gb\b            # cartão de memória
    | \boc[uó]lo[s]?\b
    | \bcase\b
    | \bmaleta\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


def is_bundle(titulo: str) -> bool:
    return bool(BUNDLE_PATTERNS.search(titulo))


def extrair_extras_bundle(titulo: str) -> list[str]:
    """Retorna lista legível dos extras detectados no título."""
    extras = []
    for m in _EXTRAS_RE.finditer(titulo):
        extras.append(m.group(0).strip().lower())
    return extras


# ─── Extração de marca e modelo ───────────────────────────────────────────────

def extrair_marca_drone(titulo: str) -> str | None:
    t = titulo.upper()
    marcas = [
        ("DJI",        r"\bDJI\b"),
        ("Autel",      r"\bAUTEL\b"),
        ("Parrot",     r"\bPARROT\b"),
        ("Skydio",     r"\bSKYDIO\b"),
        ("Wingtra",    r"\bWINGTRA\b"),
        ("Yuneec",     r"\bYUNEEC\b"),
        ("Hubsan",     r"\bHUBSAN\b"),
        ("Holy Stone", r"\bHOLY\s*STONE\b"),
        ("SYMA",       r"\bSYMA\b"),
    ]
    for nome, pat in marcas:
        if re.search(pat, t):
            return nome
    return None


def extrair_modelo_drone(titulo: str) -> str:
    """
    Extrai o modelo do drone do título.
    Ex: "Drone DJI Mavic Air 2 c/ controle ..."  →  "Mavic Air 2"
        "Drone DJI Phantom 4 Pro V2.0"           →  "Phantom 4 Pro"
        "Drone DJI Mini 3 Pro"                   →  "Mini 3 Pro"
    """
    t = titulo

    # DJI: Mavic, Phantom, Mini, Air, FPV, Matrice, Agras, Avata
    m = re.search(
        r"\b(Mavic\s+(?:Air|Pro|Mini|Enterprise|3|2|Classic|Sport)?\s*[\d.]*\s*(?:Pro|Cine|Zoom|Thermal)?)",
        t, re.I
    )
    if m:
        return m.group(1).strip()

    m = re.search(r"\b(Phantom\s+\d+\s*(?:Pro|V2\.0|Advanced|RTK)?)", t, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r"\b(Mini\s*\d+\s*(?:Pro|SE|Cine)?)", t, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r"\b(Air\s*\d+\s*(?:Pro|S|Combo)?)", t, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r"\b(FPV\s*(?:Combo|Explorer|Goggles)?)", t, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r"\b(Matrice\s*[\d]+(?:\s*Pro|RTK)?)", t, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r"\b(Avata\s*\d*\s*(?:Pro|Explorer)?)", t, re.I)
    if m:
        return m.group(1).strip()

    # Autel: EVO
    m = re.search(r"\b(EVO\s*(?:Lite|Nano|II|2)?\s*\+?\s*(?:Pro|V2|Enterprise)?)", t, re.I)
    if m:
        return m.group(1).strip()

    # Parrot: Anafi
    m = re.search(r"\b(Anafi\s*(?:Ai|USA|Thermal|Work|FPV)?)", t, re.I)
    if m:
        return m.group(1).strip()

    # Fallback: remove marca + palavra "drone" e pega as 4 primeiras palavras restantes
    clean = re.sub(r"\b(drone|dji|autel|parrot|skydio)\b", "", t, flags=re.I)
    clean = re.sub(r"\s+", " ", clean).strip()
    palavras = clean.split()
    return " ".join(palavras[:4]) if palavras else titulo[:40]


def query_busca_drone(titulo: str, bundle: bool = False) -> str:
    """
    Monta query de busca para o Mercado Livre.
    Se for bundle, adiciona "combo" — termo usado no ML para kits DJI.
    Ex: bundle DJI Avata 2 c/ 3 baterias  →  "drone DJI Avata 2 combo"
        DJI Mavic Air 2 unitário          →  "drone DJI Mavic Air 2"
    """
    marca  = extrair_marca_drone(titulo)
    modelo = extrair_modelo_drone(titulo)
    base = (
        f"drone {marca} {modelo}"
        if marca and marca.upper() not in modelo.upper()
        else f"drone {modelo}"
    )
    return f"{base} combo" if bundle else base


# ─── Helpers monetários ───────────────────────────────────────────────────────

def parse_brl(v) -> float | None:
    if v is None:
        return None
    s = str(v).replace("R$", "").replace("\xa0", "").replace(" ", "").strip()
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        val = float(s)
        return val if 50 <= val <= 500_000 else None
    except Exception:
        return None


def fmt_brl(v) -> str:
    val = parse_brl(v)
    if val is None:
        return "—"
    s = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def pct_desconto(lance: float | None, mercado: float | None) -> float | None:
    if lance and mercado and mercado > 0:
        return round((1 - lance / mercado) * 100, 1)
    return None


# ─── Helpers de parse ─────────────────────────────────────────────────────────

def parse_brl_raw(v) -> float | None:
    """Alias claro para parse_brl."""
    return parse_brl(v)


def parse_lance_texto(texto: str) -> float | None:
    m = re.search(r"Lance\s+(?:inicial|atual)\s*\n?\s*R\$\s*([\d.,]+)", texto, re.I)
    if m:
        return parse_brl(m.group(1))
    return None


def parse_data_iso(data_str: str, hora_str: str = "00:00") -> str | None:
    if not data_str:
        return None
    try:
        dt = datetime.strptime(f"{data_str} {hora_str}", "%d/%m/%Y %H:%M")
        return dt.isoformat()
    except Exception:
        return None


def parse_km(s: str) -> int | None:
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", str(s))
    try:
        v = int(digits)
        return v if 0 <= v <= 2_000_000 else None
    except Exception:
        return None


def parse_estado(texto: str) -> str | None:
    UFS = {
        "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
        "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
    }
    m = re.search(r"Lote\s*\n\s*/\s*([A-Z]{2})\s*\n", texto)
    if m and m.group(1) in UFS:
        return m.group(1)
    m = re.search(r"/\s*([A-Z]{2})\b", texto)
    if m and m.group(1) in UFS:
        return m.group(1)
    return None


def parse_cidade(localizacao: str | None) -> tuple[str | None, str | None]:
    if not localizacao:
        return None, None
    parts = [p.strip() for p in localizacao.split("-")]
    last  = parts[-1] if parts else ""
    m = re.match(r"^(.+?)\s+([A-Z]{2})\s*(?:\d{5})?$", last)
    if m:
        return m.group(1).strip(), m.group(2)
    return last or None, None


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


def _extrair_nome_leilao(texto: str) -> str | None:
    m = re.search(r"Pertence ao\s*\n+(.+)", texto)
    if m:
        return m.group(1).strip()
    m = re.search(r"(Leil[aã]o de [^\n\-]{3,60}?)(?:\s*-\s*\d{2}/\d{2}|\n)", texto)
    return m.group(1).strip() if m else None


# ─── Playwright: coleta listagem ──────────────────────────────────────────────

async def coletar_cards_pagina(page) -> list[dict]:
    """Extrai todos os cards de lote de uma página de busca."""
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

                const img      = card.querySelector('img');
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

                const seen = new Set();
                const imagens_unicas = [];
                for (const src of imagens) {
                    const base = src.split('?')[0];
                    if (!seen.has(base)) {
                        seen.add(base);
                        imagens_unicas.push(base);
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
    titulo = (card.get("titulo") or "").strip()
    link   = card.get("link", "")
    if not titulo or not link:
        return None

    # Filtra apenas drones
    if not is_drone(titulo):
        return None

    texto_det = detalhe.get("texto_pagina", "")

    # ── Valores ──────────────────────────────────────────────────────────
    lance_raw = parse_brl(card.get("lance_inicial")) or parse_lance_texto(texto_det)

    # ── Datas ─────────────────────────────────────────────────────────────
    data_str    = card.get("data_leilao") or _extrair_data_texto(texto_det)
    hora_str    = card.get("hora_leilao") or _extrair_hora_texto(texto_det) or "00:00"
    data_leilao = parse_data_iso(data_str, hora_str)

    # ── Localização ───────────────────────────────────────────────────────
    loc_str    = _extrair_localizacao(texto_det)
    cidade_det, estado_det = parse_cidade(loc_str)
    estado     = parse_estado(texto_det) or estado_det

    # ── Imagens ───────────────────────────────────────────────────────────
    imagens = detalhe.get("imagens") or []
    seen_imgs, imagens_unicas = set(), []
    for img in imagens:
        base = img.split("?")[0]
        if base not in seen_imgs and "8573860" not in base:  # remove logo Pestana
            seen_imgs.add(base)
            imagens_unicas.append(base)
    imagens = imagens_unicas[:10]

    # ── Campos adicionais do detalhe ──────────────────────────────────────
    placa    = _campo(texto_det, r"Placa[:\s]+([^\n]+)",     max_len=10)
    tipo_bem = _campo(texto_det, r"Tipo[:\s]+([^\n]+)",      max_len=30)
    serie    = _campo(texto_det, r"N[uú]mero\s+de\s+[Ss][eé]rie[:\s]+([^\n]+)", max_len=40)

    # ── Drone: marca, modelo e flags ─────────────────────────────────────
    marca  = extrair_marca_drone(titulo)
    modelo = extrair_modelo_drone(titulo)
    bundle = is_bundle(titulo)
    extras = extrair_extras_bundle(titulo) if bundle else []

    # Bundle → busca "drone X kit" no ML para pegar preço de combo
    # Unitário → busca só "drone X modelo"
    query_ml = query_busca_drone(titulo, bundle=bundle)

    return {
        # Identificação
        "titulo":           titulo,
        "marca":            marca,
        "modelo":           modelo,
        "link":             link,
        "query_ml":         query_ml,

        # Bundle (kit com acessórios incluídos)
        "bundle":           bundle,
        "bundle_extras":    extras,  # ["3 baterias", "controle", ...]

        # Valores
        "lance_raw":        lance_raw,
        "lance":            fmt_brl(lance_raw),

        # Mercado Livre (preenchido depois)
        "preco_mercado":         None,
        "preco_mercado_min":     None,
        "preco_mercado_max":     None,
        "preco_mercado_fmt":     "—",
        "desconto_pct":          None,
        "desconto_label":        "—",
        "acima_mercado":         False,
        "margem_bruta":          None,
        "margem_bruta_fmt":      "—",

        # Características
        "serie":            serie,
        "placa":            placa,
        "tipo_bem":         tipo_bem,

        # Localização
        "estado":           estado,
        "cidade":           cidade_det,
        "localizacao":      loc_str,

        # Leilão
        "data_leilao":      data_leilao,
        "nome_leilao":      _extrair_nome_leilao(texto_det),
        "origem":           "Pestana",

        # Imagens
        "imagens":          imagens,

        # Flags (preenchidas depois)
        "is_premium":       False,
    }


# ─── Mercado Livre: busca de preço via Playwright ────────────────────────────

async def _scrape_ml_precos(query: str, debug: bool = False) -> list[float]:
    url = (
        "https://lista.mercadolivre.com.br/"
        + query.replace(" ", "-")
        + "_OrderId_PRICE_NoIndex_True"
    )
    precos: list[float] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="pt-BR",
            viewport={"width": 1280, "height": 900},
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )
        page = await ctx.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(2)

            seletores = [
                "span.andes-money-amount__fraction",
                ".price-tag-fraction",
                "[class*='price'] [class*='fraction']",
            ]
            textos: list[str] = []
            for sel in seletores:
                elementos = await page.query_selector_all(sel)
                if elementos:
                    for el in elementos:
                        textos.append(await el.inner_text())
                    if debug:
                        print(f"  {DIM}[ML] seletor={sel!r} → {len(textos)} valores{RESET}")
                    break

            if not textos:
                html = await page.content()
                textos = re.findall(r'"price"\s*:\s*(\d+(?:\.\d+)?)', html)
                if debug:
                    print(f"  {DIM}[ML] fallback JSON-in-HTML → {len(textos)} matches{RESET}")

        except Exception as e:
            if debug:
                print(f"  {RED}[ML] erro playwright: {e}{RESET}")
            textos = []
        finally:
            await browser.close()

    for t in textos:
        s = str(t).replace(".", "").replace(",", "").strip()
        try:
            v = float(s)
            # Drones: R$ 800 (entry level) até R$ 80.000 (Matrice profissional)
            if 800 <= v <= 80_000:
                precos.append(v)
        except ValueError:
            pass

    return precos


async def buscar_preco_mercadolivre(query: str, debug: bool = False) -> dict:
    url_busca = (
        "https://lista.mercadolivre.com.br/"
        + query.replace(" ", "-")
        + "_OrderId_PRICE_NoIndex_True"
    )

    todos = await _scrape_ml_precos(query, debug=debug)

    if not todos:
        return {
            "preco_medio":    None,
            "preco_min":      None,
            "preco_max":      None,
            "num_resultados": 0,
            "fonte":          "mercadolivre_scrape",
            "query":          query,
            "url_busca":      url_busca,
        }

    todos.sort()
    mediana   = todos[len(todos) // 2]
    filtrados = [p for p in todos if mediana * 0.25 <= p <= mediana * 3.5]
    if not filtrados:
        filtrados = todos

    preco_medio = round(sum(filtrados) / len(filtrados), 2)
    return {
        "preco_medio":    preco_medio,
        "preco_min":      min(filtrados),
        "preco_max":      max(filtrados),
        "num_resultados": len(filtrados),
        "fonte":          "mercadolivre_scrape",
        "query":          query,
        "url_busca":      url_busca,
    }


# ─── Enriquecimento de preços ─────────────────────────────────────────────────

async def enriquecer_precos(lotes: list[dict], debug: bool = False) -> list[dict]:
    """
    Busca preço de mercado no ML para cada drone.

    Bundle → query inclui "kit" → ML retorna preços de combo com acessórios.
    Unitário → query só com modelo → ML retorna preço do drone sozinho.

    Nunca exibe desconto negativo: usa "X% ACIMA do mercado" nesses casos.
    Cache por query para não repetir buscas do mesmo modelo.
    """
    cache: dict[str, dict] = {}
    total = len(lotes)
    ok = falhou = 0

    for i, lote in enumerate(lotes, 1):
        titulo = lote.get("titulo", "")[:55]
        query  = lote.get("query_ml", "")
        bundle = lote.get("bundle", False)
        extras = lote.get("bundle_extras") or []

        bundle_tag = f"  {CYAN}[kit: {', '.join(extras)}]{RESET}" if bundle else ""
        print(f"  {DIM}[{i}/{total}]{RESET} {titulo}{bundle_tag}", end=" ", flush=True)

        if not query:
            print(f"{YELLOW}sem query{RESET}")
            continue

        if query in cache:
            mercado = cache[query]
            print(f"{DIM}(cache){RESET}")
        else:
            mercado = await buscar_preco_mercadolivre(query, debug=debug)
            cache[query] = mercado
            pm_log = mercado.get("preco_medio")
            if pm_log:
                kit_tag = "  (preço combo)" if bundle else ""
                print(f"→ {GREEN}{fmt_brl(pm_log)}{RESET}  ({mercado['num_resultados']} preços){kit_tag}")
                ok += 1
            else:
                print(f"→ {YELLOW}sem preço{RESET}")
                falhou += 1
            await asyncio.sleep(0.5)

        pm     = mercado.get("preco_medio")
        pm_min = mercado.get("preco_min")
        pm_max = mercado.get("preco_max")
        lance  = lote.get("lance_raw")

        lote["preco_mercado"]     = pm
        lote["preco_mercado_min"] = pm_min
        lote["preco_mercado_max"] = pm_max
        lote["preco_mercado_fmt"] = fmt_brl(pm)
        lote["preco_mercado_url"] = mercado.get("url_busca")

        if pm and lance:
            diff_pct = round((1 - lance / pm) * 100, 1)

            if diff_pct > 0:
                lote["desconto_pct"]   = diff_pct
                lote["acima_mercado"]  = False
                ref = " (vs. combo ML)" if bundle else ""
                lote["desconto_label"] = f"{diff_pct:.1f}% abaixo do mercado{ref}"
                margem = round(pm - lance, 2)
                lote["margem_bruta"]     = margem
                lote["margem_bruta_fmt"] = fmt_brl(margem)

            else:
                pct_acima = round(abs(diff_pct), 1)
                lote["desconto_pct"]  = None
                lote["acima_mercado"] = True
                lote["pct_acima"]     = pct_acima
                ref = " (vs. combo ML)" if bundle else ""
                lote["desconto_label"] = f"⚠ {pct_acima:.1f}% ACIMA do mercado{ref}"
                lote["margem_bruta"]     = round(pm - lance, 2)
                lote["margem_bruta_fmt"] = fmt_brl(pm - lance)

        else:
            lote["desconto_pct"]   = None
            lote["acima_mercado"]  = False
            lote["desconto_label"] = "—"

        lote["is_premium"] = bool(
            lote.get("desconto_pct") is not None
            and lote["desconto_pct"] >= PREMIUM_DESCONTO_MIN
        )

    print(f"\n  {GREEN}ML OK: {ok}{RESET}  ·  {YELLOW}sem preço: {falhou}{RESET}")
    return lotes


# ─── Playwright: coleta completa ─────────────────────────────────────────────

async def coletar_playwright(headless: bool = True, limit: int = 0) -> tuple[list[dict], dict]:
    cards_todos:   list[dict] = []
    detalhes_dict: dict       = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent=BROWSER_UA, locale="pt-BR")
        page    = await context.new_page()

        # ── Paginação da busca ────────────────────────────────────────────
        for pagina in range(1, 50):
            url = SEARCH_URL.format(pagina=pagina)
            print(f"  {DIM}página {pagina}:{RESET} {url}")

            await page.goto(url, wait_until="networkidle", timeout=60_000)
            await asyncio.sleep(1.5)

            cards = await coletar_cards_pagina(page)
            if not cards:
                print(f"  {YELLOW}Sem lotes — fim da busca.{RESET}")
                break

            # Parse básico dos campos do card
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

            # Filtra já na listagem para não acessar páginas de detalhe desnecessárias
            cards_drones = [c for c in cards if is_drone(c.get("titulo", ""))]
            print(f"  {GREEN}✓  {len(cards)} lotes na página  ·  {len(cards_drones)} drones{RESET}")
            cards_todos.extend(cards_drones)

            if limit > 0 and len(cards_todos) >= limit:
                cards_todos = cards_todos[:limit]
                print(f"  {YELLOW}--limit {limit} atingido.{RESET}")
                break

            if len(cards) < 96:
                print(f"  {DIM}Última página detectada.{RESET}")
                break

            await asyncio.sleep(1.0)

        total_cards = len(cards_todos)
        print(f"\n  Drones para detalhar: {total_cards}\n")

        # ── Detalhe de cada lote ─────────────────────────────────────────
        for i, card in enumerate(cards_todos, 1):
            link   = card.get("link", "")
            titulo = card.get("titulo", "?")[:55]
            print(f"  [{i:03d}/{total_cards}] {titulo}", end=" ", flush=True)

            if not link:
                print(f"{YELLOW}sem link{RESET}")
                detalhes_dict[i] = {"url": "", "imagens": [], "texto_pagina": ""}
                continue

            det  = await coletar_detalhe(page, link)
            detalhes_dict[i] = det
            imgs = len(det.get("imagens", []))
            erro = det.get("erro")
            print(f"{GREEN}✓ {imgs} imgs{RESET}" if not erro else f"{RED}erro: {erro[:60]}{RESET}")
            await asyncio.sleep(0.8)

        await browser.close()

    return cards_todos, detalhes_dict


# ─── Normalização → schema auctions.tecnologia ───────────────────────────────

def normalize_to_db(lote: dict) -> dict | None:
    if not lote.get("link") or not lote.get("titulo"):
        return None
    if not lote.get("lance_raw"):
        return None

    imagens  = lote.get("imagens") or []
    lance    = lote.get("lance_raw")
    acima    = lote.get("acima_mercado", False)
    bundle   = lote.get("bundle", False)
    extras   = lote.get("bundle_extras") or []
    pm       = lote.get("preco_mercado")
    pm_min   = lote.get("preco_mercado_min")
    pm_max   = lote.get("preco_mercado_max")
    desc     = lote.get("desconto_pct")
    margem   = lote.get("margem_bruta")

    data_enc = lote.get("data_leilao") or (
        datetime.now(timezone.utc) + timedelta(days=30)
    ).isoformat()

    especificacoes: dict = {
        "modelo_completo": lote["titulo"],
        "query_mercado_livre": lote.get("query_ml"),
    }
    if bundle:
        especificacoes["bundle"]        = True
        especificacoes["bundle_extras"] = extras
        especificacoes["preco_ml_ref"]  = "kit (com acessórios)"
    if lote.get("serie"):
        especificacoes["numero_serie"] = lote["serie"]
    if lote.get("preco_mercado_url"):
        especificacoes["url_busca_mercado"] = lote["preco_mercado_url"]
    if acima and lote.get("pct_acima"):
        especificacoes["pct_acima_mercado"] = lote["pct_acima"]

    tags: list[str] = []
    if desc is not None:
        if desc >= 60:
            tags.append("super_oferta")
        elif desc >= 40:
            tags.append("boa_oferta")
        elif desc >= 20:
            tags.append("oportunidade")
    if acima:
        tags.append("acima_mercado_kit" if bundle else "acima_mercado")
    if bundle:
        tags.append("kit")
    if margem and margem >= 500:
        tags.append("margem_alta")

    is_premium = bool(desc is not None and desc >= PREMIUM_DESCONTO_MIN)

    return {
        "titulo":                    lote["titulo"],
        "descricao":                 lote.get("localizacao"),
        "tipo":                      "drone",
        "sub_categoria":             "drone_kit" if bundle else "drone",
        "marca":                     lote.get("marca"),
        "modelo":                    lote.get("modelo"),
        "especificacoes":            especificacoes,
        "estado":                    lote.get("estado"),
        "cidade":                    lote.get("cidade"),
        "modalidade":                "leilao",
        "valor_inicial":             lance,
        "valor_atual":               lance,
        "data_encerramento":         data_enc,
        "link":                      lote["link"],
        "imagem_1":                  imagens[0] if len(imagens) > 0 else None,
        "imagem_2":                  imagens[1] if len(imagens) > 1 else None,
        "imagem_3":                  imagens[2] if len(imagens) > 2 else None,
        "preco_mercado":             pm,
        "preco_mercado_min":         pm_min,
        "preco_mercado_max":         pm_max,
        "percentual_abaixo_mercado": desc if (desc and desc > 0) else None,
        "margem_revenda":            margem,
        "alta_procura":              bool(desc and desc >= 30),
        "tags_oportunidade":         tags,
        "destaque":                  bool(desc and desc >= 50),
        "ativo":                     True,
        "origem":                    "Pestana",
        "premium":                   is_premium,
    }


# ─── Upload para Supabase ────────────────────────────────────────────────────

def upload_to_supabase(lotes: list[dict]) -> dict:
    try:
        db = SupabaseClient()
    except Exception as e:
        print(f"\n  {RED}❌  Falha ao inicializar SupabaseClient: {e}{RESET}")
        return {"inserted": 0, "updated": 0, "errors": len(lotes), "duplicates_removed": 0}

    registros, skipped = [], 0
    for lote in lotes:
        rec = normalize_to_db(lote)
        if rec:
            registros.append(rec)
        else:
            skipped += 1

    if skipped:
        print(f"  {YELLOW}⚠️  {skipped} lote(s) ignorado(s) — sem link/lance{RESET}")
    if not registros:
        print(f"  {RED}Nenhum registro válido para upload.{RESET}")
        return {}

    premium_count = sum(1 for r in registros if r.get("premium"))

    print(f"\n{BOLD}{'='*68}{RESET}")
    print(f"{BOLD}  ☁️   UPLOAD → auctions.tecnologia  ({len(registros)} registros){RESET}")
    print(f"{BOLD}{'='*68}{RESET}")
    if premium_count:
        print(f"  {CYAN}★   Premium (≥{PREMIUM_DESCONTO_MIN:.0f}% abaixo mercado): {premium_count}{RESET}\n")

    try:
        stats   = db.upsert("tecnologia", registros)
        total_s = stats.get("inserted", 0) + stats.get("updated", 0)
        print(f"\n  ✅  Enviados:        {total_s}  "
              f"({stats.get('inserted', 0)} novos + {stats.get('updated', 0)} atualizados)")
        print(f"  🔄  Dupes removidas: {stats.get('duplicates_removed', 0)}")
        print(f"  ❌  Erros:           {stats.get('errors', 0)}\n")
        return stats
    except Exception as e:
        print(f"\n  {RED}❌  Erro no upsert: {e}{RESET}\n")
        return {"inserted": 0, "updated": 0, "errors": len(registros), "duplicates_removed": 0}


# ─── Print ────────────────────────────────────────────────────────────────────

def print_lote(lote: dict, i: int, total: int):
    titulo      = lote["titulo"][:60]
    desc        = lote.get("desconto_pct")
    margem      = lote.get("margem_bruta")
    acima       = lote.get("acima_mercado", False)
    bundle      = lote.get("bundle", False)
    extras      = lote.get("bundle_extras") or []
    premium_str = f"  {CYAN}{BOLD}★ PREMIUM{RESET}" if lote.get("is_premium") else ""
    bundle_str  = f"  {CYAN}[kit]{RESET}" if bundle else ""

    cor_desc = RED if acima else (GREEN if (desc and desc > 0) else YELLOW)
    cor_marg = GREEN if (margem and margem > 0) else (RED if margem is not None and margem < 0 else DIM)

    print(f"\n{'─'*68}")
    print(f"{BOLD}{YELLOW}[{i}/{total}] {titulo}{RESET}{premium_str}{bundle_str}")
    print(f"{'─'*68}")
    print(f"  {DIM}marca/modelo:{RESET}  {lote.get('marca') or '?'}  ·  {lote.get('modelo') or '?'}")
    if bundle and extras:
        print(f"  {DIM}extras kit:{RESET}    {', '.join(extras)}")
    print(f"  {DIM}local:{RESET}         {lote.get('cidade') or '?'} / {lote.get('estado') or '?'}")
    print(f"  {DIM}lance:{RESET}         {lote['lance']}")
    kit_ref = "  (preço combo ML)" if bundle else ""
    print(f"  {DIM}mercado ML:{RESET}    {lote['preco_mercado_fmt']}{DIM}{kit_ref}{RESET}  {cor_desc}{lote['desconto_label']}{RESET}")
    print(f"  {DIM}margem bruta:{RESET}  {cor_marg}{lote['margem_bruta_fmt']}{RESET}")
    print(f"  {DIM}data:{RESET}          {lote['data_leilao']}")
    print(f"  {DIM}imagens:{RESET}       {len(lote['imagens'])}x")
    print(f"  {DIM}query ML:{RESET}      {lote.get('query_ml')}")
    print(f"  {DIM}link:{RESET}          {lote['link']}")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Pestana Leilões (drones) → auctions.tecnologia")
    parser.add_argument("--no-upload",    action="store_true", help="Não sobe pro Supabase")
    parser.add_argument("--no-market",    action="store_true", help="Pula busca de preço ML")
    parser.add_argument("--show-browser", action="store_true", help="Abre browser visível")
    parser.add_argument("--debug",        action="store_true", help="Log detalhado ML")
    parser.add_argument("--limit",        type=int, default=0, help="Limita a N lotes")
    parser.add_argument("--output",       default="drone_coleta.json")
    args = parser.parse_args()

    headless = not args.show_browser

    print(f"\n{BOLD}{'='*68}{RESET}")
    print(f"{BOLD}  🚁  PESTANA LEILÕES — DRONES{RESET}")
    print(f"{BOLD}{'='*68}{RESET}")
    print(f"  {DIM}busca:  {SEARCH_URL.format(pagina=1)}{RESET}")
    print(f"  {DIM}upload: {'nao (debug)' if args.no_upload else 'sim → auctions.tecnologia'}{RESET}")
    print(f"  {DIM}premium: ≥ {PREMIUM_DESCONTO_MIN:.0f}% abaixo do mercado{RESET}\n")

    # ── 1. Coleta Playwright ─────────────────────────────────────────────
    print(f"{BOLD}  🌐  Coletando com Playwright...{RESET}\n")
    cards_brutos, detalhes_dict = await coletar_playwright(headless=headless, limit=args.limit)

    # ── 2. Extração estruturada ──────────────────────────────────────────
    print(f"\n{BOLD}  🔧  Extraindo campos...{RESET}\n")
    lotes, falhos = [], []
    for i, card in enumerate(cards_brutos, 1):
        detalhe = detalhes_dict.get(i, {})
        lote    = extract(card, detalhe)
        if lote:
            lotes.append(lote)
        else:
            falhos.append(card.get("titulo", "?"))

    print(f"  {GREEN}✓  {len(lotes)} drones extraídos{RESET}  ·  {RED}{len(falhos)} falhos/ignorados{RESET}")
    if falhos:
        print(f"  {DIM}Ignorados: {', '.join(falhos[:5])}{RESET}")

    if not lotes:
        print(f"\n  {YELLOW}Nenhum drone encontrado. Verifique a URL ou tente --show-browser.{RESET}")
        return

    # ── 3. Preço de mercado via Mercado Livre ────────────────────────────
    if not args.no_market:
        print(f"\n{BOLD}  🔍  Buscando preços no Mercado Livre ({len(lotes)} drones)...{RESET}\n")
        lotes = await enriquecer_precos(lotes, debug=args.debug)

    # ── 4. Ordena por desconto ───────────────────────────────────────────
    lotes.sort(key=lambda x: x.get("desconto_pct") or -999, reverse=True)

    # ── 5. Print ─────────────────────────────────────────────────────────
    for i, lote in enumerate(lotes, 1):
        print_lote(lote, i, len(lotes))

    # ── 6. Resumo ────────────────────────────────────────────────────────
    com_mercado = sum(1 for l in lotes if l.get("preco_mercado"))
    com_desc    = sum(1 for l in lotes if l.get("desconto_pct") is not None and l["desconto_pct"] > 0)
    com_acima   = sum(1 for l in lotes if l.get("acima_mercado"))
    com_img     = sum(1 for l in lotes if l.get("imagens"))
    com_premium = sum(1 for l in lotes if l.get("is_premium"))

    print(f"\n\n{'='*68}")
    print(f"{BOLD}  📊  RESUMO{RESET}")
    print(f"{'='*68}")
    print(f"  Total drones:        {len(lotes)}")
    print(f"  Com preço ML:        {com_mercado}")
    print(f"  Abaixo do mercado:   {com_desc}")
    if com_acima:
        print(f"  {RED}Acima do mercado:    {com_acima}{RESET}")
    print(f"  Com imagem:          {com_img}")
    print(f"  {CYAN}★ Premium (≥{PREMIUM_DESCONTO_MIN:.0f}%):    {com_premium}{RESET}")

    oportunidades = [l for l in lotes if l.get("desconto_pct") and l["desconto_pct"] > 0]
    if oportunidades:
        top = max(oportunidades, key=lambda x: x["desconto_pct"])
        print(f"  Melhor desconto:     {top['desconto_pct']}%  ({top['titulo'][:42]})")
        top_m = [l for l in oportunidades if l.get("margem_bruta") and l["margem_bruta"] > 0]
        if top_m:
            top_m.sort(key=lambda x: x["margem_bruta"], reverse=True)
            print(f"  Melhor margem:       {top_m[0]['margem_bruta_fmt']}  ({top_m[0]['titulo'][:38]})")

    # ── 7. Salva JSON ────────────────────────────────────────────────────
    output_data = {
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "total_lotes": len(lotes),
        "com_mercado": com_mercado,
        "com_premium": com_premium,
        "lotes":       lotes,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"\n  JSON salvo em: {args.output}")

    # ── 8. Upload Supabase ───────────────────────────────────────────────
    if not args.no_upload:
        upload_to_supabase(lotes)


if __name__ == "__main__":
    asyncio.run(main())