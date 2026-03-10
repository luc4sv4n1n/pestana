#!/usr/bin/env python3
"""
fipe_client.py — Busca valor FIPE via API parallelum (gratuita, sem captcha)

API: https://fipe.parallelum.com.br/api/v2
Limite: 500 req/dia sem token, 1000/dia com token gratuito (fipe.online)

Uso:
    python fipe_client.py "Ford - Transit 350 FL AT - 2024 / 2025"
    python fipe_client.py "Gm - Onix 1.0Mt Lt - 2018 / 2019" --debug
    python fipe_client.py "Honda - CG 160 START - 2024 / 2024"

Como módulo:
    from fipe_client import buscar_valor_mercado
    r = await buscar_valor_mercado("Ford - Transit 350 FL AT - 2024 / 2025")
"""

import asyncio
import re
import argparse
import os
import httpx
from difflib import SequenceMatcher

# ─── Config ───────────────────────────────────────────────────────────────────

FIPE_BASE = "https://fipe.parallelum.com.br/api/v2"

FIPE_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
}

FIPE_TOKEN = os.getenv("FIPE_TOKEN", "")
if FIPE_TOKEN:
    FIPE_HEADERS["X-Subscription-Token"] = FIPE_TOKEN

MIN_SCORE_MARCA  = 0.75
MIN_SCORE_MODELO = 0.58  # mais rigoroso — evita matches errados como Onix→OnixPlus

# Categorias FIPE — tenta em ordem até achar
FIPE_CATEGORIAS = ["cars", "motorcycles", "trucks"]

# Marcas que são EXCLUSIVAMENTE de moto na FIPE
MARCAS_MOTO = {
    "honda", "yamaha", "kawasaki", "suzuki", "triumph", "ducati",
    "ktm", "royal enfield", "dafra", "shineray", "haojue",
    "kasinski", "traxx", "benelli", "cfmoto",
}

# Modelos/palavras que indicam moto mesmo em marca mista (ex: BMW S1000RR)
PALAVRAS_MOTO = {
    "cg", "xre", "biz", "pop", "elite", "pcx", "lead", "nxr", "bros",
    "cb", "cbr", "hornet", "shadow", "fazer", "ybr", "factor", "lander",
    "tenere", "xtz", "nmax", "ninja", "z", "versys", "er", "zx",
    "gsx", "bandit", "burgman", "intruder", "hayabusa",
    "r1", "r3", "r6", "r7", "r15", "r25", "mt", "fz",
    "s1000", "g310", "f850", "r1250", "adventure",
    "scrambler", "monster", "panigale", "multistrada",
}

# Palavras no modelo que indicam caminhão/pesado
PALAVRAS_TRUCK = {
    "truck", "caminhao", "caminhão", "cargo", "worker", "constellation",
    "atego", "actros", "axor", "tector", "stralis", "vertis",
}

# Prefixos de modelo que indicam truck:
#   - número com ponto: "24.280", "9.150" (VW, Ford Cargo)
#   - 4+ dígitos: "1719", "2630" (Mercedes, VW pesados)
# NÃO pega: "208", "307", "2008" (Peugeot), "500" (Fiat), "3008" seria 4 dígitos
# mas Peugeot 3008 começa com "3008" — logo excluímos marcas conhecidas de carro
RE_MODELO_TRUCK = re.compile(r"^\d{2,3}\.\d|^\d{4,}")

# Marcas que usam números como nome de modelo (carros) — nunca são truck
MARCAS_MODELO_NUMERICO = {
    "peugeot", "fiat", "bmw", "mercedes-benz", "audi", "volvo",
    "alfa romeo", "citroen", "renault",
}

# Marcas que SÓ fabricam carros/SUVs de passeio — nunca truck nem moto
MARCAS_SOMENTE_CARRO = {
    "jac", "byd", "chery", "caoa chery", "gwm", "great wall", "haval",
    "lifan", "geely", "mg", "subaru", "mitsubishi", "nissan", "toyota",
    "hyundai", "kia", "honda", "chevrolet", "ford", "volkswagen",
    "jeep", "dodge", "ram", "land rover", "land-rover", "jaguar",
    "porsche", "lexus", "infiniti", "acura", "tesla", "rivian",
    "peugeot", "fiat", "bmw", "mercedes-benz", "audi", "volvo",
    "alfa romeo", "citroen", "renault", "seat", "skoda", "dacia",
    "smart", "mini", "genesis", "ssangyong", "troller",
}


def _detectar_categorias(marca_norm: str, modelo_norm: str) -> list[str]:
    """
    Retorna a lista de categorias FIPE em ordem de probabilidade.
    Evita tentar 3 categorias quando a resposta é óbvia — economiza requests.
    """
    m1 = modelo_norm.split()[0] if modelo_norm.split() else ""

    # Marcas que só fazem carros — nunca truck, nunca moto
    if marca_norm in MARCAS_SOMENTE_CARRO:
        return ["cars"]

    # Truck: modelo começa com número tipo "24.280", "1719" etc
    # Mas ignora se a marca é conhecida por usar números como nome (Peugeot 208, Fiat 500)
    if RE_MODELO_TRUCK.match(modelo_norm) and marca_norm not in MARCAS_MODELO_NUMERICO:
        return ["trucks", "cars"]

    # Truck por palavra-chave no modelo
    if any(w in modelo_norm for w in PALAVRAS_TRUCK):
        return ["trucks", "cars"]

    # Moto: marca exclusivamente de moto
    if marca_norm in MARCAS_MOTO:
        return ["motorcycles", "cars"]

    # Moto: primeira palavra do modelo é conhecida de moto
    if m1 in PALAVRAS_MOTO:
        return ["motorcycles", "cars"]

    # Marca mista (BMW, Honda) — tenta carro primeiro, depois moto
    # (já cobre S1000RR via PALAVRAS_MOTO acima)
    return ["cars", "motorcycles", "trucks"]

# Aliases de marca — normaliza nomes alternativos para o nome FIPE
ALIAS_MARCA = {
    "gm":         "chevrolet",
    "vw":         "volkswagen",
    "vag":        "volkswagen",
    "mb":         "mercedes-benz",
    "mercedes":   "mercedes-benz",
    "land rover": "land-rover",
    "sr":         None,  # marca desconhecida → skip
    "--":         None,  # sem marca → skip
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def fmt_brl(v: float) -> str:
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def parse_brl_str(s: str) -> float | None:
    s = re.sub(r"[R$\s]", "", str(s)).replace(".", "").replace(",", ".")
    try:
        val = float(s)
        return val if 1_000 <= val <= 10_000_000 else None
    except Exception:
        return None


def _expand_nums(s: str) -> str:
    """
    Separa números colados a letras e expande cilindradas.
    '10MT' → '1 0 MT', '16' → '1 6', '20tfsi' → '2 0 tfsi'
    Só expande números <= 30 (cilindradas), não anos ou códigos grandes.
    """
    s = re.sub(r"(\d)([a-zA-Z])", r"\1 \2", s)
    s = re.sub(r"([a-zA-Z])(\d)", r"\1 \2", s)

    def splitter(m):
        n = m.group(0)
        return " ".join(n) if len(n) <= 2 and int(n) <= 30 else n

    return re.sub(r"\b\d{1,3}\b", splitter, s)


def _norm(s: str) -> str:
    """Normaliza: minúsculo, sem acentos, sem pontuação."""
    s = s.lower().strip()
    for a, b in [("a","a"),("a","a"),("a","a"),("a","a"),
                 ("e","e"),("e","e"),("e","e"),
                 ("i","i"),("i","i"),
                 ("o","o"),("o","o"),("o","o"),
                 ("u","u"),("u","u"),
                 ("c","c"),("n","n"),
                 ("ã","a"),("â","a"),("á","a"),("à","a"),("ä","a"),
                 ("ê","e"),("é","e"),("è","e"),("ë","e"),
                 ("î","i"),("í","i"),("ì","i"),
                 ("ô","o"),("õ","o"),("ó","o"),("ò","o"),("ö","o"),
                 ("û","u"),("ú","u"),("ù","u"),("ü","u"),
                 ("ç","c"),("ñ","n")]:
        s = s.replace(a, b)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = _expand_nums(s)
    return re.sub(r"\s+", " ", s).strip()


def _score(fipe_name: str, titulo_name: str) -> float:
    """
    Score combinado: SequenceMatcher base + bonus/penalidade por palavras.

    Lógica:
    - Base: similaridade de string geral
    - Bonus: palavras do título que aparecem no FIPE
    - Penalidade: se a primeira palavra (modelo principal) não bater
    - Penalidade extra: se há palavras numéricas no título (ex: "1.0", "160")
      que NÃO aparecem no nome FIPE — evita confusão entre versões/cilindradas
    """
    a = _norm(fipe_name)
    b = _norm(titulo_name)
    base = SequenceMatcher(None, a, b).ratio()

    words_a = set(a.split())
    words_b = set(b.split())
    common  = words_a & words_b
    bonus   = len(common) / max(len(words_b), 1) * 0.25

    # Penalidade: primeira palavra do modelo não bate
    first_word = b.split()[0] if b.split() else ""
    penalty = 0.0 if (not first_word or first_word in a) else 0.20

    # Penalidade extra: números/cilindradas do título ausentes no FIPE
    # Ex: título tem "1 0" (de 1.0) mas FIPE tem "1 4" → versão errada
    nums_b = {w for w in words_b if w.isdigit() and len(w) <= 3}
    nums_a = {w for w in words_a if w.isdigit()}
    if nums_b and not (nums_b & nums_a):
        penalty += 0.15

    return min(base + bonus - penalty, 1.0)


def _parse_titulo(titulo: str) -> tuple[str | None, str, int | None, int | None]:
    """
    Extrai (marca_norm, modelo_norm, ano_fab, ano_mod) do título Bradesco.
    Retorna marca=None para itens sem marca conhecida (equipamentos, etc).
    """
    anos = re.findall(r"\b(19[5-9]\d|20[0-3]\d)\b", titulo)
    ano_fab = int(anos[0]) if anos else None
    ano_mod = int(anos[1]) if len(anos) >= 2 else ano_fab

    clean = re.sub(r"\b(19[5-9]\d|20[0-3]\d)\b", "", titulo)
    clean = re.sub(r"[/\-–]", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()

    parts = [p.strip() for p in clean.split() if p.strip()]
    marca_raw  = parts[0].lower() if parts else ""
    modelo_raw = " ".join(parts[1:]) if len(parts) > 1 else ""

    marca_norm = ALIAS_MARCA.get(marca_raw, marca_raw)  # None = skip
    return marca_norm, _norm(modelo_raw), ano_fab, ano_mod


# ─── Cache em memória ─────────────────────────────────────────────────────────

_cache_marcas:  list[dict] | None = None
_cache_modelos: dict[str, list[dict]] = {}


# ─── Chamadas API ─────────────────────────────────────────────────────────────

DELAY_ENTRE_REQUESTS = 1.5  # segundos — seguro para 500 req/dia sem token


async def _get(client: httpx.AsyncClient, path: str) -> list | dict | None:
    await asyncio.sleep(DELAY_ENTRE_REQUESTS)
    try:
        resp = await client.get(f"{FIPE_BASE}{path}", timeout=15)
        if resp.status_code == 429:
            raise RuntimeError(
                "⛔ FIPE rate limit atingido (500 req/dia). "
                "Crie um token gratuito em https://fipe.online e exporte FIPE_TOKEN=<token>"
            )
        return resp.json() if resp.status_code == 200 else None
    except RuntimeError:
        raise
    except Exception:
        return None


# Cache por categoria
_cache_marcas_cat:  dict[str, list[dict]] = {}
_cache_modelos_cat: dict[str, list[dict]] = {}  # "cat/marca_code" → modelos


async def _get_marcas_cat(client: httpx.AsyncClient, cat: str) -> list[dict]:
    if cat not in _cache_marcas_cat:
        _cache_marcas_cat[cat] = await _get(client, f"/{cat}/brands") or []
    return _cache_marcas_cat[cat]


async def _get_modelos_cat(client: httpx.AsyncClient, cat: str, marca_code: str) -> list[dict]:
    key = f"{cat}/{marca_code}"
    if key not in _cache_modelos_cat:
        data = await _get(client, f"/{cat}/brands/{marca_code}/models") or {}
        _cache_modelos_cat[key] = data if isinstance(data, list) else data.get("models", [])
    return _cache_modelos_cat[key]


async def _buscar_marca_cat(
    client: httpx.AsyncClient, cat: str, marca_norm: str
) -> dict | None:
    marcas = await _get_marcas_cat(client, cat)
    melhor, score = None, 0.0
    for m in marcas:
        s = _score(m["name"], marca_norm)
        if s > score:
            score, melhor = s, m
    return melhor if score >= MIN_SCORE_MARCA else None


async def _buscar_modelo_cat(
    client: httpx.AsyncClient,
    cat: str,
    marca_code: str,
    modelo_norm: str,
    min_score: float = MIN_SCORE_MODELO,
) -> tuple[dict | None, float]:
    modelos = await _get_modelos_cat(client, cat, marca_code)
    melhor, score = None, 0.0
    for m in modelos:
        s = _score(m["name"], modelo_norm)
        if s > score:
            score, melhor = s, m
    if score < min_score:
        return None, score
    return melhor, score


async def _buscar_anos_match(
    client: httpx.AsyncClient,
    cat: str,
    marca_code: str,
    modelo_code: str,
    ano_mod: int | None,
    ano_fab: int | None,
    fallback_recente: bool = False,
) -> tuple[list[dict], bool]:
    """
    Retorna (anos, usou_fallback).
    Se fallback_recente=True e não achar o ano exato, devolve o ano mais recente disponível.
    """
    anos = await _get(client, f"/{cat}/brands/{marca_code}/models/{modelo_code}/years") or []
    if not anos:
        return [], False
    matches = [
        a for a in anos
        if (ano_mod and str(ano_mod) in a.get("name", ""))
        or (ano_fab and str(ano_fab) in a.get("name", ""))
    ]
    if matches:
        return matches, False
    if fallback_recente:
        # Pega o ano mais recente disponível (geralmente o primeiro da lista)
        return [anos[0]], True
    return [], False


# ─── Função principal ─────────────────────────────────────────────────────────

async def _tentar_busca(
    client: httpx.AsyncClient,
    marca_norm: str,
    modelo_norm: str,
    ano_fab: int | None,
    ano_mod: int | None,
    min_score: float,
    fallback_ano: bool,
    debug: bool,
) -> tuple[str | None, dict | None, dict | None, float, list[dict], bool]:
    """
    Tenta achar marca + modelo + anos em todas as categorias.
    Retorna (cat, marca, modelo, score, anos_match, ano_era_fallback).
    """
    melhor_cat, melhor_marca, melhor_modelo = None, None, None
    melhor_score = 0.0
    melhor_anos: list[dict] = []
    melhor_ano_fb = False

    categorias = _detectar_categorias(marca_norm, modelo_norm)

    for cat in categorias:
        m = await _buscar_marca_cat(client, cat, marca_norm)
        if not m:
            continue
        mod, sc = await _buscar_modelo_cat(client, cat, m["code"], modelo_norm, min_score=min_score)
        if not mod or sc <= melhor_score:
            continue
        anos, ano_fb = await _buscar_anos_match(
            client, cat, m["code"], mod["code"], ano_mod, ano_fab,
            fallback_recente=fallback_ano,
        )
        if not anos:
            continue
        melhor_cat, melhor_marca, melhor_modelo, melhor_score, melhor_anos, melhor_ano_fb = (
            cat, m, mod, sc, anos, ano_fb
        )

    return melhor_cat, melhor_marca, melhor_modelo, melhor_score, melhor_anos, melhor_ano_fb


async def buscar_valor_mercado(titulo: str, debug: bool = False) -> dict:
    """
    Busca valor FIPE pelo título do veículo (formato Bradesco).
    Usa o MENOR valor entre todas as versões do ano — conservador para margem.

    Cascata de fallbacks (do mais rigoroso ao mais permissivo):
      1. score normal  + ano exato
      2. score normal  + ano mais recente disponível  (ex: Transit 2025 não tabelado ainda)
      3. score relaxado (0.45) + ano exato
      4. score relaxado (0.45) + ano mais recente
      5. só primeira palavra do modelo + score relaxado + ano mais recente

    Retorna dict:
        valor, valor_min, valor_max, fonte, snippet, query
        valor = None se não encontrado após todos os fallbacks
    """
    resultado = {
        "query": titulo, "valor": None, "valor_min": None,
        "valor_max": None, "fonte": None, "snippet": None,
        "confiavel": False,
    }

    marca_norm, modelo_norm, ano_fab, ano_mod = _parse_titulo(titulo)

    if marca_norm is None:
        resultado["snippet"] = "Marca ignorada (equipamento/sem marca)"
        return resultado

    # Estratégias em cascata: (min_score, fallback_ano, modelo_query, label)
    ESTRATEGIAS = [
        (MIN_SCORE_MODELO, False, modelo_norm,            "exato"),
        (MIN_SCORE_MODELO, True,  modelo_norm,            "ano_recente"),
        (0.45,             False, modelo_norm,            "score_relaxado"),
        (0.45,             True,  modelo_norm,            "score_relaxado+ano_recente"),
        (0.45,             True,  modelo_norm.split()[0], "primeira_palavra+ano_recente"),
    ]

    async with httpx.AsyncClient(headers=FIPE_HEADERS, follow_redirects=True) as client:

        cat_found = marca = modelo = None
        modelo_score = 0.0
        anos_match: list[dict] = []
        estrategia_usada = "exato"
        ano_era_fallback = False

        for min_score, fb_ano, mod_query, label in ESTRATEGIAS:
            if debug:
                print(f"    ↪ tentando estratégia [{label}] score≥{min_score} fb_ano={fb_ano} modelo='{mod_query}'")
            cat, marca_, modelo_, score, anos, ano_fb = await _tentar_busca(
                client, marca_norm, mod_query, ano_fab, ano_mod,
                min_score=min_score, fallback_ano=fb_ano, debug=debug,
            )
            if cat and modelo_:
                cat_found, marca, modelo, modelo_score, anos_match, ano_era_fallback = (
                    cat, marca_, modelo_, score, anos, ano_fb
                )
                estrategia_usada = label
                break  # achou — para aqui

        if not marca:
            resultado["snippet"] = f"Marca não encontrada: '{marca_norm}'"
            return resultado
        if not modelo:
            resultado["snippet"] = f"Modelo sem match após todos os fallbacks ('{modelo_norm}')"
            return resultado
        if not anos_match:
            resultado["snippet"] = f"Ano {ano_mod}/{ano_fab} não encontrado em {marca['name']} {modelo['name']}"
            return resultado

        if debug:
            fb_tag = " ⚠ ano_fallback" if ano_era_fallback else ""
            print(f"    ✓ [{estrategia_usada}] cat:{cat_found}  {marca['name']} {modelo['name']} (score {modelo_score:.2f}){fb_tag}")

        # Preços de cada versão do ano
        precos: list[tuple[float, dict]] = []
        for ano_entry in anos_match:
            p = await _get(
                client,
                f"/{cat_found}/brands/{marca['code']}/models/{modelo['code']}/years/{ano_entry['code']}"
            )
            if not p:
                continue
            v = parse_brl_str(p.get("price") or p.get("Valor") or "")
            if v:
                precos.append((v, p))

        if not precos:
            resultado["snippet"] = f"Nenhum preço retornado para {marca['name']} {modelo['name']}"
            return resultado

    # Menor valor = mais conservador para cálculo de margem
    precos.sort(key=lambda x: x[0])
    valor_min = precos[0][0]
    valor_max = precos[-1][0]
    ref       = precos[0][1]

    tags = []
    if estrategia_usada != "exato":
        tags.append(f"fallback:{estrategia_usada}")
    if ano_era_fallback:
        tags.append(f"ano_ref:{ref.get('modelYear')}")
    tag_str = f" [{' · '.join(tags)}]" if tags else ""

    # Confiabilidade: ano retornado não pode divergir > 5 anos do pedido
    ano_retornado = ref.get("modelYear") or ref.get("anoModelo")
    try:
        ano_ret_int = int(str(ano_retornado)[:4])
        ano_ref_int = ano_mod or ano_fab or ano_ret_int
        ano_diff    = abs(ano_ret_int - ano_ref_int)
    except Exception:
        ano_diff = 0
    confiavel = ano_diff <= 5

    resultado.update({
        "valor":      round(valor_min),
        "valor_min":  round(valor_min),
        "valor_max":  round(valor_max),
        "fonte":      "fipe_parallelum",
        "confiavel":  confiavel,
        "snippet": (
            f"{ref.get('brand')} {ref.get('model')} {ref.get('modelYear')} "
            f"→ min {fmt_brl(valor_min)} / max {fmt_brl(valor_max)} "
            f"[{len(precos)} versão(ões)] [{ref.get('referenceMonth')}] "
            f"[match: {modelo['name']} score={modelo_score:.2f}]{tag_str}"
            + ("" if confiavel else " ⚠ ANO_DIVERGENTE")
        ),
    })
    return resultado


# ─── CLI ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Busca FIPE via parallelum API")
    parser.add_argument("titulo", nargs="?", default="Ford - Transit 350 FL AT - 2024 / 2025")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    print(f"\n  🔍  {args.titulo}")
    marca_norm, modelo_norm, ano_fab, ano_mod = _parse_titulo(args.titulo)
    print(f"  parsed → marca: '{marca_norm}'  modelo: '{modelo_norm}'  ano: {ano_fab}/{ano_mod}\n")

    r = await buscar_valor_mercado(args.titulo, debug=True)

    print(f"  fonte:    {r['fonte'] or '—'}")
    print(f"  valor:    {fmt_brl(r['valor']) if r['valor'] else '—'}")
    print(f"  min/max:  {fmt_brl(r['valor_min']) if r['valor_min'] else '—'}  →  {fmt_brl(r['valor_max']) if r['valor_max'] else '—'}")
    print(f"  snippet:  {r['snippet'] or '—'}\n")


if __name__ == "__main__":
    asyncio.run(main())