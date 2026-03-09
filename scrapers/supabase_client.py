#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT — auctions.veiculos
✅ Schema: auctions / tabela: veiculos
✅ Upsert por link: SELECT → INSERT ou PATCH
✅ Normaliza chaves antes de enviar (fix PGRST102)
✅ Remove duplicatas DENTRO do batch (fix PGRST21000)
"""

import os
import time
import requests
from typing import List, Dict, Optional


class SupabaseClient:
    """Cliente Supabase — schema auctions, tabela veiculos"""

    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

        if not self.url or not self.key:
            raise ValueError("⚠️  Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")

        self.url = self.url.rstrip('/')

        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    # =========================================================================
    # DEDUPLICAÇÃO E NORMALIZAÇÃO
    # =========================================================================

    def _deduplicate_batch(self, items: List[Dict]) -> tuple:
        """
        Remove duplicatas DENTRO do batch baseado em `link`.
        Resolve PGRST21000: "cannot affect row a second time"
        """
        if not items:
            return items, 0

        seen: dict = {}
        unique: list = []
        dupes = 0

        for item in items:
            key = item.get('link')
            if not key:
                continue
            if key not in seen:
                seen[key] = True
                unique.append(item)
            else:
                dupes += 1

        return unique, dupes

    def _normalize_batch_keys(self, items: List[Dict]) -> List[Dict]:
        """
        Garante que todos os items do batch tenham as mesmas chaves.
        Resolve PGRST102: "All object keys must match"
        """
        if not items:
            return items

        all_keys: set = set()
        for item in items:
            all_keys.update(item.keys())

        return [{k: item.get(k) for k in all_keys} for item in items]

    # =========================================================================
    # UPSERT → auctions.veiculos
    # =========================================================================

    def upsert_veiculos(self, items: List[Dict]) -> Dict:
        """
        Upsert em auctions.veiculos com conflito em `link`.

        Campos esperados (mínimo obrigatório):
            titulo, tipo, ano_fabricacao, ano_modelo,
            modalidade, valor_inicial, data_encerramento, link
        """
        return self.upsert('veiculos', items)

    def upsert(self, tabela: str, items: List[Dict]) -> Dict:
        """
        Upsert via link: para cada item, verifica se já existe pelo `link`
        e faz INSERT ou PATCH conforme necessário.

        Não depende de UNIQUE constraint no banco — funciona com o schema atual.
        """
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0,
                    'total': 0, 'duplicates_removed': 0}

        # Remove campos que o DB gerencia automaticamente
        _auto_fields = {'id', 'criado_em', 'atualizado_em'}
        for item in items:
            for f in _auto_fields:
                item.pop(f, None)

        stats = {
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'total': len(items),
            'duplicates_removed': 0,
        }

        batch_size = 500
        total_batches = (len(items) + batch_size - 1) // batch_size

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            try:
                batch_unique, batch_dupes = self._deduplicate_batch(batch)

                if batch_dupes > 0:
                    stats['duplicates_removed'] += batch_dupes
                    print(f"  🔄 Batch {batch_num}/{total_batches}: "
                          f"{batch_dupes} duplicata(s) removida(s)")

                if not batch_unique:
                    print(f"  ⚠️  Batch {batch_num}/{total_batches}: "
                          f"vazio após deduplicação")
                    continue

                # Busca quais links já existem no banco
                links = [item['link'] for item in batch_unique if item.get('link')]
                existing_links = self._fetch_existing_links(tabela, links)

                to_insert = []
                to_update = []

                for item in batch_unique:
                    link = item.get('link')
                    if not link:
                        stats['errors'] += 1
                        continue
                    if link in existing_links:
                        to_update.append((existing_links[link], item))
                    else:
                        to_insert.append(item)

                # INSERT em batch
                if to_insert:
                    normalized = self._normalize_batch_keys(to_insert)
                    inserted = self._insert_batch(tabela, normalized, batch_num, total_batches)
                    stats['inserted'] += inserted
                    if inserted < len(to_insert):
                        stats['errors'] += len(to_insert) - inserted

                # PATCH individualmente (cada registro tem seu próprio id)
                for record_id, item in to_update:
                    ok = self._patch_record(tabela, record_id, item)
                    if ok:
                        stats['updated'] += 1
                    else:
                        stats['errors'] += 1

                inserted_count = len(to_insert)
                updated_count = len(to_update)
                print(f"  ✅ Batch {batch_num}/{total_batches}: "
                      f"{inserted_count} inserido(s), {updated_count} atualizado(s)")

            except requests.exceptions.Timeout:
                print(f"  ⏱️  Batch {batch_num}/{total_batches}: Timeout (120s)")
                stats['errors'] += len(batch)

            except Exception as e:
                print(f"  ❌ Batch {batch_num}/{total_batches}: "
                      f"{type(e).__name__}: {str(e)[:200]}")
                stats['errors'] += len(batch)

            if batch_num < total_batches:
                time.sleep(0.5)

        return stats

    def _fetch_existing_links(self, tabela: str, links: List[str]) -> Dict[str, str]:
        """
        Retorna {link: id} para todos os links que já existem na tabela.
        Usa filtro `in` do PostgREST para buscar em batch.
        """
        if not links:
            return {}

        existing = {}
        # PostgREST aceita: link=in.(url1,url2,...) — mas URLs podem ter vírgulas/parênteses
        # Mais seguro: buscar individualmente para links com caracteres especiais,
        # ou usar o formato correto de array do PostgREST
        chunk_size = 100
        for j in range(0, len(links), chunk_size):
            chunk = links[j:j + chunk_size]
            # Formato PostgREST: link=in.(val1,val2)
            in_filter = f"({','.join(chunk)})"
            try:
                r = self.session.get(
                    f"{self.url}/rest/v1/{tabela}",
                    params={'select': 'id,link', 'link': f'in.{in_filter}'},
                    timeout=30,
                )
                if r.status_code == 200:
                    for row in r.json():
                        existing[row['link']] = row['id']
                else:
                    print(f"  ⚠️  Erro ao buscar links existentes: HTTP {r.status_code}")
            except Exception as e:
                print(f"  ⚠️  Erro ao buscar links existentes: {e}")

        return existing

    def _insert_batch(self, tabela: str, items: List[Dict],
                      batch_num: int, total_batches: int) -> int:
        """INSERT em batch. Retorna quantidade inserida."""
        url = f"{self.url}/rest/v1/{tabela}"
        insert_headers = {
            **self.headers,
            'Prefer': 'return=representation',
        }
        r = self.session.post(url, json=items, headers=insert_headers, timeout=120)
        if r.status_code in (200, 201):
            try:
                return len(r.json()) if isinstance(r.json(), list) else len(items)
            except Exception:
                return len(items)
        else:
            print(f"  ❌ INSERT batch {batch_num}/{total_batches}: "
                  f"HTTP {r.status_code} — {r.text[:300]}")
            return 0

    def _patch_record(self, tabela: str, record_id: str, item: Dict) -> bool:
        """PATCH de um único registro pelo id."""
        url = f"{self.url}/rest/v1/{tabela}?id=eq.{record_id}"
        patch_headers = {**self.headers, 'Prefer': 'return=minimal'}
        try:
            r = self.session.patch(url, json=item, headers=patch_headers, timeout=30)
            return r.status_code in (200, 204)
        except Exception as e:
            print(f"  ⚠️  PATCH {record_id}: {e}")
            return False

    # =========================================================================
    # AUXILIARES
    # =========================================================================

    def test(self) -> bool:
        """Testa conexão com Supabase."""
        try:
            r = self.session.get(f"{self.url}/rest/v1/", timeout=10)
            if r.status_code == 200:
                print("✅ Conexão com Supabase OK")
                return True
            print(f"❌ HTTP {r.status_code}: {r.text[:200]}")
            return False
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False

    def get_stats(self, tabela: str = 'veiculos') -> Dict:
        """Retorna estatísticas da tabela (total e ativos)."""
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30,
            )
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0/0').split('/')[-1])

                r_a = self.session.get(
                    url,
                    params={'select': 'count', 'ativo': 'eq.true'},
                    headers={**self.headers, 'Prefer': 'count=exact'},
                    timeout=30,
                )
                active = 0
                if r_a.status_code == 200:
                    active = int(r_a.headers.get('Content-Range', '0/0')
                                 .split('/')[-1])

                return {'total': total, 'active': active,
                        'inactive': total - active, 'table': tabela}
        except Exception as e:
            print(f"  ⚠️  Erro ao buscar stats: {e}")
        return {'total': 0, 'active': 0, 'inactive': 0, 'table': tabela}

    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()