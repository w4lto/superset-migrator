"""
Cliente HTTP para a API do Apache Superset.

Suporta:
  - Autenticação via JWT (POST /api/v1/security/login)
  - Listagem de dashboards com filtro por título ou slug
  - Busca por slug diretamente (GET /api/v1/dashboard/{slug})
  - Export de dashboard como ZIP (GET /api/v1/dashboard/export/)
  - Import de ZIP (POST /api/v1/dashboard/import/)
"""

from __future__ import annotations

import io
import warnings

import httpx
import urllib3

from . import logger


class DashboardInfo:
    def __init__(self, data: dict):
        self.id: int = data["id"]
        self.title: str = data.get("dashboard_title", "")
        self.slug: str = data.get("slug") or ""
        self.status: str = data.get("status", "")

    def display_label(self) -> str:
        slug_part = f"  [slug: {self.slug}]" if self.slug else ""
        return f"[{self.id}] {self.title}{slug_part}"


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 60, verify_ssl: bool = False):
        self.base_url = base_url.rstrip("/")
        if not verify_ssl:
            warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
        self._http = httpx.Client(timeout=timeout, verify=verify_ssl)
        self._token: str = ""
        self._login(username, password)

    def _login(self, username: str, password: str):
        url = f"{self.base_url}/api/v1/security/login"
        logger.log_info(f"Autenticando em {self.base_url} como '{username}'")

        resp = self._http.post(
            url,
            json={"username": username, "password": password, "provider": "db", "refresh": True},
        )

        if resp.status_code != 200:
            logger.log_request("POST", url, resp.status_code, resp.text, "Autenticação falhou")
            raise ConnectionError(
                f"Autenticação falhou ({resp.status_code}): {resp.text[:200]}"
            )

        logger.log_request("POST", url, resp.status_code)
        self._token = resp.json()["access_token"]
        self._http.headers.update({"Authorization": f"Bearer {self._token}"})

    def _get_csrf_token(self) -> str:
        resp = self._http.get(f"{self.base_url}/api/v1/security/csrf_token/")
        resp.raise_for_status()
        return resp.json()["result"]

    # ── Dashboard ─────────────────────────────────────────────────────────────

    def get_dashboard_by_slug(self, slug: str) -> DashboardInfo:
        """
        Busca um dashboard pelo slug customizado.
        O Superset aceita slug diretamente: GET /api/v1/dashboard/{slug}
        """
        resp = self._http.get(f"{self.base_url}/api/v1/dashboard/{slug}")
        if resp.status_code == 404:
            raise ValueError(f"Dashboard com slug '{slug}' não encontrado.")
        resp.raise_for_status()
        return DashboardInfo(resp.json()["result"])

    def get_dashboard_by_id(self, dashboard_id: int) -> DashboardInfo:
        resp = self._http.get(f"{self.base_url}/api/v1/dashboard/{dashboard_id}")
        resp.raise_for_status()
        return DashboardInfo(resp.json()["result"])

    def list_dashboards(self, search: str = "") -> list[DashboardInfo]:
        """Lista dashboards com filtro opcional por título/slug."""
        if search:
            # A API do Superset usa Rison para queries complexas
            rison_filter = (
                f"(filters:!((col:dashboard_title,opr:DashboardTitleOrSlugFilter,"
                f"val:'{search}')),page:0,page_size:100)"
            )
        else:
            rison_filter = "(page:0,page_size:100,order_column:dashboard_title,order_direction:asc)"

        resp = self._http.get(
            f"{self.base_url}/api/v1/dashboard/",
            params={"q": rison_filter},
        )
        resp.raise_for_status()
        return [DashboardInfo(d) for d in resp.json().get("result", [])]

    def export_dashboard(self, dashboard_id: int) -> bytes:
        """Exporta um dashboard como bytes de ZIP."""
        url = f"{self.base_url}/api/v1/dashboard/export/"
        logger.log_info(f"Exportando dashboard ID={dashboard_id}")

        resp = self._http.get(url, params={"q": f"!({dashboard_id})"})

        if resp.status_code != 200:
            logger.log_request("GET", url, resp.status_code, resp.text, "Exportação falhou")
            raise RuntimeError(f"Exportação falhou ({resp.status_code}): {resp.text[:300]}")

        logger.log_request("GET", url, resp.status_code)
        logger.log_info(f"Dashboard exportado ({len(resp.content)} bytes)")
        return resp.content

    def import_dashboard(self, zip_bytes: bytes, overwrite: bool = True) -> None:
        """Importa um ZIP de dashboard. Requer CSRF token."""
        url = f"{self.base_url}/api/v1/dashboard/import/"
        logger.log_info(f"Importando dashboard ({len(zip_bytes)} bytes, overwrite={overwrite})")

        csrf = self._get_csrf_token()
        files = {"formData": ("dashboard.zip", io.BytesIO(zip_bytes), "application/zip")}
        data = {"overwrite": str(overwrite).lower()}

        resp = self._http.post(
            url,
            files=files,
            data=data,
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )

        if resp.status_code != 200:
            # Log completo do erro para diagnóstico
            logger.log_request("POST", url, resp.status_code, resp.text, "Importação falhou")

            # Tenta extrair mensagem de erro do JSON
            error_msg = self._extract_error_message(resp)
            raise RuntimeError(f"Importação falhou ({resp.status_code}): {error_msg}")

        logger.log_request("POST", url, resp.status_code)
        logger.log_info("Dashboard importado com sucesso")

    def _extract_error_message(self, resp) -> str:
        """Extrai mensagem de erro da resposta, seja JSON ou HTML."""
        try:
            # Tenta parsear como JSON
            data = resp.json()

            # Formato comum do Superset: {"errors": [{"message": "..."}]}
            if "errors" in data:
                errors = data["errors"]
                if isinstance(errors, list) and errors:
                    messages = []
                    for err in errors:
                        if isinstance(err, dict):
                            msg = err.get("message", "")
                            extra = err.get("extra", {})
                            if extra and isinstance(extra, dict):
                                # Detalhes adicionais de validação
                                issue = extra.get("issue_codes", [])
                                if issue:
                                    msg += f" [{issue}]"
                            if msg:
                                messages.append(msg)
                    if messages:
                        return " | ".join(messages)

            # Formato alternativo: {"message": "..."}
            if "message" in data:
                return data["message"]

            # Retorna o JSON como string se não encontrou padrão conhecido
            return str(data)[:500]

        except Exception:
            # Não é JSON, retorna texto truncado
            text = resp.text
            if "<html" in text.lower():
                # É HTML, extrai apenas o título ou mensagem principal
                if "<title>" in text:
                    start = text.find("<title>") + 7
                    end = text.find("</title>")
                    if end > start:
                        return f"[HTML] {text[start:end]}"
                return "[HTML Error Page]"
            return text[:500]

    # ── Database ───────────────────────────────────────────────────────────────

    def list_databases(self) -> list[dict]:
        """Lista todos os bancos de dados cadastrados no Superset."""
        url = f"{self.base_url}/api/v1/database/"
        rison_filter = "(page:0,page_size:100,order_column:database_name,order_direction:asc)"

        resp = self._http.get(url, params={"q": rison_filter})
        resp.raise_for_status()

        result = resp.json().get("result", [])
        logger.log_info(f"Listados {len(result)} banco(s) de dados")
        return result

    def get_database_by_name(self, database_name: str) -> dict | None:
        """Busca um banco de dados pelo nome e retorna detalhes completos (incluindo UUID)."""
        databases = self.list_databases()
        for db in databases:
            if db.get("database_name") == database_name:
                # Busca detalhes completos do banco (inclui UUID)
                db_id = db.get("id")
                if db_id:
                    resp = self._http.get(f"{self.base_url}/api/v1/database/{db_id}")
                    if resp.status_code == 200:
                        return resp.json().get("result", {})
                return db
        return None

    def get_database_uuid_map(self) -> dict[str, str]:
        """Retorna mapeamento de nome do banco -> UUID para todos os bancos."""
        uuid_map = {}
        databases = self.list_databases()
        for db in databases:
            db_id = db.get("id")
            db_name = db.get("database_name", "")
            if db_id and db_name:
                # Busca UUID do banco
                resp = self._http.get(f"{self.base_url}/api/v1/database/{db_id}")
                if resp.status_code == 200:
                    result = resp.json().get("result", {})
                    uuid = result.get("uuid")
                    if uuid:
                        uuid_map[db_name] = uuid
        return uuid_map

    def get_database_names(self) -> set[str]:
        """Retorna conjunto com nomes de todos os bancos cadastrados."""
        databases = self.list_databases()
        return {db.get("database_name", "") for db in databases}

    def database_exists(self, database_name: str) -> bool:
        """Verifica se um banco de dados já existe no Superset."""
        return database_name in self.get_database_names()

    def create_database(self, database_name: str, sqlalchemy_uri: str, expose_in_sqllab: bool = True, skip_if_exists: bool = True) -> dict:
        """
        Cria um novo banco de dados no Superset.

        Args:
            database_name: Nome do banco (como aparece no Superset)
            sqlalchemy_uri: URI de conexão SQLAlchemy
            expose_in_sqllab: Se deve aparecer no SQL Lab (default: True)
            skip_if_exists: Se True, não tenta criar se já existir (default: True)

        Returns:
            Dados do banco criado (ou vazio se já existia e skip_if_exists=True)

        Raises:
            RuntimeError: Se a criação falhar
        """
        # Verifica se já existe
        if skip_if_exists and self.database_exists(database_name):
            logger.log_info(f"Banco '{database_name}' já existe, pulando criação")
            return {"id": None, "database_name": database_name, "already_exists": True}

        url = f"{self.base_url}/api/v1/database/"
        logger.log_info(f"Criando banco de dados '{database_name}'")
        logger.log_debug(f"URI: {sqlalchemy_uri[:50]}...")

        csrf = self._get_csrf_token()

        payload = {
            "database_name": database_name,
            "sqlalchemy_uri": sqlalchemy_uri,
            "expose_in_sqllab": expose_in_sqllab,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
            "allow_run_async": False,
        }

        resp = self._http.post(
            url,
            json=payload,
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )

        if resp.status_code not in (200, 201):
            error_msg = self._extract_error_message(resp)
            logger.log_request("POST", url, resp.status_code, resp.text, "Criação de banco falhou")
            raise RuntimeError(f"Falha ao criar banco '{database_name}' ({resp.status_code}): {error_msg}")

        logger.log_request("POST", url, resp.status_code)
        logger.log_info(f"Banco '{database_name}' criado com sucesso")
        return resp.json()

    def test_database_connection(self, sqlalchemy_uri: str) -> tuple[bool, str]:
        """
        Testa uma conexão de banco antes de criar.

        Returns:
            (sucesso: bool, mensagem: str)
        """
        url = f"{self.base_url}/api/v1/database/test_connection/"
        csrf = self._get_csrf_token()

        payload = {"sqlalchemy_uri": sqlalchemy_uri}

        resp = self._http.post(
            url,
            json=payload,
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )

        if resp.status_code == 200:
            return True, "Conexão OK"
        else:
            error_msg = self._extract_error_message(resp)
            return False, error_msg

    # ── Dataset ────────────────────────────────────────────────────────────────

    def list_datasets(self, search: str = "") -> list[dict]:
        """Lista datasets com filtro opcional por nome."""
        # Lista todos os datasets (a filtragem por nome com caracteres especiais
        # causa problemas no Rison, então filtramos localmente)
        rison_filter = "(page:0,page_size:1000,order_column:table_name,order_direction:asc)"

        resp = self._http.get(
            f"{self.base_url}/api/v1/dataset/",
            params={"q": rison_filter},
        )
        resp.raise_for_status()
        result = resp.json().get("result", [])

        logger.log_debug(f"list_datasets: {len(result)} dataset(s) retornados da API")
        if len(result) >= 1000:
            logger.log_debug("list_datasets: ATENÇÃO — limite de 1000 atingido, pode haver datasets não listados")

        # Filtra localmente se necessário
        if search:
            search_lower = search.lower()
            result = [ds for ds in result if search_lower in ds.get("table_name", "").lower()]

        return result

    def get_dataset_by_name(self, table_name: str, database_name: str = "") -> dict | None:
        """Busca um dataset pelo nome da tabela (e opcionalmente banco).

        Usa fallback em camadas:
        1. Match exato table_name + database_name
        2. Match exato table_name apenas
        3. Match case-insensitive table_name + database_name
        4. Match case-insensitive table_name apenas
        """
        datasets = self.list_datasets(search=table_name)

        table_lower = table_name.lower()
        db_lower = database_name.lower() if database_name else ""

        # Tier 1: match exato table_name + database_name
        if database_name:
            for ds in datasets:
                ds_db = ds.get("database", {}).get("database_name", "")
                if ds.get("table_name") == table_name and ds_db == database_name:
                    logger.log_debug(f"get_dataset_by_name: '{table_name}' encontrado via match exato (tier 1)")
                    return ds

        # Tier 2: match exato table_name apenas
        for ds in datasets:
            if ds.get("table_name") == table_name:
                logger.log_debug(f"get_dataset_by_name: '{table_name}' encontrado via table_name exato (tier 2)")
                return ds

        # Tier 3: match case-insensitive table_name + database_name
        if database_name:
            for ds in datasets:
                ds_db = ds.get("database", {}).get("database_name", "")
                if ds.get("table_name", "").lower() == table_lower and ds_db.lower() == db_lower:
                    logger.log_debug(f"get_dataset_by_name: '{table_name}' encontrado via case-insensitive + db (tier 3)")
                    return ds

        # Tier 4: match case-insensitive table_name apenas
        for ds in datasets:
            if ds.get("table_name", "").lower() == table_lower:
                logger.log_debug(f"get_dataset_by_name: '{table_name}' encontrado via case-insensitive (tier 4)")
                return ds

        logger.log_debug(f"get_dataset_by_name: '{table_name}' (db='{database_name}') não encontrado em nenhum tier")
        return None

    def sync_dataset_columns(self, dataset_id: int) -> bool:
        """
        Sincroniza as colunas de um dataset com a fonte de dados.
        Equivalente a "Sync columns from source" na UI.
        """
        csrf = self._get_csrf_token()
        url = f"{self.base_url}/api/v1/dataset/{dataset_id}/refresh"
        resp = self._http.put(
            url,
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )
        logger.log_request("PUT", url, resp.status_code)
        if 200 <= resp.status_code < 300:
            return True
        logger.log_error(
            f"sync_dataset_columns: dataset_id={dataset_id} retornou HTTP {resp.status_code}: {resp.text[:200]}"
        )
        return False

    # ── RLS (Row Level Security) ───────────────────────────────────────────────

    def list_rls_rules(self) -> list[dict]:
        """Lista todas as regras de RLS cadastradas no Superset."""
        url = f"{self.base_url}/api/v1/rowlevelsecurity/"
        rison_filter = "(page:0,page_size:100,order_column:name,order_direction:asc)"

        resp = self._http.get(url, params={"q": rison_filter})
        resp.raise_for_status()

        result = resp.json().get("result", [])
        logger.log_info(f"list_rls_rules: {len(result)} regra(s) encontrada(s)")
        return result

    def get_rls_rule(self, rls_id: int) -> dict:
        """Obtém uma regra de RLS com todos os detalhes (incluindo lista completa de tables)."""
        url = f"{self.base_url}/api/v1/rowlevelsecurity/{rls_id}"
        resp = self._http.get(url)
        resp.raise_for_status()
        return resp.json().get("result", {})

    def add_datasets_to_rls_rule(self, rls_id: int, dataset_ids: list[int]) -> bool:
        """
        Adiciona datasets a uma regra de RLS existente, preservando os já cadastrados.

        Faz GET do estado atual, merge de IDs (idempotente) e PUT com o payload completo.
        """
        rule = self.get_rls_rule(rls_id)

        existing_table_ids = {t["id"] for t in rule.get("tables", [])}
        all_table_ids = existing_table_ids | set(dataset_ids)
        # A API do Superset espera tabelas como lista de inteiros
        merged_tables = sorted(all_table_ids)

        roles = [{"id": r["id"]} for r in rule.get("roles", [])]

        payload: dict = {
            "clause": rule.get("clause", ""),
            "filter_type": rule.get("filter_type", "Regular"),
            "name": rule.get("name", ""),
            "roles": roles,
            "tables": merged_tables,
        }
        if rule.get("group_key") is not None:
            payload["group_key"] = rule["group_key"]
        if rule.get("description") is not None:
            payload["description"] = rule["description"]

        csrf = self._get_csrf_token()
        url = f"{self.base_url}/api/v1/rowlevelsecurity/{rls_id}"
        resp = self._http.put(
            url,
            json=payload,
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )
        logger.log_request("PUT", url, resp.status_code)

        if 200 <= resp.status_code < 300:
            return True

        logger.log_error(
            f"add_datasets_to_rls_rule: rls_id={rls_id} retornou HTTP {resp.status_code}: {resp.text[:200]}"
        )
        return False

    # ── Roles ─────────────────────────────────────────────────────────────────

    def list_roles(self) -> list[dict]:
        """Lista todos os papéis (roles) cadastrados no Superset.

        Tenta múltiplas variações de endpoint para compatibilidade com diferentes versões.
        """
        candidates = [
            (f"{self.base_url}/api/v1/security/roles/", {"q": "(page:0,page_size:100,order_column:name,order_direction:asc)"}),
            (f"{self.base_url}/api/v1/security/roles/", {}),
            (f"{self.base_url}/api/v1/roles/",           {"q": "(page:0,page_size:100,order_column:name,order_direction:asc)"}),
            (f"{self.base_url}/api/v1/roles/",           {}),
        ]

        last_exc: Exception | None = None
        for url, params in candidates:
            try:
                resp = self._http.get(url, params=params)
                if resp.status_code == 404:
                    logger.log_debug(f"list_roles: {url} retornou 404, tentando próxima variação")
                    continue
                resp.raise_for_status()
                result = resp.json().get("result", [])
                logger.log_info(f"list_roles: {len(result)} papel(is) encontrado(s) via {url}")
                return result
            except Exception as exc:
                last_exc = exc
                logger.log_debug(f"list_roles: erro em {url}: {exc}")
                continue

        raise RuntimeError(
            "Não foi possível listar os papéis (roles) do Superset. "
            "Verifique se o usuário tem permissão 'can_list' em 'RolesModelView' "
            f"ou se o endpoint está disponível nesta versão. Último erro: {last_exc}"
        )

    def get_dataset_perm(self, dataset_id: int) -> str | None:
        """
        Retorna a string de permissão de um dataset, ex: '[DBName].[TableName](id:N)'.
        Usada para localizar o permission_view_menu correspondente.
        """
        url = f"{self.base_url}/api/v1/dataset/{dataset_id}"
        resp = self._http.get(url)
        if resp.status_code != 200:
            logger.log_error(
                f"get_dataset_perm: dataset_id={dataset_id} retornou HTTP {resp.status_code}"
            )
            return None
        return resp.json().get("result", {}).get("perm")

    def get_datasource_permission_ids(self, perm_strings: list[str]) -> list[int]:
        """
        Retorna os IDs de permission_view_menu para permissões 'datasource access'
        cujo view_menu_name bate com as strings fornecidas.
        """
        if not perm_strings:
            return []

        url = f"{self.base_url}/api/v1/security/permissions/"
        rison_filter = (
            "(page:0,page_size:1000,"
            "filters:!((col:permission_name,opr:eq,val:'datasource access')))"
        )
        resp = self._http.get(url, params={"q": rison_filter})
        resp.raise_for_status()

        all_perms = resp.json().get("result", [])
        perm_set = set(perm_strings)
        matched_ids = [
            p["id"] for p in all_perms
            if p.get("view_menu_name") in perm_set
        ]

        logger.log_info(
            f"get_datasource_permission_ids: {len(matched_ids)}/{len(perm_strings)} perm(s) encontrada(s)"
        )
        if len(matched_ids) < len(perm_strings):
            found_names = {p.get("view_menu_name") for p in all_perms}
            missing = perm_set - found_names
            logger.log_error(f"get_datasource_permission_ids: permissões não encontradas: {missing}")

        return matched_ids

    def add_dataset_permissions_to_role(self, role_id: int, dataset_ids: list[int]) -> bool:
        """
        Adiciona permissões de acesso a datasets para um papel (role).

        Preserva as permissões existentes (merge seguro para APIs replace-all e append).
        """
        perm_strings = []
        for ds_id in dataset_ids:
            perm = self.get_dataset_perm(ds_id)
            if perm:
                perm_strings.append(perm)
            else:
                logger.log_error(
                    f"add_dataset_permissions_to_role: perm não encontrado para dataset_id={ds_id}"
                )

        if not perm_strings:
            logger.log_error(
                f"add_dataset_permissions_to_role: nenhum perm string encontrado para os datasets fornecidos"
            )
            return False

        new_pvm_ids = self.get_datasource_permission_ids(perm_strings)
        if not new_pvm_ids:
            logger.log_error(
                f"add_dataset_permissions_to_role: nenhuma permissão encontrada no Superset para {perm_strings}"
            )
            return False

        current_url = f"{self.base_url}/api/v1/security/roles/{role_id}/permissions/"
        resp = self._http.get(current_url)
        resp.raise_for_status()
        current_perms = resp.json().get("result", [])
        existing_pvm_ids = {p["id"] for p in current_perms}

        # Merge completo: funciona em modo replace-all e append
        all_ids = list(existing_pvm_ids | set(new_pvm_ids))

        if set(new_pvm_ids).issubset(existing_pvm_ids):
            logger.log_info(
                f"add_dataset_permissions_to_role: todas as permissões já existem no role_id={role_id}"
            )
            return True

        csrf = self._get_csrf_token()
        post_url = f"{self.base_url}/api/v1/security/roles/{role_id}/permissions/"
        resp = self._http.post(
            post_url,
            json={"permission_view_menu_ids": all_ids},
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )
        logger.log_request("POST", post_url, resp.status_code)

        if 200 <= resp.status_code < 300:
            return True

        logger.log_error(
            f"add_dataset_permissions_to_role: role_id={role_id} retornou HTTP {resp.status_code}: {resp.text[:200]}"
        )
        return False

    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
