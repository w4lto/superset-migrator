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

        # Filtra localmente se necessário
        if search:
            search_lower = search.lower()
            result = [ds for ds in result if search_lower in ds.get("table_name", "").lower()]

        return result

    def get_dataset_by_name(self, table_name: str, database_name: str = "") -> dict | None:
        """Busca um dataset pelo nome da tabela (e opcionalmente banco)."""
        datasets = self.list_datasets(search=table_name)
        for ds in datasets:
            if ds.get("table_name") == table_name:
                if database_name:
                    if ds.get("database", {}).get("database_name") == database_name:
                        return ds
                else:
                    return ds
        return None

    def sync_dataset_columns(self, dataset_id: int) -> bool:
        """
        Sincroniza as colunas de um dataset com a fonte de dados.
        Equivalente a "Sync columns from source" na UI.
        """
        csrf = self._get_csrf_token()
        resp = self._http.put(
            f"{self.base_url}/api/v1/dataset/{dataset_id}/refresh",
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )
        return resp.status_code == 200

    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
