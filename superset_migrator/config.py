"""
Gerenciamento de configuração persistente.

O arquivo de config fica em ~/.superset-migrator/config.yaml
e é gerenciado inteiramente pela própria ferramenta — o usuário
nunca precisa editá-lo manualmente.

Estrutura:
  environments:
    devel:
      url: "http://..."
      username: "admin"
      password: "..."
      is_source: true
    staging:
      url: "http://..."
      ...

  database_mappings:
    - name: "Meu Banco"           # nome exato no Superset de origem
      environments:
        devel:
          sqlalchemy_uri: "mssql+pyodbc://..."
        staging:
          sqlalchemy_uri: "mssql+pyodbc://..."
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml


CONFIG_DIR = Path.home() / ".superset-migrator"
CONFIG_FILE = CONFIG_DIR / "config.yaml"


class DBConn:
    def __init__(self, sqlalchemy_uri: str):
        self.sqlalchemy_uri = sqlalchemy_uri

    def to_dict(self) -> dict:
        return {"sqlalchemy_uri": self.sqlalchemy_uri}


class Environment:
    def __init__(self, name: str, url: str, username: str, password: str, is_source: bool = False):
        self.name = name
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.is_source = is_source

    def to_dict(self) -> dict:
        d = {
            "url": self.url,
            "username": self.username,
            "password": self.password,
        }
        if self.is_source:
            d["is_source"] = True
        return d


class DatabaseMapping:
    def __init__(self, name: str, environments: dict[str, DBConn] | None = None):
        self.name = name
        self.environments: dict[str, DBConn] = environments or {}

    def get_conn(self, env_name: str) -> Optional[DBConn]:
        return self.environments.get(env_name)

    def set_conn(self, env_name: str, conn: DBConn):
        self.environments[env_name] = conn

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "environments": {k: v.to_dict() for k, v in self.environments.items()},
        }


class Config:
    def __init__(self):
        self.environments: dict[str, Environment] = {}
        self.database_mappings: list[DatabaseMapping] = []

    # ── Environments ──────────────────────────────────────────────────────────

    def add_environment(self, env: Environment):
        # Se for marcado como source, remove o flag dos outros
        if env.is_source:
            for e in self.environments.values():
                e.is_source = False
        self.environments[env.name] = env

    def remove_environment(self, name: str):
        self.environments.pop(name, None)

    def source_env(self) -> Optional[str]:
        """Retorna o nome do ambiente marcado como is_source."""
        for name, env in self.environments.items():
            if env.is_source:
                return name
        # fallback: primeiro ambiente
        return next(iter(self.environments), None)

    def target_envs(self) -> list[str]:
        """Retorna os ambientes que NÃO são o de origem."""
        src = self.source_env()
        return [name for name in self.environments if name != src]

    # ── Database Mappings ─────────────────────────────────────────────────────

    def get_mapping(self, db_name: str) -> Optional[DatabaseMapping]:
        for m in self.database_mappings:
            if m.name == db_name:
                return m
        return None

    def get_or_create_mapping(self, db_name: str) -> DatabaseMapping:
        m = self.get_mapping(db_name)
        if m is None:
            m = DatabaseMapping(db_name)
            self.database_mappings.append(m)
        return m

    def remove_mapping(self, db_name: str):
        self.database_mappings = [m for m in self.database_mappings if m.name != db_name]

    def mapped_dbs(self) -> list[str]:
        return [m.name for m in self.database_mappings]

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self):
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "environments": {
                name: env.to_dict() for name, env in self.environments.items()
            },
            "database_mappings": [m.to_dict() for m in self.database_mappings],
        }
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)

    @classmethod
    def load(cls) -> "Config":
        cfg = cls()
        if not CONFIG_FILE.exists():
            return cfg

        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        for name, env_data in (data.get("environments") or {}).items():
            cfg.environments[name] = Environment(
                name=name,
                url=env_data.get("url", ""),
                username=env_data.get("username", ""),
                password=env_data.get("password", ""),
                is_source=env_data.get("is_source", False),
            )

        for mapping_data in data.get("database_mappings") or []:
            envs = {}
            for env_name, conn_data in (mapping_data.get("environments") or {}).items():
                envs[env_name] = DBConn(conn_data.get("sqlalchemy_uri", ""))
            cfg.database_mappings.append(
                DatabaseMapping(name=mapping_data["name"], environments=envs)
            )

        return cfg

    def is_empty(self) -> bool:
        return len(self.environments) == 0


def mask_uri(uri: str) -> str:
    """Oculta a senha de uma SQLAlchemy URI para exibição segura."""
    if "@" in uri:
        scheme_end = uri.find("://")
        at_idx = uri.rfind("@", 0, uri.rfind("/", scheme_end + 3) if "/" in uri[scheme_end + 3:] else len(uri))
        if scheme_end > 0 and at_idx > scheme_end:
            user_info = uri[scheme_end + 3 : at_idx]
            if ":" in user_info:
                user = user_info[: user_info.rfind(":")]
                return uri[: scheme_end + 3] + user + ":***@" + uri[at_idx + 1 :]
    return uri
