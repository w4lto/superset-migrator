"""
Transformação do ZIP de exportação do Superset.

O ZIP tem esta estrutura:
  export_<hash>/
  ├── metadata.yaml
  ├── databases/
  │   └── Meu_Banco.yaml       ← contém sqlalchemy_uri (substituído aqui)
  ├── datasets/
  │   └── Meu_Banco/
  │       └── tabela.yaml
  ├── charts/
  │   └── ....yaml
  └── dashboards/
      └── ....yaml

Esta classe:
  1. Abre o ZIP
  2. Identifica todos os arquivos em databases/
  3. Para cada um, substitui o sqlalchemy_uri pelo valor do config
  4. Reempacota como novo ZIP em memória
  5. Retorna relatório com o que foi substituído e o que está faltando
"""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Optional

import yaml

from .config import Config, mask_uri


@dataclass
class DBInfo:
    """Informações extraídas de um arquivo databases/*.yaml."""
    file_path: str          # caminho dentro do ZIP
    db_name: str            # valor de database_name
    current_uri: str        # sqlalchemy_uri atual (do ambiente de origem)


@dataclass
class DatasetInfo:
    """Informações extraídas de um arquivo datasets/*/*.yaml."""
    file_path: str          # caminho dentro do ZIP
    table_name: str         # nome da tabela/dataset
    database_name: str      # nome do banco de dados


@dataclass
class MigrationReport:
    target_env: str
    replaced: list[dict] = field(default_factory=list)      # {db_name, old_uri, new_uri}
    not_mapped: list[DBInfo] = field(default_factory=list)  # bancos sem mapeamento
    files_total: int = 0


def _is_database_file(zip_path: str) -> bool:
    """Verifica se o arquivo é um YAML dentro da pasta databases/."""
    parts = PurePosixPath(zip_path.replace("\\", "/")).parts
    return "databases" in parts and zip_path.endswith(".yaml")


def _is_dataset_file(zip_path: str) -> bool:
    """Verifica se o arquivo é um YAML dentro da pasta datasets/."""
    parts = PurePosixPath(zip_path.replace("\\", "/")).parts
    return "datasets" in parts and zip_path.endswith(".yaml")


def extract_db_infos(zip_bytes: bytes) -> list[DBInfo]:
    """
    Lê o ZIP e retorna informações de todos os bancos presentes,
    sem modificar nada. Útil para mostrar ao usuário quais bancos
    serão afetados antes de pedir as credenciais.
    """
    infos: list[DBInfo] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        for name in zf.namelist():
            if _is_database_file(name):
                content = zf.read(name).decode("utf-8")
                data = yaml.safe_load(content) or {}
                db_name = data.get("database_name", "<desconhecido>")
                uri = data.get("sqlalchemy_uri", "")
                infos.append(DBInfo(file_path=name, db_name=db_name, current_uri=uri))
    return infos


def extract_dataset_infos(zip_bytes: bytes) -> list[DatasetInfo]:
    """
    Lê o ZIP e retorna informações de todos os datasets presentes.
    Útil para sincronizar colunas após importação.
    """
    infos: list[DatasetInfo] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        for name in zf.namelist():
            if _is_dataset_file(name):
                content = zf.read(name).decode("utf-8")
                data = yaml.safe_load(content) or {}
                table_name = data.get("table_name", "")
                database_name = data.get("database_name", "")
                if table_name:
                    infos.append(DatasetInfo(
                        file_path=name,
                        table_name=table_name,
                        database_name=database_name,
                    ))
    return infos


def transform_zip(
    zip_bytes: bytes,
    target_env: str,
    cfg: Config,
    db_uuid_map: Optional[dict[str, str]] = None,
) -> tuple[bytes, MigrationReport]:
    """
    Transforma o ZIP substituindo as URIs de banco para o ambiente alvo.

    Args:
        zip_bytes: Conteúdo do ZIP original
        target_env: Nome do ambiente de destino
        cfg: Configuração com mapeamentos de URI
        db_uuid_map: Mapeamento opcional de nome do banco -> UUID no destino.
                     Se fornecido, substitui os UUIDs dos bancos para evitar conflitos.

    Retorna:
        (novo_zip_bytes, relatório)
    """
    report = MigrationReport(target_env=target_env)
    out_buf = io.BytesIO()

    # Primeiro, coleta os mapeamentos de UUID antigo -> novo
    old_to_new_uuid: dict[str, str] = {}

    if db_uuid_map:
        # Lê os UUIDs atuais dos bancos no ZIP
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            for name in zf.namelist():
                if _is_database_file(name):
                    content = zf.read(name).decode("utf-8")
                    data = yaml.safe_load(content) or {}
                    db_name = data.get("database_name", "")
                    old_uuid = data.get("uuid", "")
                    if db_name and old_uuid and db_name in db_uuid_map:
                        old_to_new_uuid[old_uuid] = db_uuid_map[db_name]

    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf_in, \
         zipfile.ZipFile(out_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf_out:

        for item in zf_in.infolist():
            report.files_total += 1
            content = zf_in.read(item.filename)

            if _is_database_file(item.filename):
                content, replacement, missing = _transform_db_yaml(
                    content, item.filename, target_env, cfg, db_uuid_map
                )
                if replacement:
                    report.replaced.append(replacement)
                if missing:
                    report.not_mapped.append(missing)

            elif _is_dataset_file(item.filename) and old_to_new_uuid:
                # Substitui database_uuid nos datasets
                content = _transform_dataset_uuid(content, old_to_new_uuid)

            zf_out.writestr(item, content)

    return out_buf.getvalue(), report


def _transform_dataset_uuid(content: bytes, uuid_map: dict[str, str]) -> bytes:
    """Substitui database_uuid em arquivos de dataset."""
    try:
        data = yaml.safe_load(content.decode("utf-8")) or {}
        old_uuid = data.get("database_uuid", "")
        if old_uuid and old_uuid in uuid_map:
            data["database_uuid"] = uuid_map[old_uuid]
            return yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False).encode("utf-8")
    except Exception:
        pass
    return content


def _transform_db_yaml(
    content: bytes,
    file_path: str,
    target_env: str,
    cfg: Config,
    db_uuid_map: Optional[dict[str, str]] = None,
) -> tuple[bytes, Optional[dict], Optional[DBInfo]]:
    """
    Transforma um arquivo databases/*.yaml.
    Retorna (novo_conteúdo, replacement_info, missing_info).
    Usa yaml.safe_load/dump preservando a estrutura mas não os comentários
    (comportamento padrão do Superset export — não há comentários no ZIP).
    """
    data = yaml.safe_load(content.decode("utf-8")) or {}
    db_name = data.get("database_name", "<desconhecido>")
    old_uri = data.get("sqlalchemy_uri", "")

    mapping = cfg.get_mapping(db_name)
    if mapping is None:
        db_info = DBInfo(file_path=file_path, db_name=db_name, current_uri=old_uri)
        return content, None, db_info

    conn = mapping.get_conn(target_env)
    if conn is None:
        db_info = DBInfo(file_path=file_path, db_name=db_name, current_uri=old_uri)
        return content, None, db_info

    data["sqlalchemy_uri"] = conn.sqlalchemy_uri

    # Substitui UUID se fornecido
    if db_uuid_map and db_name in db_uuid_map:
        data["uuid"] = db_uuid_map[db_name]

    # Remove campos problemáticos do extra que causam erro na importação
    # (schema_options não é suportado em algumas versões do Superset)
    if "extra" in data and isinstance(data["extra"], dict):
        data["extra"].pop("schema_options", None)

    new_content = yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
    replacement = {
        "db_name": db_name,
        "old_uri": old_uri,
        "new_uri": conn.sqlalchemy_uri,
    }
    return new_content.encode("utf-8"), replacement, None
