"""
Sistema de logging para o superset-migrator.

Cria um arquivo de log para cada execução em ~/.superset-migrator/logs/
com informações detalhadas para diagnóstico de problemas.
"""

from __future__ import annotations

import logging
import datetime
from pathlib import Path
from typing import Optional

LOG_DIR = Path.home() / ".superset-migrator" / "logs"
_logger: Optional[logging.Logger] = None
_log_file: Optional[Path] = None


def setup_logger() -> logging.Logger:
    """Configura e retorna o logger da aplicação."""
    global _logger, _log_file

    if _logger is not None:
        return _logger

    # Cria diretório de logs
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Nome do arquivo com timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    _log_file = LOG_DIR / f"superset_migrator_{timestamp}.log"

    # Configura logger
    _logger = logging.getLogger("superset_migrator")
    _logger.setLevel(logging.DEBUG)

    # Handler para arquivo
    file_handler = logging.FileHandler(_log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    # Formato detalhado
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)

    _logger.addHandler(file_handler)

    # Log inicial
    _logger.info("=" * 60)
    _logger.info("superset-migrator iniciado")
    _logger.info("=" * 60)

    return _logger


def get_logger() -> logging.Logger:
    """Retorna o logger configurado."""
    global _logger
    if _logger is None:
        return setup_logger()
    return _logger


def get_log_file() -> Optional[Path]:
    """Retorna o caminho do arquivo de log atual."""
    return _log_file


def log_request(method: str, url: str, status_code: int, response_text: str = "", error: str = ""):
    """Loga uma requisição HTTP."""
    logger = get_logger()

    if error:
        logger.error(f"HTTP {method} {url}")
        logger.error(f"  Status: {status_code}")
        logger.error(f"  Erro: {error}")
        if response_text:
            # Limita o tamanho do response no log
            truncated = response_text[:2000] + "..." if len(response_text) > 2000 else response_text
            logger.error(f"  Response: {truncated}")
    else:
        logger.info(f"HTTP {method} {url} -> {status_code}")


def log_action(action: str, details: str = ""):
    """Loga uma ação do usuário."""
    logger = get_logger()
    if details:
        logger.info(f"[AÇÃO] {action}: {details}")
    else:
        logger.info(f"[AÇÃO] {action}")


def log_error(message: str, exception: Exception = None):
    """Loga um erro."""
    logger = get_logger()
    logger.error(f"[ERRO] {message}")
    if exception:
        logger.exception(exception)


def log_debug(message: str):
    """Loga informação de debug."""
    logger = get_logger()
    logger.debug(message)


def log_info(message: str):
    """Loga informação geral."""
    logger = get_logger()
    logger.info(message)


def cleanup_old_logs(keep_days: int = 7):
    """Remove logs mais antigos que keep_days dias."""
    if not LOG_DIR.exists():
        return

    cutoff = datetime.datetime.now() - datetime.timedelta(days=keep_days)

    for log_file in LOG_DIR.glob("superset_migrator_*.log"):
        try:
            # Extrai timestamp do nome do arquivo
            parts = log_file.stem.split("_")
            if len(parts) >= 3:
                date_str = parts[2]  # YYYYMMDD
                file_date = datetime.datetime.strptime(date_str, "%Y%m%d")
                if file_date < cutoff:
                    log_file.unlink()
        except (ValueError, IndexError):
            pass
