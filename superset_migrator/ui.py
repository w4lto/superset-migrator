"""
Camada de interação com o usuário.

Centraliza todos os prompts interativos (questionary) e a
renderização de painéis/tabelas (rich), mantendo o cli.py limpo.
"""

from __future__ import annotations

from typing import Optional

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.text import Text

from .config import Config, Environment, DBConn, mask_uri
from .client import DashboardInfo
from .transformer import DBInfo

console = Console()


# ── Estilos questionary ────────────────────────────────────────────────────────

STYLE = questionary.Style([
    ("qmark",      "fg:#00d7af bold"),
    ("question",   "bold"),
    ("answer",     "fg:#00d7af bold"),
    ("pointer",    "fg:#00d7af bold"),
    ("highlighted","fg:#00d7af bold"),
    ("selected",   "fg:#00d7af"),
    ("separator",  "fg:#555555"),
    ("instruction","fg:#555555"),
])


def _ask(prompt_obj) -> Optional[str]:
    """Wrapper que trata Ctrl+C graciosamente."""
    try:
        result = prompt_obj.ask()
        if result is None:
            raise KeyboardInterrupt
        return result
    except KeyboardInterrupt:
        console.print("\n[dim]Operação cancelada.[/dim]")
        raise


# ── Header ────────────────────────────────────────────────────────────────────

def print_header():
    console.print(Panel(
        Text("superset-migrator", style="bold cyan", justify="center"),
        subtitle="[dim]Migração de dashboards entre ambientes[/dim]",
        border_style="cyan",
        padding=(0, 4),
    ))
    console.print()


# ── Environments ──────────────────────────────────────────────────────────────

def show_environments(cfg: Config):
    if not cfg.environments:
        console.print("[dim]Nenhum ambiente cadastrado ainda.[/dim]\n")
        return

    table = Table(box=box.ROUNDED, show_header=True, header_style="bold cyan")
    table.add_column("Ambiente", style="bold")
    table.add_column("URL")
    table.add_column("Usuário")
    table.add_column("Função", justify="center")

    src = cfg.source_env()
    for name, env in cfg.environments.items():
        role = "[green]● ORIGEM[/green]" if name == src else "[blue]○ DESTINO[/blue]"
        table.add_row(name, env.url, env.username, role)

    console.print(table)
    console.print()


def prompt_new_environment(cfg: Config) -> Optional[Environment]:
    """Fluxo interativo para cadastrar um novo ambiente."""
    console.print(Panel("[bold]Novo Ambiente[/bold]", border_style="cyan", padding=(0, 2)))

    name = _ask(questionary.text(
        "Nome do ambiente (ex: devel, staging, production):",
        style=STYLE,
        validate=lambda v: True if v.strip() else "Nome não pode ser vazio",
    ))

    url = _ask(questionary.text(
        "URL do Superset (ex: http://superset-devel:8088):",
        style=STYLE,
        validate=lambda v: v.startswith("http") if v else False,
    ))

    username = _ask(questionary.text("Usuário:", style=STYLE, default="admin"))
    password = _ask(questionary.password("Senha:", style=STYLE))

    is_source = False
    if not cfg.source_env() or cfg.is_empty():
        is_source = True
        console.print("[dim]  → Definido como ambiente de ORIGEM (primeiro cadastrado)[/dim]")
    else:
        is_source = _ask(questionary.confirm(
            f"Definir '{name}' como ambiente de ORIGEM (de onde os dashboards são exportados)?",
            default=False,
            style=STYLE,
        ))

    return Environment(
        name=name.strip(),
        url=url.strip(),
        username=username.strip(),
        password=password,
        is_source=is_source,
    )


def prompt_manage_environments(cfg: Config) -> str:
    """Menu de gerenciamento de ambientes. Retorna ação executada."""
    show_environments(cfg)

    menu_options = [("add", "[+] Adicionar ambiente")]
    if cfg.environments:
        menu_options.append(("source", "[⇄] Trocar origem"))
        menu_options.append(("edit", "[✎] Editar ambiente"))
        menu_options.append(("remove", "[-] Remover ambiente"))
    menu_options.append(("back", "← Voltar"))

    choices = [label for _, label in menu_options]
    action_map = {label: action for action, label in menu_options}

    selected = _ask(questionary.select("O que deseja fazer?", choices=choices, style=STYLE))
    action = action_map.get(selected)

    if action == "add":
        env = prompt_new_environment(cfg)
        if env:
            cfg.add_environment(env)
            cfg.save()
            console.print(f"\n[green]✓ Ambiente '[bold]{env.name}[/bold]' cadastrado com sucesso![/green]\n")
        return "added"

    elif action == "source":
        current_source = cfg.source_env()
        env_names = list(cfg.environments.keys())

        # Mostra qual é a origem atual
        choices_with_marker = []
        for name in env_names:
            if name == current_source:
                choices_with_marker.append(f"{name} [atual]")
            else:
                choices_with_marker.append(name)

        new_source = _ask(questionary.select(
            "Selecione o novo ambiente de ORIGEM:",
            choices=choices_with_marker,
            style=STYLE,
        ))

        # Remove o marcador [atual] se presente
        new_source = new_source.replace(" [atual]", "")

        if new_source != current_source:
            # Remove flag de todos e define no novo
            for env in cfg.environments.values():
                env.is_source = False
            cfg.environments[new_source].is_source = True
            cfg.save()
            console.print(f"\n[green]✓ Origem alterada para '[bold]{new_source}[/bold]'[/green]\n")
        else:
            console.print(f"\n[dim]'{new_source}' já é a origem.[/dim]\n")
        return "source_changed"

    elif action == "edit":
        env_names = list(cfg.environments.keys())
        to_edit = _ask(questionary.select(
            "Selecione o ambiente para editar:", choices=env_names, style=STYLE
        ))
        updated_env = prompt_edit_environment(cfg, to_edit)
        if updated_env:
            cfg.add_environment(updated_env)
            cfg.save()
            console.print(f"\n[green]✓ Ambiente '[bold]{updated_env.name}[/bold]' atualizado![/green]\n")
        return "edited"

    elif action == "remove":
        env_names = list(cfg.environments.keys())
        to_remove = _ask(questionary.select(
            "Selecione o ambiente para remover:", choices=env_names, style=STYLE
        ))
        confirmed = _ask(questionary.confirm(
            f"Confirma remoção do ambiente '{to_remove}'?", default=False, style=STYLE
        ))
        if confirmed:
            cfg.remove_environment(to_remove)
            cfg.save()
            console.print(f"[green]✓ Ambiente '{to_remove}' removido.[/green]\n")
        return "removed"

    return "back"


def prompt_edit_environment(cfg: Config, env_name: str) -> Optional[Environment]:
    """Fluxo interativo para editar um ambiente existente."""
    env = cfg.environments.get(env_name)
    if not env:
        return None

    console.print(Panel(f"[bold]Editar Ambiente: {env_name}[/bold]", border_style="cyan", padding=(0, 2)))
    console.print("[dim]Pressione Enter para manter o valor atual[/dim]\n")

    # URL
    new_url = _ask(questionary.text(
        f"URL do Superset [{env.url}]:",
        default=env.url,
        style=STYLE,
        validate=lambda v: v.startswith("http") if v else False,
    ))

    # Usuário
    new_username = _ask(questionary.text(
        f"Usuário [{env.username}]:",
        default=env.username,
        style=STYLE,
    ))

    # Senha
    change_password = _ask(questionary.confirm(
        "Alterar senha?",
        default=False,
        style=STYLE,
    ))
    if change_password:
        new_password = _ask(questionary.password("Nova senha:", style=STYLE))
    else:
        new_password = env.password

    # Definir como origem
    is_source = env.is_source
    if not env.is_source:
        is_source = _ask(questionary.confirm(
            f"Definir '{env_name}' como ambiente de ORIGEM?",
            default=False,
            style=STYLE,
        ))

    return Environment(
        name=env_name,
        url=new_url.strip(),
        username=new_username.strip(),
        password=new_password,
        is_source=is_source,
    )


# ── Database Mappings ─────────────────────────────────────────────────────────

def show_database_mappings(cfg: Config):
    if not cfg.database_mappings:
        console.print("[dim]Nenhum banco cadastrado ainda.[/dim]\n")
        return

    table = Table(box=box.ROUNDED, show_header=True, header_style="bold cyan")
    table.add_column("Banco (nome no Superset)", style="bold")
    for env_name in cfg.environments:
        table.add_column(env_name.capitalize())

    for mapping in cfg.database_mappings:
        row = [mapping.name]
        for env_name in cfg.environments:
            conn = mapping.get_conn(env_name)
            if conn:
                row.append(f"[green]✓[/green] [dim]{mask_uri(conn.sqlalchemy_uri)[:40]}…[/dim]")
            else:
                row.append("[red]✗ não mapeado[/red]")
        table.add_row(*row)

    console.print(table)
    console.print()


def prompt_db_conn_for_env(db_name: str, env_name: str, default_uri: str = "") -> Optional[DBConn]:
    """Pede ao usuário a URI de conexão de um banco para um ambiente específico."""
    console.print(f"\n[bold cyan]Banco:[/bold cyan] {db_name}  →  Ambiente: [bold]{env_name}[/bold]")

    uri = _ask(questionary.text(
        "SQLAlchemy URI:",
        default=default_uri,
        style=STYLE,
        validate=lambda v: True if v.strip() else "URI não pode ser vazia",
    ))
    return DBConn(sqlalchemy_uri=uri.strip())


def prompt_manage_databases(cfg: Config) -> str:
    """Menu de gerenciamento de mapeamentos de banco."""
    show_database_mappings(cfg)

    if not cfg.environments:
        console.print("[yellow]⚠ Cadastre ambientes antes de configurar bancos.[/yellow]\n")
        return "back"

    menu_options = [("add", "[+] Adicionar/editar banco")]
    if cfg.database_mappings:
        menu_options.append(("remove", "[-] Remover banco"))
    menu_options.append(("back", "← Voltar"))

    choices = [label for _, label in menu_options]
    action_map = {label: action for action, label in menu_options}

    selected = _ask(questionary.select("O que deseja fazer?", choices=choices, style=STYLE))
    action = action_map.get(selected)

    if action == "add":
        _prompt_add_or_edit_db(cfg)
        return "added"
    elif action == "remove":
        _prompt_remove_db(cfg)
        return "removed"
    return "back"


def _prompt_add_or_edit_db(cfg: Config):
    """Adiciona ou edita um mapeamento de banco de dados."""
    # Pergunta o nome do banco
    existing = [m.name for m in cfg.database_mappings]
    choices = existing + ["[novo banco]"]
    selected = _ask(questionary.select(
        "Selecione o banco para editar ou adicione um novo:",
        choices=choices,
        style=STYLE,
    ))

    if selected == "[novo banco]":
        db_name = _ask(questionary.text(
            "Nome do banco (deve ser EXATAMENTE igual ao nome no Superset):",
            style=STYLE,
            validate=lambda v: True if v.strip() else "Nome não pode ser vazio",
        ))
    else:
        db_name = selected

    mapping = cfg.get_or_create_mapping(db_name)

    console.print(f"\n[dim]Configure a URI para cada ambiente:[/dim]")

    for env_name in cfg.environments:
        existing_conn = mapping.get_conn(env_name)
        default = existing_conn.sqlalchemy_uri if existing_conn else ""

        conn = prompt_db_conn_for_env(db_name, env_name, default_uri=default)
        if conn:
            mapping.set_conn(env_name, conn)

    cfg.save()
    console.print(f"\n[green]✓ Banco '[bold]{db_name}[/bold]' salvo com sucesso![/green]\n")


def _prompt_remove_db(cfg: Config):
    db_names = [m.name for m in cfg.database_mappings]
    to_remove = _ask(questionary.select(
        "Selecione o banco para remover:", choices=db_names, style=STYLE
    ))
    confirmed = _ask(questionary.confirm(
        f"Confirma remoção do banco '{to_remove}'?", default=False, style=STYLE
    ))
    if confirmed:
        cfg.remove_mapping(to_remove)
        cfg.save()
        console.print(f"[green]✓ Banco '{to_remove}' removido.[/green]\n")


# ── Dashboard search ──────────────────────────────────────────────────────────

def prompt_dashboard_selection(dashboards: list[DashboardInfo]) -> Optional[DashboardInfo]:
    """Exibe lista de dashboards para seleção interativa (único)."""
    if not dashboards:
        console.print("[yellow]Nenhum dashboard encontrado.[/yellow]")
        return None

    options = {d.display_label(): d for d in dashboards}
    selected = _ask(questionary.select(
        "Selecione o dashboard:",
        choices=list(options.keys()),
        style=STYLE,
    ))
    return options[selected]


def prompt_dashboard_multi_selection(dashboards: list[DashboardInfo]) -> list[DashboardInfo]:
    """Exibe lista de dashboards para seleção múltipla."""
    if not dashboards:
        console.print("[yellow]Nenhum dashboard encontrado.[/yellow]")
        return []

    options = {d.display_label(): d for d in dashboards}
    selected = _ask(questionary.checkbox(
        "Selecione os dashboards (espaço para marcar, enter para confirmar):",
        choices=list(options.keys()),
        style=STYLE,
    ))

    if not selected:
        return []

    return [options[label] for label in selected]


# ── Migration flow ────────────────────────────────────────────────────────────

def prompt_target_env(cfg: Config) -> Optional[str]:
    targets = cfg.target_envs()
    if not targets:
        console.print("[red]Nenhum ambiente de destino disponível. Cadastre pelo menos 2 ambientes.[/red]")
        return None
    if len(targets) == 1:
        return targets[0]
    return _ask(questionary.select(
        "Ambiente de destino:", choices=targets, style=STYLE
    ))


def prompt_resolve_missing_dbs(
    missing: list[DBInfo],
    target_env: str,
    cfg: Config,
) -> bool:
    """
    Para cada banco sem mapeamento, pergunta ao usuário se quer
    cadastrar agora. Retorna True se todos foram resolvidos.
    """
    if not missing:
        return True

    console.print(Panel(
        f"[yellow]Os seguintes bancos não têm mapeamento para '[bold]{target_env}[/bold]':[/yellow]",
        border_style="yellow",
        padding=(0, 2),
    ))
    for db in missing:
        console.print(f"  [bold]• {db.db_name}[/bold]")
        console.print(f"    URI atual (origem): [dim]{mask_uri(db.current_uri)}[/dim]")
    console.print()

    resolved = _ask(questionary.confirm(
        "Deseja cadastrar as URIs agora para continuar?",
        default=True,
        style=STYLE,
    ))
    if not resolved:
        return False

    for db in missing:
        mapping = cfg.get_or_create_mapping(db.db_name)
        conn = prompt_db_conn_for_env(db.db_name, target_env, default_uri="")
        if conn:
            mapping.set_conn(target_env, conn)

    cfg.save()
    console.print(f"\n[green]✓ Mapeamentos salvos para uso futuro.[/green]\n")
    return True


# ── Export report ─────────────────────────────────────────────────────────────

def print_export_report(report, output_path: str):
    """Exibe relatório da exportação com substituições de URI."""
    console.print()
    console.print(Panel(
        f"[bold green]ZIP gerado com sucesso[/bold green]  →  ambiente: [bold]{report.target_env}[/bold]",
        border_style="green",
        padding=(0, 2),
    ))

    if report.replaced:
        console.print(f"\n[green]✓ {len(report.replaced)} banco(s) com URI substituída:[/green]")
        for r in report.replaced:
            console.print(f"  [bold]• {r['db_name']}[/bold]")
            console.print(f"    [dim]de:[/dim]  {mask_uri(r['old_uri'])}")
            console.print(f"    [dim]para:[/dim] {mask_uri(r['new_uri'])}")

    if report.not_mapped:
        console.print(f"\n[yellow]⚠ {len(report.not_mapped)} banco(s) SEM mapeamento (URI original mantida):[/yellow]")
        for db in report.not_mapped:
            console.print(f"  [bold]• {db.db_name}[/bold] [dim]({db.file_path})[/dim]")

    console.print(f"\n[cyan]📦 ZIP salvo em:[/cyan] [bold]{output_path}[/bold]")
    console.print(f"[dim]Use 'Enviar ZIP para ambiente' no menu principal para importar.[/dim]")


def print_success_import(target_env: str):
    """Exibe mensagem de sucesso após importação."""
    console.print(f"\n[bold green]✓ Dashboard importado com sucesso no ambiente '{target_env}'![/bold green]")
