"""
Orquestrador principal do CLI.

Toda a navegação é feita por menus interativos — nenhum argumento
de linha de comando é necessário para o uso normal. Suporte a
argumentos opcionais para uso em pipelines de CI/CD.
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path
from typing import Optional

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .config import Config
from .client import SupersetClient, DashboardInfo
from .transformer import extract_db_infos, extract_dataset_infos, transform_zip
from . import ui
from . import logger

console = Console()


def _show_log_hint():
    """Mostra dica sobre o arquivo de log."""
    log_file = logger.get_log_file()
    if log_file:
        console.print(f"[dim]📋 Log detalhado: {log_file}[/dim]")


# ─── CLI entry point ──────────────────────────────────────────────────────────

@click.group(invoke_without_command=True)
@click.option("--config", "-c", default=None, help="Caminho alternativo para o config.yaml")
@click.pass_context
def cli(ctx: click.Context, config: Optional[str]):
    """
    superset-migrator — Migração interativa de dashboards do Apache Superset.

    Execute sem subcomandos para o menu interativo principal.
    """
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config

    if ctx.invoked_subcommand is None:
        # Modo interativo: abre o menu principal
        run_interactive_menu(config)


@cli.command("migrate")
@click.option("--slug", "-s", required=True, help="Slug do dashboard (URL customizada no Superset)")
@click.option("--to", "target_env", required=True, help="Ambiente de destino")
@click.option("--push", is_flag=True, default=False, help="Importar automaticamente após gerar o ZIP")
@click.option("--overwrite/--no-overwrite", default=True, help="Sobrescrever dashboard existente (com --push)")
@click.option("--output", "-o", default=".", help="Diretório de saída do ZIP")
@click.pass_context
def cmd_migrate(ctx, slug: str, target_env: str, push: bool, overwrite: bool, output: str):
    """
    Migra um dashboard para outro ambiente (modo não-interativo para CI/CD).

    Exemplo:
        superset-migrator migrate --slug vendas-regiao --to staging --push
    """
    cfg = Config.load()
    _run_migration(cfg, slug=slug, target_env=target_env, push=push, overwrite=overwrite, output_dir=output)


@cli.command("import-zip")
@click.argument("zip_file", type=click.Path(exists=True))
@click.option("--env", "-e", required=True, help="Ambiente de destino")
@click.option("--overwrite/--no-overwrite", default=True, help="Sobrescrever se existir")
@click.option("--sync/--no-sync", default=True, help="Sincronizar colunas dos datasets após importação")
@click.pass_context
def cmd_import(ctx, zip_file: str, env: str, overwrite: bool, sync: bool):
    """Importa um ZIP de dashboard diretamente em um ambiente."""
    cfg = Config.load()
    env_config = cfg.environments.get(env)
    if not env_config:
        console.print(f"[red]Ambiente '{env}' não encontrado.[/red]")
        sys.exit(1)

    zip_bytes = Path(zip_file).read_bytes()
    console.print(f"⬆  Importando em '{env}' ({env_config.url})...")

    try:
        client = SupersetClient(env_config.url, env_config.username, env_config.password)
        client.import_dashboard(zip_bytes, overwrite)
        ui.print_success_import(env)

        # Sincronizar datasets se solicitado
        if sync:
            dataset_infos = extract_dataset_infos(zip_bytes)
            if dataset_infos:
                _sync_datasets(client, dataset_infos)

        client.close()
    except Exception as e:
        console.print(f"[red]Erro: {e}[/red]")
        sys.exit(1)


# ─── Interactive menu ─────────────────────────────────────────────────────────

def run_interactive_menu(config_path: Optional[str] = None):
    ui.print_header()
    cfg = Config.load()

    # Primeira execução: guia o usuário pelo setup
    if cfg.is_empty():
        _first_run_wizard(cfg)
        return

    while True:
        src = cfg.source_env()
        menu_options = [
            ("export", "🚀  Exportar dashboard (gerar ZIP)"),
            ("import", "📤  Enviar ZIP para ambiente"),
            ("list", "📋  Listar dashboards"),
            ("environments", "⚙️   Gerenciar ambientes"),
            ("databases", "🗄️   Gerenciar bancos de dados"),
            ("exit", "❌  Sair"),
        ]
        choices = [label for _, label in menu_options]
        action_map = {label: action for action, label in menu_options}

        try:
            selected = ui._ask(ui.questionary.select(
                f"Menu principal  [origem: {src}]",
                choices=choices,
                style=ui.STYLE,
            ))
        except KeyboardInterrupt:
            console.print("\n[dim]Até logo![/dim]")
            return

        action = action_map.get(selected)
        if action == "export":
            _interactive_export(cfg)
        elif action == "import":
            _interactive_import(cfg)
        elif action == "list":
            _interactive_list(cfg)
        elif action == "environments":
            ui.prompt_manage_environments(cfg)
        elif action == "databases":
            ui.prompt_manage_databases(cfg)
        elif action == "exit":
            console.print("[dim]Até logo![/dim]")
            return


def _first_run_wizard(cfg: Config):
    """Wizard de primeira execução para configurar ambientes."""
    console.print(Panel(
        "Nenhum ambiente configurado. Precisamos cadastrar pelo menos dois ambientes (um de [green]origem[/green] e um de [blue]destino[/blue]):\n",
        border_style="yellow",
        padding=(1, 2),
    ))

    console.print("\n[bold]Ambiente de ORIGEM (onde os dashboards são desenvolvidos):[/bold]")
    env = ui.prompt_new_environment(cfg)
    if env:
        env.is_source = True
        cfg.add_environment(env)
        cfg.save()

    console.print("\n[bold]Ambiente de DESTINO:[/bold]")
    env2 = ui.prompt_new_environment(cfg)
    if env2:
        env2.is_source = False
        cfg.add_environment(env2)
        cfg.save()

    console.print("\n[green]✓ Configuração inicial concluída![/green]")
    console.print("[dim]Use 'Gerenciar ambientes' para adicionar mais ambientes depois.[/dim]\n")

    continue_ = ui._ask(ui.questionary.confirm(
        "Deseja exportar um dashboard agora?", default=True, style=ui.STYLE
    ))
    if continue_:
        _interactive_export(cfg)


# ─── Export flow (principal) ───────────────────────────────────────────────────

def _interactive_export(cfg: Config):
    """Fluxo interativo para exportar dashboards e gerar ZIPs ajustados."""
    if not cfg.environments:
        console.print("[red]Nenhum ambiente configurado.[/red]")
        return

    src_name = cfg.source_env()
    src_env = cfg.environments[src_name]

    # 1. Conecta ao ambiente de origem
    console.print(f"\n[dim]Conectando ao ambiente '{src_name}' ({src_env.url})...[/dim]")

    try:
        src_client = SupersetClient(src_env.url, src_env.username, src_env.password)
    except Exception as e:
        console.print(f"[red]Erro ao conectar: {e}[/red]")
        return

    # 2. Escolha do(s) dashboard(s)
    search_options = [
        ("list", "📋  Listar todos e selecionar"),
        ("title", "🔎  Buscar por título"),
        ("slug", "🔍  Buscar por slug (único)"),
    ]
    choices = [label for _, label in search_options]
    search_map = {label: key for key, label in search_options}

    selected = ui._ask(ui.questionary.select(
        "Como deseja localizar os dashboards?",
        choices=choices,
        style=ui.STYLE,
    ))
    search_mode = search_map.get(selected)

    dashboards_to_export: list[DashboardInfo] = []

    try:
        if search_mode == "slug":
            # Slug é busca única
            slug = ui._ask(ui.questionary.text(
                "Slug do dashboard:",
                style=ui.STYLE,
                validate=lambda v: True if v.strip() else "Slug não pode ser vazio",
            ))
            with console.status(f"Buscando dashboard com slug '{slug}'..."):
                dashboard = src_client.get_dashboard_by_slug(slug.strip())
            console.print(f"[green]✓ Dashboard encontrado:[/green] {dashboard.title} (ID: {dashboard.id})")
            dashboards_to_export = [dashboard]

        elif search_mode == "list":
            with console.status("Carregando dashboards..."):
                all_dashboards = src_client.list_dashboards()
            dashboards_to_export = ui.prompt_dashboard_multi_selection(all_dashboards)

        else:  # title
            term = ui._ask(ui.questionary.text("Termo de busca:", style=ui.STYLE))
            with console.status(f"Buscando '{term}'..."):
                found_dashboards = src_client.list_dashboards(search=term)
            dashboards_to_export = ui.prompt_dashboard_multi_selection(found_dashboards)

    except Exception as e:
        console.print(f"[red]Erro: {e}[/red]")
        return

    if not dashboards_to_export:
        console.print("[yellow]Nenhum dashboard selecionado.[/yellow]")
        return

    console.print(f"\n[cyan]📦 {len(dashboards_to_export)} dashboard(s) selecionado(s)[/cyan]")

    # 3. Ambiente de destino
    target_env = ui.prompt_target_env(cfg)
    if not target_env:
        return

    # 4. Gera os ZIPs
    _run_batch_export(
        cfg,
        dashboards=dashboards_to_export,
        target_env=target_env,
        src_client=src_client,
    )


def _run_batch_export(
    cfg: Config,
    dashboards: list[DashboardInfo],
    target_env: str,
    src_client: SupersetClient,
    output_dir: str = ".",
):
    """Exporta múltiplos dashboards em batch."""
    src_name = cfg.source_env()
    total = len(dashboards)
    success = 0
    failed = []
    output_files = []

    # Primeiro, coleta todos os bancos que precisam de mapeamento
    console.print(f"\n[dim]Verificando mapeamentos de banco...[/dim]")
    all_missing_dbs = set()

    for dash in dashboards:
        try:
            with console.status(f"Analisando '{dash.title}'..."):
                zip_bytes = src_client.export_dashboard(dash.id)
            db_infos = extract_db_infos(zip_bytes)
            for db in db_infos:
                mapping = cfg.get_mapping(db.db_name)
                if not mapping or not mapping.get_conn(target_env):
                    all_missing_dbs.add((db.db_name, db.current_uri))
        except Exception as e:
            console.print(f"  [red]✗[/red] {dash.title}: {e}")
            failed.append(dash.title)

    # Resolve mapeamentos faltantes de uma vez
    if all_missing_dbs:
        from .transformer import DBInfo
        missing_list = [
            DBInfo(file_path="", db_name=name, current_uri=uri)
            for name, uri in all_missing_dbs
        ]
        resolved = ui.prompt_resolve_missing_dbs(missing_list, target_env, cfg)
        if not resolved:
            console.print("[yellow]Exportação cancelada.[/yellow]")
            return

    # Agora exporta cada dashboard
    console.print(f"\n[bold]Exportando {total} dashboard(s) para '{target_env}':[/bold]\n")

    for i, dash in enumerate(dashboards, 1):
        if dash.title in failed:
            continue

        console.print(f"[dim]({i}/{total})[/dim] {dash.title}")

        try:
            # Exporta
            with console.status("  Exportando..."):
                zip_bytes = src_client.export_dashboard(dash.id)

            # Transforma
            with console.status("  Transformando..."):
                new_zip, report = transform_zip(zip_bytes, target_env, cfg)

            # Salva
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            slug_part = f"_{dash.slug}" if dash.slug else ""
            filename = f"dashboard_{dash.id}{slug_part}_{target_env}_{timestamp}.zip"
            output_path = str(Path(output_dir) / filename)
            Path(output_path).write_bytes(new_zip)

            console.print(f"  [green]✓[/green] {output_path}")
            output_files.append(output_path)
            success += 1

        except Exception as e:
            console.print(f"  [red]✗[/red] Erro: {e}")
            failed.append(dash.title)

    # Resumo final
    console.print()
    if success == total:
        console.print(Panel(
            f"[bold green]✓ {success} dashboard(s) exportado(s) com sucesso![/bold green]",
            border_style="green",
            padding=(0, 2),
        ))
    elif success > 0:
        console.print(Panel(
            f"[yellow]⚠ {success}/{total} dashboard(s) exportado(s)[/yellow]\n"
            f"Falhas: {', '.join(failed)}",
            border_style="yellow",
            padding=(0, 2),
        ))
    else:
        console.print(Panel(
            f"[red]✗ Nenhum dashboard exportado[/red]",
            border_style="red",
            padding=(0, 2),
        ))

    if output_files:
        console.print(f"\n[dim]Use 'Enviar ZIP para ambiente' para importar os arquivos gerados.[/dim]")
    console.print()


# ─── Import flow (secundário) ──────────────────────────────────────────────────

def _interactive_import(cfg: Config):
    """Fluxo interativo para enviar ZIPs para um ambiente."""
    if not cfg.environments:
        console.print("[red]Nenhum ambiente configurado.[/red]")
        return

    # 1. Modo de seleção
    mode_options = [
        ("single", "📄  Importar um único arquivo"),
        ("multiple", "📁  Importar múltiplos arquivos de um diretório"),
    ]
    choices = [label for _, label in mode_options]
    mode_map = {label: key for key, label in mode_options}

    selected = ui._ask(ui.questionary.select(
        "Modo de importação:",
        choices=choices,
        style=ui.STYLE,
    ))
    mode = mode_map.get(selected)

    zip_files: list[Path] = []

    if mode == "single":
        # Selecionar arquivo único
        zip_path = ui._ask(ui.questionary.path(
            "Caminho do arquivo ZIP:",
            style=ui.STYLE,
            validate=lambda v: True if (v and Path(v).exists() and v.endswith(".zip")) else "Arquivo não encontrado ou não é um ZIP",
        ))
        if not zip_path or not Path(zip_path).exists():
            console.print("[red]Arquivo não encontrado.[/red]")
            return
        zip_files = [Path(zip_path)]

    else:  # multiple
        # Selecionar diretório e listar ZIPs
        dir_path = ui._ask(ui.questionary.path(
            "Diretório com os arquivos ZIP:",
            style=ui.STYLE,
            only_directories=True,
            validate=lambda v: True if (v and Path(v).is_dir()) else "Diretório não encontrado",
        ))
        if not dir_path or not Path(dir_path).is_dir():
            console.print("[red]Diretório não encontrado.[/red]")
            return

        # Lista ZIPs no diretório
        all_zips = sorted(Path(dir_path).glob("*.zip"))
        if not all_zips:
            console.print("[yellow]Nenhum arquivo ZIP encontrado no diretório.[/yellow]")
            return

        # Permite selecionar quais importar
        zip_options = [f.name for f in all_zips]
        selected_files = ui._ask(ui.questionary.checkbox(
            f"Selecione os arquivos ({len(all_zips)} encontrados):",
            choices=zip_options,
            style=ui.STYLE,
        ))

        if not selected_files:
            console.print("[yellow]Nenhum arquivo selecionado.[/yellow]")
            return

        zip_files = [Path(dir_path) / name for name in selected_files]

    console.print(f"\n[cyan]📦 {len(zip_files)} arquivo(s) selecionado(s)[/cyan]")

    # 2. Selecionar ambiente de destino
    env_names = list(cfg.environments.keys())
    target_env = ui._ask(ui.questionary.select(
        "Ambiente de destino:",
        choices=env_names,
        style=ui.STYLE,
    ))

    env_config = cfg.environments[target_env]

    # 3. Confirmar sobrescrita
    overwrite = ui._ask(ui.questionary.confirm(
        "Sobrescrever dashboards se já existirem?",
        default=True,
        style=ui.STYLE,
    ))

    # 4. Conectar ao destino
    try:
        client = SupersetClient(env_config.url, env_config.username, env_config.password)
    except Exception as e:
        console.print(f"[red]Erro ao conectar: {e}[/red]")
        return

    # 5. Verificação prévia - bancos de dados
    console.print(f"\n[dim]Verificando pré-requisitos no ambiente '{target_env}'...[/dim]")

    if not _verify_databases_exist(client, zip_files, cfg, target_env):
        client.close()
        return

    # Obtém mapeamento de UUIDs dos bancos no destino
    console.print(f"[dim]Obtendo UUIDs dos bancos no destino...[/dim]")
    try:
        db_uuid_map = client.get_database_uuid_map()
        logger.log_info(f"Mapeamento de UUIDs: {len(db_uuid_map)} banco(s)")
    except Exception as e:
        logger.log_error(f"Erro ao obter UUIDs: {e}")
        db_uuid_map = {}

    console.print(f"\n[bold]Importando {len(zip_files)} arquivo(s) para '{target_env}':[/bold]\n")

    success = 0
    failed = []
    all_datasets = []

    for i, zip_file in enumerate(zip_files, 1):
        console.print(f"[dim]({i}/{len(zip_files)})[/dim] {zip_file.name}")

        try:
            zip_bytes = zip_file.read_bytes()

            # Transforma o ZIP (substitui URIs e UUIDs de banco) antes de importar
            with console.status("  Transformando..."):
                transformed_zip, report = transform_zip(zip_bytes, target_env, cfg, db_uuid_map)

            if report.replaced:
                for r in report.replaced:
                    console.print(f"  [dim]↳ {r['db_name']}: URI substituída[/dim]")

            if report.not_mapped:
                for nm in report.not_mapped:
                    console.print(f"  [yellow]⚠ {nm.db_name}: sem mapeamento (URI original mantida)[/yellow]")

            with console.status("  Importando..."):
                client.import_dashboard(transformed_zip, overwrite)
            console.print(f"  [green]✓[/green] Importado")
            success += 1

            # Coleta datasets para sincronização
            dataset_infos = extract_dataset_infos(transformed_zip)
            all_datasets.extend(dataset_infos)

        except Exception as e:
            logger.log_error(f"Falha ao importar {zip_file.name}: {e}")
            console.print(f"  [red]✗[/red] Erro: {e}")
            failed.append(zip_file.name)

    # Resumo
    console.print()
    if success == len(zip_files):
        console.print(f"[bold green]✓ {success} dashboard(s) importado(s) com sucesso![/bold green]")
    elif success > 0:
        console.print(f"[yellow]⚠ {success}/{len(zip_files)} importado(s)[/yellow]")
        _show_log_hint()
    else:
        console.print(f"[red]✗ Nenhum dashboard importado[/red]")
        _show_log_hint()
        client.close()
        return

    # 5. Sincronizar datasets (todos de uma vez)
    if all_datasets:
        # Remove duplicados
        unique_datasets = {(d.table_name, d.database_name): d for d in all_datasets}
        unique_list = list(unique_datasets.values())

        sync = ui._ask(ui.questionary.confirm(
            f"Sincronizar colunas dos {len(unique_list)} dataset(s)?",
            default=True,
            style=ui.STYLE,
        ))

        if sync:
            _sync_datasets(client, unique_list)

    client.close()
    console.print()


def _verify_databases_exist(client: SupersetClient, zip_files: list[Path], cfg: Config, target_env: str) -> bool:
    """
    Verifica se os bancos de dados referenciados nos ZIPs existem no ambiente de destino.
    Oferece criar automaticamente os bancos faltantes se houver mapeamento.
    Retorna True se pode continuar, False para abortar.
    """
    # Coleta todos os bancos referenciados nos ZIPs
    required_dbs: set[str] = set()

    for zip_file in zip_files:
        try:
            zip_bytes = zip_file.read_bytes()
            db_infos = extract_db_infos(zip_bytes)
            for db in db_infos:
                required_dbs.add(db.db_name)
        except Exception as e:
            logger.log_error(f"Erro ao analisar {zip_file.name}: {e}")
            console.print(f"[yellow]⚠ Não foi possível analisar {zip_file.name}[/yellow]")

    if not required_dbs:
        # Nenhum banco encontrado nos ZIPs, pode continuar
        return True

    # Obtém bancos disponíveis no destino
    try:
        available_dbs = client.get_database_names()
    except Exception as e:
        logger.log_error(f"Erro ao listar bancos no destino: {e}")
        console.print(f"[yellow]⚠ Não foi possível verificar bancos no destino: {e}[/yellow]")
        # Continua mesmo assim - deixa o Superset dar o erro
        return True

    # Verifica quais estão faltando
    missing_dbs = required_dbs - available_dbs

    if not missing_dbs:
        console.print(f"[green]✓[/green] Todos os {len(required_dbs)} banco(s) de dados encontrados no destino")
        return True

    # Verifica quais bancos faltantes têm mapeamento no config
    can_create = []  # (db_name, uri)
    cannot_create = []  # db_name

    for db_name in missing_dbs:
        mapping = cfg.get_mapping(db_name)
        if mapping:
            conn = mapping.get_conn(target_env)
            if conn and conn.sqlalchemy_uri:
                can_create.append((db_name, conn.sqlalchemy_uri))
                continue
        cannot_create.append(db_name)

    # Exibe bancos faltantes
    console.print()
    console.print(Panel(
        f"[bold yellow]⚠ {len(missing_dbs)} banco(s) de dados não encontrado(s) no destino[/bold yellow]",
        border_style="yellow",
        padding=(0, 2),
    ))

    console.print("\n[bold]Bancos necessários:[/bold]")
    for db_name in sorted(required_dbs):
        if db_name in missing_dbs:
            if any(name == db_name for name, _ in can_create):
                console.print(f"  [yellow]○[/yellow] {db_name} [dim](não cadastrado, mas há URI mapeada)[/dim]")
            else:
                console.print(f"  [red]✗[/red] {db_name} [dim](não cadastrado, sem mapeamento)[/dim]")
        else:
            console.print(f"  [green]✓[/green] {db_name}")

    # Se há bancos que podemos criar, oferece essa opção
    if can_create:
        console.print()
        console.print(f"[cyan]ℹ {len(can_create)} banco(s) podem ser criados automaticamente usando as URIs mapeadas.[/cyan]")

        create_choice = ui._ask(ui.questionary.select(
            "O que deseja fazer?",
            choices=[
                "[+] Criar bancos automaticamente e continuar",
                "[→] Continuar sem criar (importação pode falhar)",
                "[✗] Cancelar importação",
            ],
            style=ui.STYLE,
        ))

        if "[✗]" in create_choice:
            return False

        if "[+]" in create_choice:
            # Cria os bancos automaticamente
            console.print()
            created = 0
            for db_name, uri in can_create:
                console.print(f"  Criando '{db_name}'...", end=" ")
                try:
                    result = client.create_database(db_name, uri)
                    if result.get("already_exists"):
                        console.print("[cyan]já existe[/cyan]")
                    else:
                        console.print("[green]✓[/green]")
                    created += 1
                except Exception as e:
                    logger.log_error(f"Falha ao criar banco '{db_name}': {e}")
                    console.print(f"[red]✗[/red] {e}")

            if created == len(can_create):
                console.print(f"\n[green]✓ {created} banco(s) criado(s) com sucesso![/green]")
            elif created > 0:
                console.print(f"\n[yellow]⚠ {created}/{len(can_create)} banco(s) criado(s)[/yellow]")

            # Se ainda há bancos que não pudemos criar, avisa
            if cannot_create:
                console.print(f"\n[yellow]⚠ {len(cannot_create)} banco(s) ainda precisam ser criados manualmente:[/yellow]")
                for db_name in cannot_create:
                    console.print(f"  • {db_name}")

                continue_anyway = ui._ask(ui.questionary.confirm(
                    "Deseja continuar mesmo assim?",
                    default=False,
                    style=ui.STYLE,
                ))
                return continue_anyway

            return True

        # Usuário escolheu continuar sem criar
        return True

    # Não há bancos que possamos criar - comportamento original
    console.print()
    console.print("[dim]A importação provavelmente falhará se esses bancos não existirem no Superset de destino.[/dim]")
    console.print("[dim]Cadastre os bancos manualmente no Superset ou configure os mapeamentos de URI.[/dim]\n")

    # Pergunta se quer continuar mesmo assim
    continue_anyway = ui._ask(ui.questionary.confirm(
        "Deseja tentar importar mesmo assim?",
        default=False,
        style=ui.STYLE,
    ))

    return continue_anyway


def _sync_datasets(client: SupersetClient, dataset_infos: list):
    """Sincroniza colunas dos datasets após importação."""
    console.print(f"\n[dim]🔄 Sincronizando {len(dataset_infos)} dataset(s)...[/dim]")

    success = 0
    skipped = 0
    failed = []

    for ds_info in dataset_infos:
        try:
            # Busca o dataset pelo nome
            dataset = client.get_dataset_by_name(ds_info.table_name, ds_info.database_name)

            if dataset:
                dataset_id = dataset.get("id")
                with console.status(f"Sincronizando '{ds_info.table_name}'..."):
                    if client.sync_dataset_columns(dataset_id):
                        console.print(f"  [green]✓[/green] {ds_info.table_name}")
                        success += 1
                    else:
                        console.print(f"  [yellow]⚠[/yellow] {ds_info.table_name} [dim](sincronização não necessária ou já atualizado)[/dim]")
                        skipped += 1
            else:
                logger.log_debug(f"Dataset '{ds_info.table_name}' (db='{ds_info.database_name}') não encontrado no destino")
                console.print(f"  [dim]–[/dim] {ds_info.table_name} [dim](dataset não encontrado no banco '{ds_info.database_name}', pode já estar sincronizado)[/dim]")
                skipped += 1

        except Exception as e:
            logger.log_error(f"Erro ao sincronizar {ds_info.table_name}: {e}")
            console.print(f"  [yellow]⚠[/yellow] {ds_info.table_name} [dim](erro: {str(e)[:50]})[/dim]")
            skipped += 1

    total = len(dataset_infos)
    if success == total:
        console.print(f"\n[green]✓ Todos os {success} dataset(s) sincronizados![/green]")
    elif success > 0:
        console.print(f"\n[green]✓ {success}/{total} dataset(s) sincronizado(s)[/green]")
        if skipped > 0:
            console.print(f"[dim]  ({skipped} ignorado(s) ou já atualizados)[/dim]")
    else:
        console.print(f"\n[dim]Nenhum dataset precisou de sincronização.[/dim]")


def _interactive_list(cfg: Config):
    """Lista dashboards do ambiente de origem."""
    src_name = cfg.source_env()
    src_env = cfg.environments.get(src_name)
    if not src_env:
        console.print("[red]Nenhum ambiente de origem configurado.[/red]")
        return

    search = ui._ask(ui.questionary.text(
        "Filtrar por título/slug (deixe em branco para listar todos):",
        default="",
        style=ui.STYLE,
    ))

    try:
        with console.status(f"Conectando a '{src_name}'..."):
            client = SupersetClient(src_env.url, src_env.username, src_env.password)
            dashboards = client.list_dashboards(search=search)
            client.close()
    except Exception as e:
        console.print(f"[red]Erro: {e}[/red]")
        return

    if not dashboards:
        console.print("[yellow]Nenhum dashboard encontrado.[/yellow]")
        return

    table = Table(box=box.ROUNDED, header_style="bold cyan")
    table.add_column("ID", justify="right")
    table.add_column("Título", style="bold")
    table.add_column("Slug", style="cyan")
    table.add_column("Status")

    for d in dashboards:
        table.add_row(str(d.id), d.title, d.slug or "[dim]—[/dim]", d.status)

    console.print(f"\n[bold]{len(dashboards)} dashboard(s) em '{src_name}':[/bold]\n")
    console.print(table)
    console.print()


# ─── Core export logic ─────────────────────────────────────────────────────────

def _run_export(
    cfg: Config,
    target_env: str,
    dashboard_id: int,
    output_dir: str = ".",
    src_client: Optional[SupersetClient] = None,
):
    """
    Exporta um dashboard e gera ZIP ajustado para o ambiente alvo.
    Usado pelo modo interativo.
    """
    src_name = cfg.source_env()
    src_env_cfg = cfg.environments.get(src_name)

    if not src_env_cfg:
        console.print(f"[red]Ambiente de origem '{src_name}' não encontrado.[/red]")
        return

    # Conecta no ambiente de origem se necessário
    if src_client is None:
        try:
            with console.status(f"Conectando a '{src_name}'..."):
                src_client = SupersetClient(src_env_cfg.url, src_env_cfg.username, src_env_cfg.password)
        except Exception as e:
            console.print(f"[red]Erro ao conectar em '{src_name}': {e}[/red]")
            return

    # 1. Exporta o ZIP do ambiente de origem
    console.print(f"\n[dim]⬇  Exportando dashboard {dashboard_id} de '{src_name}'...[/dim]")
    try:
        with console.status("Exportando..."):
            zip_bytes = src_client.export_dashboard(dashboard_id)
        console.print(f"   [green]✓[/green] ZIP exportado ({len(zip_bytes) // 1024} KB)")
    except Exception as e:
        console.print(f"[red]Erro ao exportar: {e}[/red]")
        return

    # 2. Inspeciona bancos presentes no ZIP
    db_infos = extract_db_infos(zip_bytes)

    # 3. Verifica quais bancos não têm mapeamento para o ambiente alvo
    missing = [
        db for db in db_infos
        if not (cfg.get_mapping(db.db_name) and cfg.get_mapping(db.db_name).get_conn(target_env))
    ]

    if missing:
        resolved = ui.prompt_resolve_missing_dbs(missing, target_env, cfg)
        if not resolved:
            console.print("[yellow]Exportação cancelada.[/yellow]")
            return

    # 4. Transforma o ZIP
    console.print(f"\n[dim]🔄 Substituindo conexões para '{target_env}'...[/dim]")
    try:
        new_zip, report = transform_zip(zip_bytes, target_env, cfg)
    except Exception as e:
        console.print(f"[red]Erro ao transformar ZIP: {e}[/red]")
        return

    # 5. Salva o ZIP
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dashboard_{dashboard_id}_{target_env}_{timestamp}.zip"
    output_path = str(Path(output_dir) / filename)

    Path(output_path).write_bytes(new_zip)
    ui.print_export_report(report, output_path)
    console.print()


# ─── CI/CD migration (with optional push) ──────────────────────────────────────

def _run_migration(
    cfg: Config,
    target_env: str,
    slug: str = "",
    dashboard_id: Optional[int] = None,
    push: bool = False,
    overwrite: bool = True,
    output_dir: str = ".",
):
    """
    Migração completa para CI/CD — exporta, transforma e opcionalmente faz push.
    """
    src_name = cfg.source_env()
    src_env_cfg = cfg.environments.get(src_name)
    target_env_cfg = cfg.environments.get(target_env)

    if not src_env_cfg:
        console.print(f"[red]Ambiente de origem '{src_name}' não encontrado.[/red]")
        return
    if not target_env_cfg:
        console.print(f"[red]Ambiente '{target_env}' não encontrado.[/red]")
        return

    # Conecta no ambiente de origem
    try:
        with console.status(f"Conectando a '{src_name}'..."):
            src_client = SupersetClient(src_env_cfg.url, src_env_cfg.username, src_env_cfg.password)
    except Exception as e:
        console.print(f"[red]Erro ao conectar em '{src_name}': {e}[/red]")
        return

    # Resolve dashboard por slug se necessário
    if dashboard_id is None and slug:
        try:
            with console.status(f"Buscando dashboard '{slug}'..."):
                info = src_client.get_dashboard_by_slug(slug)
            dashboard_id = info.id
            console.print(f"[green]✓[/green] Dashboard: {info.title} (ID: {dashboard_id})")
        except Exception as e:
            console.print(f"[red]{e}[/red]")
            return

    # 1. Exporta o ZIP do ambiente de origem
    console.print(f"\n[dim]⬇  Exportando dashboard {dashboard_id} de '{src_name}'...[/dim]")
    try:
        with console.status("Exportando..."):
            zip_bytes = src_client.export_dashboard(dashboard_id)
        console.print(f"   [green]✓[/green] ZIP exportado ({len(zip_bytes) // 1024} KB)")
    except Exception as e:
        console.print(f"[red]Erro ao exportar: {e}[/red]")
        return

    # 2. Inspeciona bancos presentes no ZIP
    db_infos = extract_db_infos(zip_bytes)

    # 3. Verifica quais bancos não têm mapeamento para o ambiente alvo
    missing = [
        db for db in db_infos
        if not (cfg.get_mapping(db.db_name) and cfg.get_mapping(db.db_name).get_conn(target_env))
    ]

    if missing:
        console.print(f"[yellow]⚠ {len(missing)} banco(s) sem mapeamento para '{target_env}'. URI original será mantida.[/yellow]")
        for db in missing:
            console.print(f"  • {db.db_name}")

    # 4. Transforma o ZIP
    console.print(f"\n[dim]🔄 Substituindo conexões para '{target_env}'...[/dim]")
    try:
        new_zip, report = transform_zip(zip_bytes, target_env, cfg)
    except Exception as e:
        console.print(f"[red]Erro ao transformar ZIP: {e}[/red]")
        return

    # 5. Salva o ZIP
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dashboard_{dashboard_id}_{target_env}_{timestamp}.zip"
    output_path = str(Path(output_dir) / filename)

    Path(output_path).write_bytes(new_zip)
    ui.print_export_report(report, output_path)

    # 6. Push (apenas se solicitado via flag)
    if push:
        console.print(f"\n[dim]⬆  Importando em '{target_env}' ({target_env_cfg.url})...[/dim]")
        try:
            with console.status("Importando..."), \
                 SupersetClient(target_env_cfg.url, target_env_cfg.username, target_env_cfg.password) as target_client:
                target_client.import_dashboard(new_zip, overwrite)
            ui.print_success_import(target_env)
        except Exception as e:
            console.print(f"[red]Erro ao importar: {e}[/red]")
            console.print(f"[dim]O ZIP foi salvo e pode ser importado manualmente: {output_path}[/dim]")

    console.print()


def run():
    """Entry point principal."""
    # Inicializa o logger
    logger.setup_logger()
    logger.cleanup_old_logs(keep_days=7)

    try:
        cli(standalone_mode=True)
    except SystemExit as e:
        if e.code != 0:
            raise
    except Exception as e:
        logger.log_error(f"Erro não tratado: {e}", e)
        log_file = logger.get_log_file()
        console.print(f"\n[red]Erro: {e}[/red]")
        if log_file:
            console.print(f"[dim]Log salvo em: {log_file}[/dim]")
        raise
