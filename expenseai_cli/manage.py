"""Custom management commands exposed through Flask's CLI."""
from __future__ import annotations

from datetime import datetime, timedelta

import click
from flask.cli import with_appcontext
from flask_migrate import upgrade

from expenseai_ai import parser_service
from expenseai_auth.services import OrganizationService, UserService
from expenseai_benchmark import service as benchmark_service
from expenseai_ext.db import db
from expenseai_models.invoice import Invoice
from expenseai_models.user import User
from expenseai_risk import orchestrator as risk_orchestrator


@click.group(help="Finvela management commands")
def manage_cli() -> None:
    """Root Click group registered under `flask manage`."""


@manage_cli.command("init-db", help="Apply database migrations")
@with_appcontext
def init_db() -> None:
    """Run pending migrations ensuring the schema is up to date."""
    upgrade()
    click.echo("Database initialized via migrations.")


@manage_cli.command("create-admin", help="Create an administrative user")
@click.option("--email", prompt=True, help="Admin email address")
@click.option("--name", prompt="Full name", help="Admin full name")
@click.option("--organization", prompt="Organization name", help="Organization to associate with the admin")
@with_appcontext
def create_admin(email: str, name: str, organization: str) -> None:
    """Create an admin account and assign the admin role."""
    password = click.prompt("Password", hide_input=True, confirmation_prompt=True)
    role = UserService.ensure_role("admin", description="Platform administrator")
    try:
        user = UserService.create_user(name, email, password, roles=[role.name])
    except ValueError as exc:
        click.secho(str(exc), fg="red")
        return
    try:
        OrganizationService.create_organization(organization, user)
    except ValueError as exc:
        click.secho(str(exc), fg="red")
        return
    click.secho(
        f"Admin user created with id {user.id} for organization '{user.organization.name}'",
        fg="green",
    )


@manage_cli.command("list-users", help="List registered users")
@with_appcontext
def list_users() -> None:
    """Display user accounts and assigned roles."""
    users = User.query.order_by(User.created_at.desc()).all()
    if not users:
        click.echo("No users found.")
        return
    for user in users:
        roles = ", ".join(role.name for role in user.roles) or "(none)"
        click.echo(f"{user.id}: {user.email} - {roles}")


@manage_cli.command("parse-invoice", help="Run Finvela parsing synchronously for a single invoice")
@click.option("--id", "invoice_id", type=int, required=True, help="Invoice identifier")
@with_appcontext
def parse_invoice_cmd(invoice_id: int) -> None:
    """Parse an invoice immediately without waiting for the background worker."""
    try:
        summary = parser_service.parse_invoice_sync(invoice_id, actor="cli")
    except ValueError as exc:
        click.secho(str(exc), fg="red")
        return
    except Exception as exc:  # pragma: no cover - runtime failures only
        click.secho(f"Parsing failed: {exc}", fg="red")
        return

    click.secho(
        f"Invoice {invoice_id} parsed successfully (line items: {summary.get('line_items', 0)}).",
        fg="green",
    )


@manage_cli.command("risk-run", help="Run risk scoring synchronously for a single invoice")
@click.option("--id", "invoice_id", type=int, required=True, help="Invoice identifier")
@with_appcontext
def risk_run_cmd(invoice_id: int) -> None:
    """Execute the composite risk scoring pipeline synchronously."""
    try:
        risk_orchestrator.run_risk_pipeline(invoice_id, actor="cli")
    except ValueError as exc:
        click.secho(str(exc), fg="red")
        return
    except Exception as exc:  # pragma: no cover - runtime only
        click.secho(f"Risk computation failed: {exc}", fg="red")
        return
    click.secho(f"Risk score computed for invoice {invoice_id}.", fg="green")


@manage_cli.command("backfill-history", help="Backfill price history for benchmarking")
@click.option("--days", type=int, default=365, show_default=True, help="Lookback window")
@with_appcontext
def backfill_history_cmd(days: int) -> None:
    """Ingest historical invoice line items into price history."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    invoices = Invoice.query.filter(Invoice.created_at >= cutoff).all()
    processed = 0
    for invoice in invoices:
        benchmark_service.ingest_invoice_line_items(invoice.id)
        processed += 1
    db.session.commit()
    click.secho(f"Benchmark history updated for {processed} invoices.", fg="green")
