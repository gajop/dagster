import uuid

import click

from dagster._core.workspace.load_target import WorkspaceFileTarget

# This module is also built into the standalone package.
from .manual_refresh import deployment_cursor_key, load_workspace_locations  # noqa: TID252


@click.command(name="refresh")
@click.argument("location_name")
@click.option(
    "-w",
    "--workspace",
    type=click.Path(exists=True, dir_okay=False),
    multiple=True,
    default=("workspace.yaml",),
    help="Read code locations from this workspace YAML file.",
)
@click.option("--revision", help="A deployment identity. Omit to force a new refresh request.")
def refresh_command(location_name: str, workspace: tuple[str, ...], revision: str | None) -> None:
    """Request a refresh of a manually refreshed location in the webserver and daemon."""
    from dagster._cli.utils import get_instance_for_cli

    with get_instance_for_cli() as instance:
        locations = load_workspace_locations(WorkspaceFileTarget(paths=workspace))
        location = locations.get(location_name)
        if location is None:
            raise click.ClickException(f"Unknown code location {location_name!r}")
        if not location.manual_refresh:
            raise click.ClickException(
                f"Code location {location_name!r} needs refresh_policy: manual"
            )
        revision = revision or uuid.uuid4().hex
        instance.daemon_cursor_storage.set_cursor_values(
            {deployment_cursor_key(location.origin): revision}
        )
    click.echo(f"Requested metadata refresh for {location_name} at revision {revision}.")
