from unittest.mock import patch

from dagster._core.workspace.load_target import WorkspaceFileTarget

from dagster_workspace_refresh.manual_refresh import (
    ManualRefreshWorkspaceProcessContext,
    load_workspace_locations,
)


def _create_origins(target: WorkspaceFileTarget):
    # The stock CLIs validate each workspace target before constructing its context.
    return [location.origin for location in load_workspace_locations(target).values()]


def run_webserver() -> None:
    from dagster_webserver import cli

    # Stock CLI fixes the workspace factory at import time. Keep its options and lifecycle.
    with (
        patch.object(WorkspaceFileTarget, "create_origins", _create_origins),
        patch.object(cli, "WorkspaceProcessContext", ManualRefreshWorkspaceProcessContext),
    ):
        cli.main()


def run_daemon() -> None:
    from dagster._daemon import controller
    from dagster._daemon.cli import main

    with (
        patch.object(WorkspaceFileTarget, "create_origins", _create_origins),
        patch.object(controller, "WorkspaceProcessContext", ManualRefreshWorkspaceProcessContext),
    ):
        main()
