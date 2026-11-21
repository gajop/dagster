from unittest.mock import patch

from dagster._core.workspace.load_target import WorkspaceFileTarget

from dagster_workspace_refresh.manual_refresh import (
    ManualRefreshWorkspaceProcessContext,
    load_workspace_locations,
)
from dagster_workspace_refresh.workspace_refresh_api import add_workspace_refresh_route


def _create_origins(target: WorkspaceFileTarget):
    # The stock CLIs validate each workspace target before constructing its context.
    return [location.origin for location in load_workspace_locations(target).values()]


def run_webserver() -> None:
    from dagster_webserver import cli

    create_app = cli.create_app_from_workspace_process_context

    def create_app_with_refresh(workspace_process_context, path_prefix="", *args, **kwargs):
        app = create_app(workspace_process_context, path_prefix, *args, **kwargs)
        return add_workspace_refresh_route(app, workspace_process_context, path_prefix)

    # Stock CLI fixes the workspace factory at import time. Keep its options and lifecycle.
    with (
        patch.object(WorkspaceFileTarget, "create_origins", _create_origins),
        patch.object(cli, "WorkspaceProcessContext", ManualRefreshWorkspaceProcessContext),
        patch.object(cli, "create_app_from_workspace_process_context", create_app_with_refresh),
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
