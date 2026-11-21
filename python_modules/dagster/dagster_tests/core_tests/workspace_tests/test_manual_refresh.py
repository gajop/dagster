# ruff: noqa: SLF001
from collections import Counter
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner
from dagster import instance_for_test
from dagster._cli.proxy_server_manager import ProxyServerManager
from dagster._core.remote_origin import GrpcServerCodeLocationOrigin
from dagster._core.remote_representation.code_location import GrpcServerCodeLocation
from dagster._core.remote_representation.grpc_server_registry import GrpcServerRegistry
from dagster._core.remote_representation.grpc_server_state_subscriber import LocationStateSubscriber
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import WorkspaceFileTarget
from dagster._core.workspace.manual_refresh import (
    ManualRefreshWorkspaceProcessContext,
    load_workspace_locations,
)
from dagster._core.workspace.manual_refresh_cli import refresh_command

WORKSPACE = """\
load_from:
  - grpc_server:
      location_name: stable
      host: stable.example.com
      port: 443
      ssl: true
      refresh_policy: manual
  - grpc_server:
      location_name: changing
      host: changing.example.com
      port: 443
      ssl: true
"""


def test_manual_refresh_keeps_origin_identity(tmp_path: Path):
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(WORKSPACE)
    target = WorkspaceFileTarget([str(workspace)])
    with_policy = load_workspace_locations(target)
    workspace.write_text(WORKSPACE.replace("      refresh_policy: manual\n", ""))
    without_policy = load_workspace_locations(target)
    assert with_policy["stable"].manual_refresh
    assert with_policy["stable"].origin == without_policy["stable"].origin
    assert with_policy["stable"].origin.get_id() == without_policy["stable"].origin.get_id()
    assert not with_policy["changing"].manual_refresh


def test_dev_workspace_preserves_external_location_settings(tmp_path: Path):
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(
        WORKSPACE.replace(
            "      refresh_policy: manual",
            "      refresh_policy: manual\n"
            "      gcp_auth:\n"
            "        audience: https://stable.example.com",
        )
    )
    with (
        instance_for_test() as instance,
        ProxyServerManager(instance, WorkspaceFileTarget([str(workspace)])) as manager,
    ):
        stable, changing = (spec["grpc_server"] for spec in manager.get_code_server_specs())
        assert stable["refresh_policy"] == "manual"
        assert stable["ssl"] is True
        assert stable["gcp_auth"] == {"audience": "https://stable.example.com"}
        assert "refresh_policy" not in changing


def test_manual_locations_skip_polling_and_refresh_once_per_revision(tmp_path: Path):
    workspace = tmp_path / "workspace.yaml"
    workspace.write_text(WORKSPACE)
    target = WorkspaceFileTarget([str(workspace)])
    loads = Counter()
    fail_stable = False

    def create_location(origin, instance):
        loads[origin.location_name] += 1
        if fail_stable and origin.location_name == "stable":
            raise RuntimeError("server unavailable")
        location = mock.MagicMock(spec=GrpcServerCodeLocation)
        location.server_id = f"{origin.location_name}-{loads[origin.location_name]}"
        location.container_image = None
        location.get_display_metadata.return_value = {}
        return location

    def make_context(instance):
        return ManualRefreshWorkspaceProcessContext(
            instance=instance,
            workspace_load_target=target,
            grpc_server_registry=mock.MagicMock(spec=GrpcServerRegistry),
        )

    with (
        instance_for_test() as instance,
        mock.patch.object(
            GrpcServerCodeLocationOrigin,
            "create_location",
            autospec=True,
            side_effect=create_location,
        ),
        mock.patch.object(WorkspaceProcessContext, "_start_watch_thread", autospec=True) as watch,
        mock.patch.object(ManualRefreshWorkspaceProcessContext, "_start_deployment_watcher"),
        make_context(instance) as web,
        make_context(instance) as daemon,
    ):
        old_entry = daemon.get_current_workspace().code_location_entries["stable"]
        events = []
        web.add_state_subscriber(LocationStateSubscriber(events.append))
        daemon.refresh_workspace()
        assert loads == {"stable": 2, "changing": 3}
        assert [call.args[1].location_name for call in watch.call_args_list] == ["changing"] * 3
        assert daemon.get_current_workspace().code_location_entries["stable"] is old_entry
        old_entry.code_location.cleanup.assert_not_called()

        runner = CliRunner()
        arguments = ["-w", str(workspace), "stable", "--revision", "revision-1"]
        result = runner.invoke(refresh_command, arguments)
        assert result.exit_code == 0, result.output
        web._refresh_deployments()
        daemon._refresh_deployments()
        assert loads == {"stable": 4, "changing": 3}
        assert len(events) == 1 and events[0].location_name == "stable"
        updated = web.get_current_workspace().code_location_entries["stable"]
        assert updated.version_key != old_entry.version_key
        assert runner.invoke(refresh_command, arguments).exit_code == 0
        web._refresh_deployments()
        daemon._refresh_deployments()
        assert loads == {"stable": 4, "changing": 3}

        fail_stable = True
        assert runner.invoke(refresh_command, arguments[:-1] + ["revision-2"]).exit_code == 0
        with pytest.warns(UserWarning, match="server unavailable"):
            web._refresh_deployments()
        web._refresh_deployments()
        assert loads["stable"] == 5
        assert web.get_current_workspace().code_location_entries["stable"] is updated
