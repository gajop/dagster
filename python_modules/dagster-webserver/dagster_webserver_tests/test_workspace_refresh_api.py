from pathlib import Path
from unittest import mock

import pytest
from dagster import instance_for_test
from dagster._core.remote_origin import GrpcServerCodeLocationOrigin
from dagster._core.remote_representation.code_location import GrpcServerCodeLocation
from dagster._core.remote_representation.grpc_server_registry import GrpcServerRegistry
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import WorkspaceFileTarget
from dagster._core.workspace.manual_refresh import (
    ManualRefreshWorkspaceProcessContext,
    deployment_cursor_key,
)
from dagster_webserver.app import create_app_from_workspace_process_context
from starlette.testclient import TestClient

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


@pytest.mark.parametrize("read_only", [False, True])
def test_request_metadata_refresh(tmp_path: Path, read_only: bool) -> None:
    path = tmp_path / "workspace.yaml"
    path.write_text(WORKSPACE)
    location = mock.MagicMock(spec=GrpcServerCodeLocation)
    location.get_display_metadata.return_value = {}
    location.server_id = "server"
    location.container_image = None

    with (
        instance_for_test(overrides={"telemetry": {"enabled": False}}) as instance,
        mock.patch.object(
            GrpcServerCodeLocationOrigin, "create_location", return_value=location
        ) as load,
        mock.patch.object(WorkspaceProcessContext, "_start_watch_thread"),
        mock.patch.object(ManualRefreshWorkspaceProcessContext, "_start_deployment_watcher"),
        ManualRefreshWorkspaceProcessContext(
            instance=instance,
            workspace_load_target=WorkspaceFileTarget([str(path)]),
            read_only=read_only,
            grpc_server_registry=mock.MagicMock(spec=GrpcServerRegistry),
        ) as context,
        TestClient(
            create_app_from_workspace_process_context(context, path_prefix="/dagster")
        ) as client,
    ):
        endpoint = "/dagster/api/code-locations/refresh"
        key = deployment_cursor_key(
            context.get_current_workspace().code_location_entries["stable"].origin
        )
        request = {"location_name": "stable", "revision": "release-42"}
        response = client.post(endpoint, json=request)
        if read_only:
            assert response.status_code == 403
            assert instance.daemon_cursor_storage.get_cursor_values({key}) == {}
            return

        assert response.status_code == 202
        assert response.json() == request
        assert client.post(endpoint, json=request).json() == request
        assert instance.daemon_cursor_storage.get_cursor_values({key}) == {key: "release-42"}
        assert load.call_count == 2

        assert client.post(endpoint, json={"location_name": "unknown"}).status_code == 404
        assert client.post(endpoint, json={"location_name": "changing"}).status_code == 409
        assert (
            client.post(endpoint, json={"location_name": "stable", "revision": 42}).status_code
            == 400
        )
        assert client.post(endpoint, data="location_name=stable").status_code == 415
        response = client.post(endpoint, json={"location_name": "stable"})
        assert response.status_code == 202
        assert response.json()["revision"] != "release-42"
