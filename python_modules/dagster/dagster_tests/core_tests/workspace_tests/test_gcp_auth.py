from unittest import mock

import pytest
from dagster._check import CheckError
from dagster._core.remote_origin import GrpcServerCodeLocationOrigin
from dagster._core.workspace.load import location_origins_from_config
from dagster._grpc.client import DagsterGrpcClient
from dagster._serdes import deserialize_value, serialize_value

AUDIENCE = "https://private-pipelines.example.run.app"


def test_gcp_auth_workspace_config():
    server: dict[str, object] = {
        "location_name": "private",
        "host": "private-pipelines.example.run.app",
        "port": 443,
        "ssl": True,
    }
    locations = {"load_from": [{"grpc_server": server}]}
    original_origin = location_origins_from_config(locations, "workspace.yaml")["private"]
    assert "gcp_auth_audience" not in serialize_value(original_origin)
    server["gcp_auth"] = False
    assert location_origins_from_config(locations, "workspace.yaml")["private"] == original_origin

    server["gcp_auth"] = True
    origin = location_origins_from_config(locations, "workspace.yaml")["private"]
    assert isinstance(origin, GrpcServerCodeLocationOrigin)
    assert origin.gcp_auth_audience == AUDIENCE
    assert deserialize_value(serialize_value(origin), GrpcServerCodeLocationOrigin) == origin

    server["gcp_auth"] = {"audience": AUDIENCE}
    assert location_origins_from_config(locations, "workspace.yaml")["private"] == origin
    server["host"] = "canary---private-pipelines.example.run.app"
    tagged_origin = location_origins_from_config(locations, "workspace.yaml")["private"]
    assert isinstance(tagged_origin, GrpcServerCodeLocationOrigin)
    assert tagged_origin.gcp_auth_audience == AUDIENCE

    server["ssl"] = False
    with pytest.raises(CheckError, match="requires a TLS gRPC port"):
        location_origins_from_config(locations, "workspace.yaml")


def test_gcp_auth_rejects_existing_bearer_metadata():
    with pytest.raises(CheckError, match="supplies its own authorization"):
        DagsterGrpcClient(
            port=443,
            use_ssl=True,
            metadata=[("authorization", "Bearer existing")],
            gcp_auth_audience=AUDIENCE,
        )


def test_gcp_auth_caches_credentials_and_refreshes_the_token(monkeypatch):
    pytest.importorskip("google.auth")
    from dagster._grpc import gcp_auth
    from google.auth.credentials import TokenState

    credentials = mock.Mock(token_state=TokenState.INVALID, token=None)

    def refresh(request):
        credentials.token = f"token-{credentials.refresh.call_count}"
        credentials.token_state = TokenState.FRESH

    credentials.refresh.side_effect = refresh
    load = mock.Mock(return_value=credentials)
    monkeypatch.setattr(gcp_auth, "fetch_id_token_credentials", load)
    gcp_auth.gcp_id_token_provider.cache_clear()
    try:
        provider = gcp_auth.gcp_id_token_provider(AUDIENCE)
        assert provider is gcp_auth.gcp_id_token_provider(AUDIENCE)
        assert provider.get_token() == provider.get_token() == "token-1"
        credentials.token_state = TokenState.STALE
        assert provider.get_token() == "token-2"
        assert credentials.refresh.call_count == 2
        load.assert_called_once_with(AUDIENCE, request=mock.ANY)
    finally:
        gcp_auth.gcp_id_token_provider.cache_clear()
