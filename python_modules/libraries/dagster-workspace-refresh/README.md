# Dagster workspace refresh

Use deploy-triggered metadata refresh with stock Dagster 1.13.31. The wheel pins the stock packages because it adapts their startup hooks. Code-location containers use their usual commands.

Build the wheel from the fork checkout and install it into the control plane environment.

```sh
uv build python_modules/libraries/dagster-workspace-refresh
uv pip install python_modules/libraries/dagster-workspace-refresh/dist/dagster_workspace_refresh-0.1.0-py3-none-any.whl
```

In a uv project, use `uv add /path/to/dagster_workspace_refresh-0.1.0-py3-none-any.whl` to record it in `pyproject.toml`.

```yaml
load_from:
  - grpc_server:
      location_name: stable
      host: stable.example.com
      port: 443
      ssl: true
      refresh_policy: manual
```

```sh
dagster-refresh-webserver -w workspace.yaml
dagster-refresh-daemon run -w workspace.yaml
```

After the revision is ready, run this against the same shared Dagster run storage as the webserver and daemon.

```sh
dagster-refresh-location -w workspace.yaml stable --revision "$REVISION"
```

Manual locations load once at startup. The webserver and daemon check deployment revisions in shared Dagster storage every five seconds. They fetch definitions when a revision changes. Omit `--revision` to force another refresh, including after a failed refresh. A failed refresh keeps the last working definitions. Locations without the setting keep normal polling. Runs and active schedules or sensors still make their usual requests.
