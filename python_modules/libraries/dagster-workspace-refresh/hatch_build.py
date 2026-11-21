# ruff: noqa: INP001
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        python_modules = (Path(self.root) / "../..").resolve()
        workspace = python_modules / "dagster/dagster/_core/workspace"
        if not workspace.is_dir():
            # The standalone source distribution already includes the shared files.
            return
        package = "dagster_workspace_refresh"
        sources = (
            workspace / "manual_refresh.py",
            workspace / "manual_refresh_cli.py",
            python_modules / "dagster-webserver/dagster_webserver/workspace_refresh_api.py",
        )
        for source in sources:
            destination = f"{package}/{source.name}"
            if self.target_name == "sdist":
                destination = f"src/{destination}"
            build_data["force_include"][str(source)] = destination
