# ruff: noqa: INP001
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        workspace = (Path(self.root) / "../../dagster/dagster/_core/workspace").resolve()
        if not workspace.is_dir():
            # The standalone source distribution already includes the shared files.
            return
        package = "dagster_workspace_refresh"
        for name in ("manual_refresh.py", "manual_refresh_cli.py"):
            destination = f"{package}/{name}"
            if self.target_name == "sdist":
                destination = f"src/{destination}"
            build_data["force_include"][str(workspace / name)] = destination
