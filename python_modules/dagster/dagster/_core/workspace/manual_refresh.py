"""Refresh selected external code locations from a shared deployment signal."""

import logging
import os
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from dagster_shared.yaml_utils import load_yaml_from_path

import dagster._check as check
from dagster._core.instance import DagsterInstance
from dagster._core.remote_origin import CodeLocationOrigin, GrpcServerCodeLocationOrigin
from dagster._core.remote_representation.grpc_server_registry import GrpcServerRegistry
from dagster._core.remote_representation.grpc_server_state_subscriber import (
    LocationStateChangeEvent,
    LocationStateChangeEventType,
)
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load import location_origins_from_config
from dagster._core.workspace.load_target import (
    CompositeTarget,
    WorkspaceFileTarget,
    WorkspaceLoadTarget,
)
from dagster._core.workspace.workspace import CodeLocationEntry, CurrentWorkspace
from dagster._grpc.constants import GrpcServerCommand

logger = logging.getLogger(__name__)
DEPLOYMENT_CHECK_INTERVAL = 5


@dataclass(frozen=True)
class WorkspaceLocation:
    origin: CodeLocationOrigin
    manual_refresh: bool = False


def load_workspace_locations(
    target: WorkspaceLoadTarget | None,
) -> dict[str, WorkspaceLocation]:
    if isinstance(target, WorkspaceFileTarget):
        locations = {}
        for path in target.paths:
            config = load_yaml_from_path(path)
            check.invariant(
                config is not None,
                f"Could not parse a workspace config from the yaml file at {os.path.abspath(path)}.",
            )
            manual = []
            if isinstance(config, dict) and isinstance(config.get("load_from"), list):
                for entry in config["load_from"]:
                    server = entry.get("grpc_server") if isinstance(entry, dict) else None
                    # Strip the policy before creating origins so it works on stock Dagster and
                    # never becomes part of an origin's serialized identity.
                    policy = (
                        server.pop("refresh_policy", "periodic")
                        if isinstance(server, dict)
                        else "periodic"
                    )
                    if policy not in ("periodic", "manual"):
                        raise ValueError(
                            f"Invalid grpc_server refresh_policy {policy!r} in {path}. "
                            "Use 'periodic' or 'manual'."
                        )
                    manual.append(policy == "manual")
            origins = location_origins_from_config(config, path)
            locations.update(
                (name, WorkspaceLocation(origin, manual_refresh))
                for (name, origin), manual_refresh in zip(origins.items(), manual, strict=True)
            )
        return locations
    if isinstance(target, CompositeTarget):
        return {
            name: location
            for component in target.targets
            for name, location in load_workspace_locations(component).items()
        }
    return {
        origin.location_name: WorkspaceLocation(origin)
        for origin in (target.create_origins() if target else ())
    }


def deployment_cursor_key(origin: CodeLocationOrigin) -> str:
    return f"workspace-metadata-refresh:{origin.get_id()}"


class ManualRefreshWorkspaceProcessContext(WorkspaceProcessContext):
    """Monitor deployment revisions in instance storage instead of polling opted-in servers."""

    def __init__(
        self,
        instance: DagsterInstance,
        workspace_load_target: WorkspaceLoadTarget | None,
        version: str = "",
        read_only: bool = False,
        grpc_server_registry: GrpcServerRegistry | None = None,
        code_server_log_level: str = "INFO",
        server_command: GrpcServerCommand = GrpcServerCommand.API_GRPC,
    ):
        initial_locations = load_workspace_locations(workspace_load_target)
        self._initial_locations: Mapping[str, WorkspaceLocation] | None = initial_locations
        # Serialize periodic workspace updates and explicit refreshes of manual locations.
        # Standard gRPC watch callbacks never take this lock.
        self._manual_refresh_lock = threading.Lock()
        self._manual_origins: dict[str, CodeLocationOrigin] = {}
        self._deployment_revisions: dict[str, str | None] = {}
        self._deployment_shutdown = threading.Event()
        self._deployment_thread: threading.Thread | None = None
        self._set_manual_origins(instance, initial_locations)
        super().__init__(
            instance=instance,
            workspace_load_target=workspace_load_target,
            version=version,
            read_only=read_only,
            grpc_server_registry=grpc_server_registry,
            code_server_log_level=code_server_log_level,
            server_command=server_command,
        )
        self._initial_locations = None
        self._start_deployment_watcher()

    @property
    def _origins(self) -> Sequence[CodeLocationOrigin]:
        locations = self._initial_locations
        if locations is None:
            locations = load_workspace_locations(self.workspace_load_target)
        return [location.origin for location in locations.values()]

    def _set_manual_origins(
        self, instance: DagsterInstance, locations: Mapping[str, WorkspaceLocation]
    ) -> None:
        origins = {
            name: location.origin for name, location in locations.items() if location.manual_refresh
        }
        keys = {deployment_cursor_key(origin) for origin in origins.values()}
        added = keys - self._deployment_revisions.keys()
        # Observe revisions before loading new locations so a deployment during a load is seen.
        initial = instance.daemon_cursor_storage.get_cursor_values(added) if added else {}
        self._deployment_revisions = {
            key: self._deployment_revisions.get(key, initial.get(key)) for key in keys
        }
        self._manual_origins = origins

    def _start_deployment_watcher(self) -> None:
        if not self._manual_origins or self._deployment_thread:
            return
        self._deployment_thread = threading.Thread(
            target=self._watch_deployments,
            name="dagster-metadata-refresh",
            daemon=True,
        )
        self._deployment_thread.start()

    def _watch_deployments(self) -> None:
        while not self._deployment_shutdown.wait(DEPLOYMENT_CHECK_INTERVAL):
            try:
                self._refresh_deployments()
            except Exception:
                logger.exception("Could not check for code-location deployment revisions")

    def _refresh_deployments(self) -> None:
        with self._manual_refresh_lock:
            if not self._deployment_revisions:
                return
            revisions = self.instance.daemon_cursor_storage.get_cursor_values(
                set(self._deployment_revisions)
            )
            for name, origin in self._manual_origins.items():
                if self._deployment_shutdown.is_set():
                    break
                key = deployment_cursor_key(origin)
                revision = revisions.get(key)
                if revision is None or revision == self._deployment_revisions[key]:
                    continue
                entry = self._load_location(origin, reload=False)
                self._deployment_revisions[key] = revision
                with self._lock:
                    previous = self._current_workspace.code_location_entries[name]
                    if entry.code_location or not previous.code_location:
                        self._current_workspace = self._current_workspace.with_code_location(
                            name, entry
                        )
                if entry.code_location:
                    self._send_state_event_to_subscribers(
                        LocationStateChangeEvent(
                            LocationStateChangeEventType.LOCATION_UPDATED,
                            name,
                            "Code-location metadata refreshed for deployment.",
                        )
                    )

    def _location_state_events_handler(self, event: LocationStateChangeEvent) -> None:
        # Manual refresh notifications describe a cache update that already happened.
        if event.location_name not in self._manual_origins:
            super()._location_state_events_handler(event)

    def refresh_workspace(self) -> None:
        self._reload_workspace(reload=False)

    def reload_workspace(self) -> None:
        self._reload_workspace(reload=True)

    def _reload_workspace(self, *, reload: bool) -> None:
        with self._manual_refresh_lock:
            locations = load_workspace_locations(self.workspace_load_target)
            previous = self.get_current_workspace().code_location_entries
            self._set_manual_origins(self.instance, locations)
            updated = {}
            for name, location in locations.items():
                entry = previous.get(name)
                if (
                    not reload
                    and location.manual_refresh
                    and entry
                    and entry.origin == location.origin
                ):
                    updated[name] = entry
                else:
                    updated[name] = self._load_location(location.origin, reload=reload)
            self._update_workspace(updated)
            self._start_deployment_watcher()

    def reload_code_location(self, name: str) -> None:
        with self._manual_refresh_lock:
            super().reload_code_location(name)

    def _start_watch_thread(self, origin: GrpcServerCodeLocationOrigin) -> None:
        if origin.location_name not in self._manual_origins:
            super()._start_watch_thread(origin)

    def _update_workspace(self, new_locations: dict[str, CodeLocationEntry]):
        # A periodic refresh can retain existing entries. The stock method cleans up every
        # old handle, including the ones still used by the new workspace.
        with self._lock:
            previous_events = self._watch_thread_shutdown_events
            self._watch_thread_shutdown_events = {}
            previous_threads = self._watch_threads
            self._watch_threads = {}
            previous_locations = self._current_workspace.code_location_entries
            self._current_workspace = CurrentWorkspace(code_location_entries=new_locations)
            for entry in new_locations.values():
                if isinstance(entry.origin, GrpcServerCodeLocationOrigin):
                    self._start_watch_thread(entry.origin)

        for event in previous_events.values():
            event.set()
        for thread in previous_threads.values():
            thread.join()
        for name, entry in previous_locations.items():
            if entry.code_location and new_locations.get(name) is not entry:
                entry.code_location.cleanup()

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        self._deployment_shutdown.set()
        if self._deployment_thread:
            self._deployment_thread.join()
        super().__exit__(exception_type, exception_value, traceback)
