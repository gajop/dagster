from json import JSONDecodeError

from dagster._core.errors import DagsterCodeLocationNotFoundError, DagsterInvalidInvocationError
from dagster._core.workspace.context import IWorkspaceProcessContext
from dagster._core.workspace.permissions import Permissions
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route


def add_workspace_refresh_route(
    app: Starlette, workspace: IWorkspaceProcessContext, path_prefix: str
) -> Starlette:
    request_refresh = getattr(workspace, "request_metadata_refresh", None)
    if request_refresh is None:
        return app

    async def refresh_location(request: Request) -> JSONResponse:
        content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            return JSONResponse({"error": "Set Content-Type to application/json"}, status_code=415)
        try:
            body = await request.json()
        except (JSONDecodeError, UnicodeDecodeError):
            return JSONResponse({"error": "Expected a JSON object"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"error": "Expected a JSON object"}, status_code=400)

        name = body.get("location_name")
        revision = body.get("revision")
        if not isinstance(name, str) or not name:
            return JSONResponse(
                {"error": "location_name must be a non-empty string"}, status_code=400
            )
        if revision is not None and (not isinstance(revision, str) or not revision):
            return JSONResponse({"error": "revision must be a non-empty string"}, status_code=400)

        context = workspace.create_request_context(source=request)
        if not context.has_permission_for_location(Permissions.RELOAD_REPOSITORY_LOCATION, name):
            return JSONResponse(
                {"error": "Code-location reload permission required"}, status_code=403
            )

        try:
            requested_revision = await run_in_threadpool(request_refresh, name, revision)
        except DagsterCodeLocationNotFoundError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        except DagsterInvalidInvocationError as exc:
            return JSONResponse({"error": str(exc)}, status_code=409)
        return JSONResponse(
            {"location_name": name, "revision": requested_revision}, status_code=202
        )

    app.router.routes.insert(
        0, Route(f"{path_prefix}/api/code-locations/refresh", refresh_location, methods=["POST"])
    )
    return app
