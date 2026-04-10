"""Windows desktop launcher for the SECI FDRE-V control room."""

from __future__ import annotations

import argparse
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from typing import Any, Callable, Sequence

from seci_fdre_v_model.runtime import default_windows_workspace_root, resolve_seed_source_config_path
from seci_fdre_v_model.web.app import create_app


def find_free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def build_app_url(host: str, port: int) -> str:
    return f"http://{host}:{port}"


def open_workspace_directory(path: Path) -> None:
    if hasattr(os, "startfile"):
        os.startfile(str(path))  # type: ignore[attr-defined]
        return
    webbrowser.open(path.resolve().as_uri())


def open_app_in_browser(url: str) -> None:
    webbrowser.open(url)


def wait_for_health(url: str, *, timeout: float = 30.0, interval: float = 0.25) -> None:
    deadline = time.monotonic() + timeout
    health_url = f"{url}/api/health"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=1.0) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if payload.get("status") == "ok":
                return
        except (OSError, ValueError, urllib.error.URLError):
            time.sleep(interval)
            continue
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {health_url}")


def _create_waitress_server(app: Any, host: str, port: int) -> Any:
    from waitress.server import create_server

    return create_server(app, host=host, port=port, threads=8)


class WaitressServer:
    def __init__(self, app: Any, *, host: str, port: int) -> None:
        self._app = app
        self._host = host
        self._port = port
        self._server: Any | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._server is not None:
                return
            self._server = _create_waitress_server(self._app, self._host, self._port)
            self._thread = threading.Thread(target=self._server.run, name="waitress-server", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            server = self._server
            thread = self._thread
            self._server = None
            self._thread = None
        if server is None:
            return
        close = getattr(server, "close", None)
        if callable(close):
            close()
        dispatcher = getattr(server, "task_dispatcher", None)
        shutdown = getattr(dispatcher, "shutdown", None)
        if callable(shutdown):
            shutdown()
        if thread is not None:
            thread.join(timeout=5.0)


class DesktopTrayApp:
    def __init__(self, *, app_url: str, workspace_root: Path, server: WaitressServer) -> None:
        self._app_url = app_url
        self._workspace_root = workspace_root
        self._server = server

    def run(self) -> None:
        pystray, menu_item, menu = _load_pystray()
        image = _build_tray_icon_image()
        icon = pystray.Icon(
            "seci-fdre-v",
            image,
            "SECI FDRE V",
            menu=menu(
                menu_item("Open App", self._on_open_app, default=True),
                menu_item("Open Workspace", self._on_open_workspace),
                menu_item("Quit", self._on_quit),
            ),
        )
        icon.run()

    def _on_open_app(self, icon: Any, item: Any) -> None:
        del icon, item
        open_app_in_browser(self._app_url)

    def _on_open_workspace(self, icon: Any, item: Any) -> None:
        del icon, item
        open_workspace_directory(self._workspace_root)

    def _on_quit(self, icon: Any, item: Any) -> None:
        del item
        self._server.stop()
        icon.stop()


def _load_pystray() -> tuple[Any, Any, Any]:
    import pystray

    return pystray, pystray.MenuItem, pystray.Menu


def _build_tray_icon_image() -> Any:
    from PIL import Image, ImageDraw

    image = Image.new("RGBA", (64, 64), "#1f2937")
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((8, 8, 56, 56), radius=12, fill="#0f766e")
    draw.rectangle((18, 18, 46, 46), fill="#f8fafc")
    draw.rectangle((24, 24, 40, 40), fill="#0f766e")
    return image


def launch_desktop_app(
    *,
    workspace_root: str | Path | None = None,
    port: int | None = None,
    open_browser_on_start: bool = True,
    server_factory: Callable[..., WaitressServer] = WaitressServer,
    tray_factory: Callable[..., DesktopTrayApp] = DesktopTrayApp,
    wait_for_health_fn: Callable[..., None] = wait_for_health,
    open_browser_fn: Callable[[str], None] = open_app_in_browser,
) -> int:
    resolved_workspace = (
        Path(workspace_root).expanduser().resolve()
        if workspace_root is not None
        else default_windows_workspace_root()
    )
    resolved_workspace.mkdir(parents=True, exist_ok=True)
    resolved_port = port or find_free_port()
    app_url = build_app_url("127.0.0.1", resolved_port)

    app = create_app(
        workspace_root=resolved_workspace,
        source_config_path=resolve_seed_source_config_path(),
    )
    server = server_factory(app, host="127.0.0.1", port=resolved_port)
    server.start()
    try:
        wait_for_health_fn(app_url)
        if open_browser_on_start:
            open_browser_fn(app_url)
        tray = tray_factory(app_url=app_url, workspace_root=resolved_workspace, server=server)
        tray.run()
    finally:
        server.stop()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the SECI FDRE-V Windows desktop app.")
    parser.add_argument("--workspace", default=None)
    parser.add_argument("--port", default=None, type=int)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(argv)
    return launch_desktop_app(
        workspace_root=args.workspace,
        port=args.port,
        open_browser_on_start=not args.no_browser,
    )


if __name__ == "__main__":
    raise SystemExit(main())
