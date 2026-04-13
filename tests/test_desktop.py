from __future__ import annotations

from pathlib import Path

from seci_fdre_v_model import desktop, runtime


def test_resolve_seed_source_config_path_prefers_frozen_bundle(tmp_path: Path, monkeypatch) -> None:
    bundled_config = tmp_path / "config" / "project.yaml"
    bundled_config.parent.mkdir(parents=True)
    bundled_config.write_text("project: {}\n", encoding="utf-8")

    monkeypatch.setattr(runtime.sys, "_MEIPASS", str(tmp_path), raising=False)

    assert runtime.resolve_seed_source_config_path() == bundled_config.resolve()


def test_default_windows_workspace_root_uses_localappdata(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    assert runtime.default_windows_workspace_root() == (tmp_path / "SECI FDRE V").resolve()


def test_launch_desktop_app_opens_browser_after_health_success(tmp_path: Path, monkeypatch) -> None:
    source_config = tmp_path / "project.yaml"
    source_config.write_text("project: {}\n", encoding="utf-8")
    monkeypatch.setattr(desktop, "resolve_seed_source_config_path", lambda: source_config)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local-app-data"))
    monkeypatch.setattr(desktop, "find_free_port", lambda host="127.0.0.1": 54321)

    events: list[tuple[str, object]] = []

    class FakeServer:
        def __init__(self, app: object, *, host: str, port: int) -> None:
            events.append(("server_init", (host, port, app is not None)))

        def start(self) -> None:
            events.append(("server_start", None))

        def stop(self) -> None:
            events.append(("server_stop", None))

    class FakeTray:
        def __init__(self, *, app_url: str, workspace_root: Path, server: FakeServer) -> None:
            del server
            events.append(("tray_init", (app_url, workspace_root)))

        def run(self) -> None:
            events.append(("tray_run", None))

    desktop.launch_desktop_app(
        server_factory=FakeServer,
        tray_factory=FakeTray,
        wait_for_health_fn=lambda url: events.append(("wait", url)),
        open_browser_fn=lambda url: events.append(("browser", url)),
    )

    assert ("wait", "http://127.0.0.1:54321") in events
    assert ("browser", "http://127.0.0.1:54321") in events
    assert ("tray_run", None) in events
    assert ("server_stop", None) in events


def test_launch_desktop_app_uses_localappdata_workspace_by_default(tmp_path: Path, monkeypatch) -> None:
    source_config = tmp_path / "project.yaml"
    source_config.write_text("project: {}\n", encoding="utf-8")
    local_app_data = tmp_path / "appdata"
    monkeypatch.setattr(desktop, "resolve_seed_source_config_path", lambda: source_config)
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))

    captured: dict[str, object] = {}

    class FakeServer:
        def __init__(self, app: object, *, host: str, port: int) -> None:
            del app, host, port

        def start(self) -> None:
            return None

        def stop(self) -> None:
            return None

    class FakeTray:
        def __init__(self, *, app_url: str, workspace_root: Path, server: FakeServer) -> None:
            del app_url, server
            captured["workspace_root"] = workspace_root

        def run(self) -> None:
            return None

    desktop.launch_desktop_app(
        server_factory=FakeServer,
        tray_factory=FakeTray,
        wait_for_health_fn=lambda url: None,
        open_browser_fn=lambda url: None,
    )

    assert captured["workspace_root"] == (local_app_data / "SECI FDRE V").resolve()


def test_tray_quit_stops_server() -> None:
    class FakeServer:
        def __init__(self) -> None:
            self.stop_called = False

        def stop(self) -> None:
            self.stop_called = True

    class FakeIcon:
        def __init__(self) -> None:
            self.stop_called = False

        def stop(self) -> None:
            self.stop_called = True

    server = FakeServer()
    icon = FakeIcon()
    tray = desktop.DesktopTrayApp(
        app_url="http://127.0.0.1:8000",
        workspace_root=Path("/tmp/workspace"),
        server=server,  # type: ignore[arg-type]
    )

    tray._on_quit(icon, None)

    assert server.stop_called is True
    assert icon.stop_called is True
