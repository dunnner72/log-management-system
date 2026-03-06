import csv
import io
import json
import sqlite3
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DB_PATH = BASE_DIR / "logs.db"


def utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class LogRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    source TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.commit()

    def list_logs(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []

        keyword = filters.get("keyword", "").strip()
        level = filters.get("level", "").strip().upper()
        source = filters.get("source", "").strip()
        start = filters.get("start", "").strip()
        end = filters.get("end", "").strip()

        if keyword:
            conditions.append("(message LIKE ? OR details LIKE ? OR source LIKE ?)")
            match = f"%{keyword}%"
            params.extend([match, match, match])
        if level:
            conditions.append("level = ?")
            params.append(level)
        if source:
            conditions.append("source LIKE ?")
            params.append(f"%{source}%")
        if start:
            conditions.append("timestamp >= ?")
            params.append(start)
        if end:
            conditions.append("timestamp <= ?")
            params.append(end)

        query = "SELECT * FROM logs"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY timestamp DESC, id DESC"

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def create_log(self, payload: dict[str, Any]) -> dict[str, Any]:
        timestamp = payload.get("timestamp") or utc_now()
        level = str(payload.get("level", "INFO")).upper()
        source = str(payload.get("source", "")).strip()
        message = str(payload.get("message", "")).strip()
        details = str(payload.get("details", "")).strip()

        if level not in {"DEBUG", "INFO", "WARN", "ERROR"}:
            raise ValueError("level must be one of DEBUG, INFO, WARN, ERROR")
        if not source:
            raise ValueError("source is required")
        if not message:
            raise ValueError("message is required")

        created_at = utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO logs (timestamp, level, source, message, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (timestamp, level, source, message, details, created_at),
            )
            log_id = cursor.lastrowid
            connection.commit()

        return self.get_log(int(log_id))

    def get_log(self, log_id: int) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM logs WHERE id = ?", (log_id,)).fetchone()
        if row is None:
            raise KeyError("log not found")
        return dict(row)

    def delete_log(self, log_id: int) -> None:
        with self._connect() as connection:
            cursor = connection.execute("DELETE FROM logs WHERE id = ?", (log_id,))
            connection.commit()
        if cursor.rowcount == 0:
            raise KeyError("log not found")

    def stats(self) -> dict[str, Any]:
        with self._connect() as connection:
            total = connection.execute("SELECT COUNT(*) FROM logs").fetchone()[0]
            by_level_rows = connection.execute(
                "SELECT level, COUNT(*) AS count FROM logs GROUP BY level ORDER BY count DESC, level ASC"
            ).fetchall()
            sources_rows = connection.execute(
                """
                SELECT source, COUNT(*) AS count
                FROM logs
                GROUP BY source
                ORDER BY count DESC, source ASC
                LIMIT 5
                """
            ).fetchall()

        return {
            "total": total,
            "by_level": [dict(row) for row in by_level_rows],
            "top_sources": [dict(row) for row in sources_rows],
        }

    def export_csv(self, filters: dict[str, str]) -> str:
        logs = self.list_logs(filters)
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=["id", "timestamp", "level", "source", "message", "details", "created_at"],
        )
        writer.writeheader()
        writer.writerows(logs)
        return output.getvalue()


class LogRequestHandler(BaseHTTPRequestHandler):
    repository = LogRepository(DB_PATH)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/" or parsed.path == "/index.html":
            self._serve_file("index.html", "text/html; charset=utf-8")
            return
        if parsed.path.startswith("/static/"):
            relative_path = parsed.path.removeprefix("/static/")
            content_type = self._guess_content_type(relative_path)
            self._serve_file(relative_path, content_type)
            return
        if parsed.path == "/api/logs":
            filters = self._normalize_query(parse_qs(parsed.query))
            self._send_json({"items": self.repository.list_logs(filters)})
            return
        if parsed.path == "/api/stats":
            self._send_json(self.repository.stats())
            return
        if parsed.path == "/api/export":
            filters = self._normalize_query(parse_qs(parsed.query))
            content = self.repository.export_csv(filters)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="logs-export.csv"')
            encoded = content.encode("utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
            return

        self._send_error(HTTPStatus.NOT_FOUND, "resource not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/logs":
            self._send_error(HTTPStatus.NOT_FOUND, "resource not found")
            return

        try:
            payload = self._read_json_body()
            created = self.repository.create_log(payload)
        except ValueError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
            return

        self._send_json(created, status=HTTPStatus.CREATED)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        if len(parts) != 3 or parts[:2] != ["api", "logs"]:
            self._send_error(HTTPStatus.NOT_FOUND, "resource not found")
            return

        try:
            self.repository.delete_log(int(parts[2]))
        except ValueError:
            self._send_error(HTTPStatus.BAD_REQUEST, "invalid log id")
            return
        except KeyError:
            self._send_error(HTTPStatus.NOT_FOUND, "log not found")
            return

        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _serve_file(self, relative_path: str, content_type: str) -> None:
        target = STATIC_DIR / relative_path
        if not target.exists() or not target.is_file():
            self._send_error(HTTPStatus.NOT_FOUND, "static file not found")
            return
        content = target.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        self._send_json({"error": message}, status=status)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("invalid JSON body") from exc

    def _normalize_query(self, query: dict[str, list[str]]) -> dict[str, str]:
        return {key: values[0] for key, values in query.items() if values}

    def _guess_content_type(self, path: str) -> str:
        suffix = Path(path).suffix.lower()
        return {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".html": "text/html; charset=utf-8",
        }.get(suffix, "application/octet-stream")


def run_server(host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), LogRequestHandler)
    print(f"Log management system running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
