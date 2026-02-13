from __future__ import annotations

import base64
import os
from typing import Optional

import httpx


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")  # format: "owner/repo"
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")


async def _get_file_sha(client: httpx.AsyncClient, path: str) -> Optional[str]:
    """Получить SHA существующего файла в GitHub (если есть)."""
    if not GITHUB_REPO:
        return None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    resp = await client.get(url, params={"ref": GITHUB_BRANCH})
    if resp.status_code == 200:
        data = resp.json()
        return data.get("sha")
    return None


async def upsert_markdown_file(path: str, content: str, message: str) -> None:
    """Создать или обновить файл в GitHub-репозитории.

    Безопасно: при любом исключении просто молча выходим, чтобы не ломать основной поток.
    """
    if not (GITHUB_TOKEN and GITHUB_REPO):
        return

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    async with httpx.AsyncClient(headers=headers, timeout=10) as client:
        try:
            sha = await _get_file_sha(client, path)
            url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
            payload = {
                "message": message,
                "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
                "branch": GITHUB_BRANCH,
            }
            if sha:
                payload["sha"] = sha
            await client.put(url, json=payload)
        except Exception:
            # Не мешаем основному приложению из-за GitHub-проблем
            return

