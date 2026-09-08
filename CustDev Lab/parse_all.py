#!/usr/bin/env python3
"""Non-interactive full channel parse for @custdevlab."""
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# Reuse parse_channel module
sys.path.insert(0, str(Path(__file__).parent))
import parse_channel as pc


async def main():
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    if not api_id or not api_hash:
        print("Нужен файл .env с TELEGRAM_API_ID и TELEGRAM_API_HASH")
        print("Ключи: https://my.telegram.org/apps")
        return 1

    parser = pc.TelegramChannelParser(api_id, api_hash)
    try:
        await parser.connect()
        posts = await parser.parse_channel(limit=None)
        if not posts:
            print("Сообщения не найдены")
            return 1
        print(f"Сохранение {len(posts)} постов...")
        for post in posts:
            parser.save_post(post)
        parser.save_json_archive(posts)
        print(f"Готово: {len(posts)} постов → {pc.OUTPUT_DIR}")
        print(f"Архив → {pc.ARCHIVES_DIR}")
        return 0
    finally:
        await parser.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
