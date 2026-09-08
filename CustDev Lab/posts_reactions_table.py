#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Таблица постов канала: номер поста, id сообщения, сумма реакций, дата.
Сортировка по дате публикации (от старых к новым), как posts_views_table.csv.

Требует авторизованную сессию telethon (тот же custdevlab_session, что у parse_channel.py).
Запуск: python3 posts_reactions_table.py
"""

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from parse_channel import CHANNEL_USERNAME, TelegramChannelParser

PROJECT_DIR = Path(__file__).parent
OUT_CSV = PROJECT_DIR / 'posts_reactions_table.csv'


def sort_key(row):
    d = row['date']
    if not d:
        return datetime.min, 0
    return datetime.strptime(d, '%Y-%m-%d %H:%M:%S'), row['message_id']


async def main():
    if load_dotenv:
        load_dotenv(PROJECT_DIR / '.env')

    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    if not api_id or not api_hash:
        print('❌ Задайте TELEGRAM_API_ID и TELEGRAM_API_HASH в .env')
        return 1

    parser = TelegramChannelParser(api_id, api_hash)
    rows = []

    try:
        await parser.connect()
        print(f'Сбор сообщений @{CHANNEL_USERNAME}...')
        async for message in parser.client.iter_messages(CHANNEL_USERNAME):
            text = getattr(message, 'text', None) or ''
            if not text.strip():
                continue
            post = parser.parse_message(message)
            if not post:
                continue
            pn = post.get('post_number')
            row_pn = str(pn) if pn not in (None, '') else '—'
            rows.append({
                'post_number': row_pn,
                'message_id': post['id'],
                'reactions': int(post.get('reactions') or 0),
                'date': post['date'] or '',
            })

        rows.sort(key=sort_key)
        lines = ['post_number;message_id;reactions;date']
        for r in rows:
            lines.append(
                f"{r['post_number']};{r['message_id']};{r['reactions']};{r['date']}"
            )
        OUT_CSV.write_text('\n'.join(lines), encoding='utf-8')
        print(f'✓ Готово: {len(rows)} строк → {OUT_CSV}')
        return 0
    except KeyboardInterrupt:
        print('\n⚠ Прервано')
        return 1
    finally:
        await parser.close()


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
