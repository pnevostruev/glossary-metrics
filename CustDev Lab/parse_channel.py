#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Парсер телеграм-канала @custdevlab
Скачивает посты и сохраняет их в структурированном виде
"""

import os
import sys
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import json

try:
    from telethon import TelegramClient
    from telethon.tl.types import Message
    from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
except ImportError:
    print("❌ Библиотека telethon не установлена")
    print("Установите: pip install telethon")
    sys.exit(1)

# Конфигурация
CHANNEL_USERNAME = 'custdevlab'  # Без @
SESSION_NAME = 'custdevlab_session'
OUTPUT_DIR = Path(__file__).parent / 'content' / 'posts'
ARCHIVES_DIR = Path(__file__).parent / 'content' / 'archives'

# Создаем директории
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVES_DIR.mkdir(parents=True, exist_ok=True)


def count_message_reactions(message: Message) -> int:
    """Сумма всех реакций на сообщение (по всем типам эмодзи в канале)."""
    r = getattr(message, 'reactions', None)
    if r is None:
        return 0
    results = getattr(r, 'results', None) or []
    total = 0
    for item in results:
        try:
            total += int(getattr(item, 'count', 0) or 0)
        except (TypeError, ValueError):
            continue
    return total


class TelegramChannelParser:
    """Парсер для телеграм-канала"""
    
    def __init__(self, api_id: str, api_hash: str):
        """Инициализация клиента"""
        self.api_id = int(api_id)
        self.api_hash = api_hash
        self.client = TelegramClient(SESSION_NAME, self.api_id, self.api_hash)
        self.parsed_posts = []
        
    async def connect(self):
        """Подключение к Telegram"""
        print("Подключение к Telegram...")
        await self.client.connect()
        
        if not await self.client.is_user_authorized():
            print("Требуется аутентификация")
            phone = input("Введите номер телефона (с кодом страны, например +79123456789): ")
            await self.client.send_code_request(phone)
            
            try:
                code = input("Введите код из Telegram: ")
                await self.client.sign_in(phone, code)
            except PhoneCodeInvalidError:
                print("❌ Неверный код")
                sys.exit(1)
            except SessionPasswordNeededError:
                password = input("Введите пароль 2FA (если установлен): ")
                await self.client.sign_in(password=password)
        
        print("✓ Успешно подключено!")
        
    async def parse_channel(self, limit: Optional[int] = None, offset_id: int = 0):
        """Парсинг канала"""
        print(f"\nПарсинг канала @{CHANNEL_USERNAME}...")
        
        try:
            messages = []
            async for message in self.client.iter_messages(
                CHANNEL_USERNAME,
                limit=limit,
                offset_id=offset_id,
                reverse=False
            ):
                if message.text:  # Только текстовые сообщения
                    messages.append(message)
            
            print(f"✓ Найдено {len(messages)} сообщений")
            
            for message in reversed(messages):  # Обрабатываем в хронологическом порядке
                post_data = self.parse_message(message)
                if post_data:
                    self.parsed_posts.append(post_data)
            
            return self.parsed_posts
            
        except ValueError as e:
            print(f"❌ Ошибка: {e}")
            print(f"   Убедитесь, что канал @{CHANNEL_USERNAME} существует и доступен")
            return []
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
            return []
    
    def parse_message(self, message: Message) -> Optional[Dict[str, Any]]:
        """Парсинг отдельного сообщения"""
        text = message.text or ""
        if not text.strip():
            return None
        
        # Извлекаем номер поста из текста (если есть паттерн "XXX. Заголовок")
        post_number = None
        title_match = re.match(r'^(\d+)\.\s+(.+)', text)
        if title_match:
            post_number = title_match.group(1)
            title = title_match.group(2).split('\n')[0].strip()
        else:
            # Пытаемся извлечь из начала текста
            first_line = text.split('\n')[0]
            num_match = re.search(r'#?(\d+)', first_line)
            if num_match:
                post_number = num_match.group(1)
            title = first_line[:100]  # Первые 100 символов
        
        # Извлекаем хештеги
        hashtags = re.findall(r'#[\w@]+', text)
        
        # Дата
        date = message.date.strftime('%Y-%m-%d %H:%M:%S') if message.date else None
        
        return {
            'id': message.id,
            'post_number': post_number,
            'title': title,
            'text': text,
            'date': date,
            'hashtags': hashtags,
            'views': message.views if hasattr(message, 'views') else None,
            'reactions': count_message_reactions(message),
        }
    
    def save_post(self, post: Dict[str, Any]):
        """Сохранение поста в файл"""
        post_num = post.get('post_number') or post['id']
        title_slug = slugify(post['title'])
        filename = f"post_{post_num}_{title_slug}.md"
        filepath = OUTPUT_DIR / filename
        
        # Формируем содержимое файла
        content = f"""# {post['title']}

**ID сообщения:** {post['id']}
**Дата:** {post['date'] or 'Не указана'}
"""
        if post['post_number']:
            content += f"**Номер поста:** #{post['post_number']}\n"
        if post['hashtags']:
            content += f"**Теги:** {' '.join(post['hashtags'])}\n"
        if post.get('views'):
            content += f"**Просмотры:** {post['views']}\n"
        content += f"**Реакции:** {post.get('reactions', 0)}\n"
        
        content += f"""
---

{post['text']}

---

*Источник: [@custdevlab](https://t.me/custdevlab)*
*Спарсено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        filepath.write_text(content, encoding='utf-8')
        print(f"  ✓ Сохранено: {filename}")
        
    def save_json_archive(self, posts: List[Dict[str, Any]]):
        """Сохранение всех постов в JSON архив"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = ARCHIVES_DIR / f"custdevlab_archive_{timestamp}.json"
        
        archive_data = {
            'channel': CHANNEL_USERNAME,
            'parsed_at': datetime.now().isoformat(),
            'total_posts': len(posts),
            'posts': posts
        }
        
        filename.write_text(
            json.dumps(archive_data, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )
        print(f"\n✓ Архив сохранен: {filename}")
        
    async def close(self):
        """Закрытие соединения"""
        await self.client.disconnect()


def slugify(text: str, max_length: int = 50) -> str:
    """Создание slug из текста для имени файла"""
    # Транслитерация и очистка
    text = text.lower()
    # Заменяем русские буквы
    replacements = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
        'е': 'e', 'ё': 'e', 'ж': 'zh', 'з': 'z', 'и': 'i',
        'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
        'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
        'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch',
        'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '',
        'э': 'e', 'ю': 'yu', 'я': 'ya'
    }
    for ru, en in replacements.items():
        text = text.replace(ru, en)
    
    # Оставляем только буквы, цифры, пробелы и дефисы
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    # Заменяем пробелы на подчеркивания
    text = re.sub(r'\s+', '_', text)
    # Убираем множественные подчеркивания
    text = re.sub(r'_+', '_', text)
    # Обрезаем до max_length
    text = text[:max_length].strip('_')
    
    return text or 'post'


async def main():
    """Основная функция"""
    print("=" * 60)
    print("Парсер телеграм-канала @custdevlab")
    print("=" * 60)
    
    # Получаем API credentials
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    
    # Пробуем загрузить из .env файла
    if not api_id or not api_hash:
        env_file = Path(__file__).parent / '.env'
        if env_file.exists():
            print(f"Загрузка credentials из .env...")
            from dotenv import load_dotenv
            load_dotenv(env_file)
            api_id = os.getenv('TELEGRAM_API_ID')
            api_hash = os.getenv('TELEGRAM_API_HASH')
    
    if not api_id or not api_hash:
        print("\n❌ Ошибка: TELEGRAM_API_ID и TELEGRAM_API_HASH не найдены")
        print("\nСоздайте файл .env в директории проекта:")
        print("TELEGRAM_API_ID=ваш_api_id")
        print("TELEGRAM_API_HASH=ваш_api_hash")
        print("\nИли установите переменные окружения")
        print("\nДля получения ключей:")
        print("1. Перейдите на https://my.telegram.org/apps")
        print("2. Войдите и создайте приложение")
        print("3. Скопируйте api_id и api_hash")
        return 1
    
    # Параметры парсинга
    print("\nПараметры парсинга:")
    limit_input = input("Количество постов для скачивания (Enter = все доступные): ").strip()
    limit = int(limit_input) if limit_input.isdigit() else None
    
    # Создаем парсер
    parser = TelegramChannelParser(api_id, api_hash)
    
    try:
        # Подключаемся
        await parser.connect()
        
        # Парсим канал
        posts = await parser.parse_channel(limit=limit)
        
        if not posts:
            print("\n⚠ Сообщения не найдены")
            return 1
        
        # Сохраняем посты
        print(f"\nСохранение {len(posts)} постов...")
        for post in posts:
            parser.save_post(post)
        
        # Сохраняем JSON архив
        parser.save_json_archive(posts)
        
        print("\n" + "=" * 60)
        print(f"✓ Готово! Спарсено {len(posts)} постов")
        print(f"  Файлы сохранены в: {OUTPUT_DIR}")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\n⚠ Парсинг прерван пользователем")
        return 1
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        await parser.close()
    
    return 0


if __name__ == '__main__':
    try:
        import asyncio
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠ Прервано пользователем")
        sys.exit(1)

