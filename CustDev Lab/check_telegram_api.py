#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для проверки доступа к Telegram API
Проверяет наличие необходимых учетных данных и возможность подключения
"""

import os
import sys
from pathlib import Path

def check_api_credentials():
    """Проверяет наличие API credentials"""
    print("=" * 60)
    print("Проверка доступа к Telegram API")
    print("=" * 60)
    
    # Проверяем переменные окружения
    api_id = os.getenv('TELEGRAM_API_ID')
    api_hash = os.getenv('TELEGRAM_API_HASH')
    
    # Проверяем .env файл
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        print(f"\n✓ Найден файл .env: {env_file}")
        with open(env_file, 'r', encoding='utf-8') as f:
            content = f.read()
            if 'TELEGRAM_API_ID' in content:
                print("  ✓ TELEGRAM_API_ID найден в .env")
            if 'TELEGRAM_API_HASH' in content:
                print("  ✓ TELEGRAM_API_HASH найден в .env")
    else:
        print(f"\n⚠ Файл .env не найден: {env_file}")
        print("  Создайте файл .env и добавьте туда:")
        print("  TELEGRAM_API_ID=ваш_api_id")
        print("  TELEGRAM_API_HASH=ваш_api_hash")
    
    # Проверяем переменные окружения
    if api_id:
        print(f"\n✓ TELEGRAM_API_ID найден в переменных окружения")
    else:
        print("\n⚠ TELEGRAM_API_ID не найден в переменных окружения")
    
    if api_hash:
        print(f"✓ TELEGRAM_API_HASH найден в переменных окружения")
    else:
        print("⚠ TELEGRAM_API_HASH не найден в переменных окружения")
    
    # Проверяем наличие telethon
    try:
        import telethon
        print(f"\n✓ Библиотека telethon установлена (версия: {telethon.__version__})")
    except ImportError:
        print("\n❌ Библиотека telethon не установлена")
        print("  Установите: pip install telethon")
        return False
    
    # Пробуем подключиться к API (если есть credentials)
    if api_id and api_hash:
        print("\n" + "=" * 60)
        print("Попытка подключения к Telegram API...")
        print("=" * 60)
        
        try:
            from telethon import TelegramClient
            from telethon.errors import ApiIdInvalidError, PhoneNumberInvalidError
            
            client = TelegramClient(
                'test_session',
                int(api_id),
                api_hash
            )
            
            print("\n✓ Клиент создан успешно")
            print("  Для полной проверки нужно запустить полную аутентификацию")
            print("  Используйте скрипт parse_channel.py для этого")
            
            return True
            
        except ValueError:
            print("\n❌ Ошибка: TELEGRAM_API_ID должен быть числом")
            return False
        except Exception as e:
            print(f"\n❌ Ошибка при создании клиента: {e}")
            return False
    else:
        print("\n" + "=" * 60)
        print("Инструкция по получению API ключей:")
        print("=" * 60)
        print("""
1. Перейдите на https://my.telegram.org/apps
2. Войдите используя ваш номер телефона
3. Заполните форму для создания приложения:
   - App title: CustDev Lab Parser (любое название)
   - Short name: custdevlab_parser (любое короткое имя)
   - Platform: Desktop
   - Description: можно оставить пустым
4. После создания вы получите:
   - api_id (число, например: 12345678)
   - api_hash (строка, например: abcdef1234567890abcdef1234567890)
5. Сохраните эти данные в файл .env:
   TELEGRAM_API_ID=ваш_api_id
   TELEGRAM_API_HASH=ваш_api_hash
   
   ИЛИ установите переменные окружения:
   export TELEGRAM_API_ID=ваш_api_id
   export TELEGRAM_API_HASH=ваш_api_hash
        """)
        return False

def main():
    """Основная функция"""
    has_access = check_api_credentials()
    
    print("\n" + "=" * 60)
    if has_access:
        print("✓ Проверка завершена успешно!")
        print("  Можете запустить parse_channel.py для парсинга канала")
    else:
        print("⚠ Необходимо настроить API ключи")
        print("  Следуйте инструкциям выше")
    print("=" * 60)
    
    return 0 if has_access else 1

if __name__ == '__main__':
    sys.exit(main())

