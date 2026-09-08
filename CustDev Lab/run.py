#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Единый скрипт для настройки и запуска парсера CustDev Lab
Автоматически проверяет зависимости, API ключи и запускает парсинг
"""

import os
import sys
import subprocess
from pathlib import Path

# Константы
PROJECT_DIR = Path(__file__).parent
ENV_FILE = PROJECT_DIR / '.env'
REQUIREMENTS_FILE = PROJECT_DIR / 'requirements.txt'

def print_header(text):
    """Красиво печатает заголовок"""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60 + "\n")

def print_step(step, text):
    """Печатает шаг"""
    print(f"\n[{step}] {text}")
    print("-" * 60)

def check_command(command):
    """Проверяет наличие команды в системе"""
    try:
        subprocess.run(
            [command, '--version'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def check_python_package(package_name):
    """Проверяет установлен ли Python пакет"""
    try:
        __import__(package_name.replace('-', '_'))
        return True
    except ImportError:
        return False

def install_requirements():
    """Устанавливает зависимости"""
    print_step(1, "Проверка зависимостей Python")
    
    # Проверяем необходимые пакеты
    required_packages = {
        'telethon': 'telethon',
        'python-dotenv': 'dotenv'
    }
    
    missing_packages = []
    for package_name, import_name in required_packages.items():
        if not check_python_package(import_name):
            missing_packages.append(package_name)
            print(f"  ⚠ {package_name} не установлен")
        else:
            print(f"  ✓ {package_name} установлен")
    
    if not missing_packages:
        print("\n✓ Все зависимости установлены!")
        return True
    
    print(f"\n⚠ Нужно установить: {', '.join(missing_packages)}")
    
    # Пробуем установить
    pip_command = 'pip3'
    if not check_command('pip3'):
        pip_command = 'python3 -m pip'
    
    print(f"\nУстановка зависимостей через {pip_command}...")
    
    try:
        if pip_command == 'pip3':
            subprocess.run(
                ['pip3', 'install', '-r', str(REQUIREMENTS_FILE)],
                check=True
            )
        else:
            subprocess.run(
                ['python3', '-m', 'pip', 'install', '-r', str(REQUIREMENTS_FILE)],
                check=True
            )
        print("\n✓ Зависимости успешно установлены!")
        return True
    except subprocess.CalledProcessError:
        print("\n❌ Ошибка при установке зависимостей")
        print(f"Попробуйте вручную: {pip_command} install telethon python-dotenv")
        return False
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
        return False

def check_api_credentials():
    """Проверяет наличие API credentials"""
    print_step(2, "Проверка API ключей Telegram")
    
    # Загружаем из .env файла
    api_id = None
    api_hash = None
    
    if ENV_FILE.exists():
        print(f"  ✓ Файл .env найден: {ENV_FILE}")
        try:
            from dotenv import load_dotenv
            load_dotenv(ENV_FILE)
            api_id = os.getenv('TELEGRAM_API_ID')
            api_hash = os.getenv('TELEGRAM_API_HASH')
        except ImportError:
            # Если dotenv не работает, читаем вручную
            with open(ENV_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('TELEGRAM_API_ID=') and not line.startswith('#'):
                        api_id = line.split('=', 1)[1].strip()
                    elif line.startswith('TELEGRAM_API_HASH=') and not line.startswith('#'):
                        api_hash = line.split('=', 1)[1].strip()
    else:
        print(f"  ⚠ Файл .env не найден: {ENV_FILE}")
    
    # Проверяем переменные окружения
    if not api_id:
        api_id = os.getenv('TELEGRAM_API_ID')
    if not api_hash:
        api_hash = os.getenv('TELEGRAM_API_HASH')
    
    if api_id and api_hash:
        print(f"  ✓ TELEGRAM_API_ID найден")
        print(f"  ✓ TELEGRAM_API_HASH найден")
        return True, api_id, api_hash
    else:
        print("\n  ⚠ API ключи не найдены!")
        return False, None, None

def setup_api_credentials():
    """Помогает настроить API credentials"""
    print_step("2.1", "Настройка API ключей")
    
    print("\nДля получения API ключей:")
    print("1. Откройте в браузере: https://my.telegram.org/apps")
    print("2. Войдите используя ваш номер телефона")
    print("3. Создайте приложение:")
    print("   - App title: CustDev Lab Parser (любое)")
    print("   - Short name: custdevlab_parser (любое)")
    print("   - Platform: Desktop")
    print("4. Скопируйте api_id и api_hash\n")
    
    api_id = input("Введите TELEGRAM_API_ID (или Enter чтобы пропустить): ").strip()
    if not api_id:
        print("\n⚠ Пропущено. Создайте файл .env вручную")
        return False, None, None
    
    api_hash = input("Введите TELEGRAM_API_HASH (или Enter чтобы пропустить): ").strip()
    if not api_hash:
        print("\n⚠ Пропущено. Создайте файл .env вручную")
        return False, None, None
    
    # Сохраняем в .env
    try:
        env_content = f"""# Telegram API credentials
# Получено через https://my.telegram.org/apps

TELEGRAM_API_ID={api_id}
TELEGRAM_API_HASH={api_hash}
"""
        ENV_FILE.write_text(env_content, encoding='utf-8')
        print(f"\n✓ API ключи сохранены в {ENV_FILE}")
        return True, api_id, api_hash
    except Exception as e:
        print(f"\n❌ Ошибка при сохранении: {e}")
        return False, None, None

async def run_parser(api_id, api_hash):
    """Запускает парсер"""
    print_step(3, "Запуск парсера канала")
    
    # Добавляем путь к модулю
    sys.path.insert(0, str(PROJECT_DIR))
    
    try:
        from parse_channel import TelegramChannelParser
        
        # Параметры парсинга
        print("\nПараметры парсинга:")
        limit_input = input("Количество постов (Enter = все доступные): ").strip()
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
                return False
            
            # Сохраняем посты
            print(f"\nСохранение {len(posts)} постов...")
            for post in posts:
                parser.save_post(post)
            
            # Сохраняем JSON архив
            parser.save_json_archive(posts)
            
            # Получаем путь для вывода
            from parse_channel import OUTPUT_DIR
            
            print("\n" + "=" * 60)
            print(f"✓ Готово! Спарсено {len(posts)} постов")
            print(f"  Файлы сохранены в: {OUTPUT_DIR}")
            print("=" * 60)
            
            return True
            
        except KeyboardInterrupt:
            print("\n\n⚠ Парсинг прерван пользователем")
            return False
        finally:
            await parser.close()
            
    except ImportError:
        print("❌ Не удалось импортировать parse_channel")
        return False
    except Exception as e:
        print(f"❌ Ошибка при парсинге: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main_async():
    """Асинхронная основная функция"""
    # 1. Установка зависимостей
    if not install_requirements():
        return 1
    
    # 2. Проверка API credentials
    has_credentials, api_id, api_hash = check_api_credentials()
    
    if not has_credentials:
        setup_success, api_id, api_hash = setup_api_credentials()
        if not setup_success:
            print("\n❌ Не удалось настроить API ключи")
            print("\nСоздайте файл .env вручную:")
            print("TELEGRAM_API_ID=ваш_api_id")
            print("TELEGRAM_API_HASH=ваш_api_hash")
            return 1
    
    # 3. Запуск парсера
    success = await run_parser(api_id, api_hash)
    
    return 0 if success else 1

def main():
    """Основная функция"""
    print_header("CustDev Lab - Автоматический парсер")
    print("Этот скрипт автоматически:")
    print("  1. Проверит и установит зависимости")
    print("  2. Настроит API ключи (если нужно)")
    print("  3. Запустит парсинг канала @custdevlab")
    
    try:
        import asyncio
        exit_code = asyncio.run(main_async())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠ Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

