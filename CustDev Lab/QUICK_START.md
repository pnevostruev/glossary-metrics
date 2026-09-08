# Быстрый старт CustDev Lab

## ✅ Шаг 1: Установка зависимостей (ГОТОВО!)

Зависимости уже установлены:
- ✓ telethon
- ✓ python-dotenv

## 📝 Шаг 2: Получение API ключей

1. Откройте в браузере: **https://my.telegram.org/apps**
2. Войдите используя ваш номер телефона (код придет в Telegram)
3. Заполните форму:
   - **App title**: CustDev Lab Parser
   - **Short name**: custdevlab_parser
   - **Platform**: Desktop
4. Скопируйте:
   - **api_id** (число, например: `12345678`)
   - **api_hash** (строка, например: `abcdef1234567890abcdef1234567890`)

## 🔐 Шаг 3: Настройка credentials

Создайте файл `.env` в директории проекта:

```bash
cd "/Users/pnevostruev/Downloads/Глоссаний метрик/CustDev Lab"
nano .env
```

Или откройте файл в любом редакторе и добавьте:

```
TELEGRAM_API_ID=ваш_api_id_здесь
TELEGRAM_API_HASH=ваш_api_hash_здесь
```

**Важно:** Замените `ваш_api_id_здесь` и `ваш_api_hash_здесь` на реальные значения.

## 🔍 Шаг 4: Проверка доступа

```bash
cd "/Users/pnevostruev/Downloads/Глоссаний метрик/CustDev Lab"
python3 check_telegram_api.py
```

Если всё ок, увидите:
```
✓ Библиотека telethon установлена
✓ Проверка завершена успешно!
```

## 📥 Шаг 5: Парсинг канала

```bash
python3 parse_channel.py
```

При первом запуске:
1. Введите номер телефона: `+79123456789` (с кодом страны)
2. Введите код из Telegram (придет в сообщении)
3. Если установлен 2FA, введите пароль
4. Введите количество постов (или Enter для всех)

После этого посты сохранятся в `content/posts/`!

## 🎯 Быстрые команды

```bash
# Переход в директорию проекта
cd "/Users/pnevostruev/Downloads/Глоссаний метрик/CustDev Lab"

# Проверка доступа
python3 check_telegram_api.py

# Парсинг канала
python3 parse_channel.py

# Просмотр спарсенных постов
ls content/posts/
```

---

**Проблемы?** См. [SETUP.md](SETUP.md) для подробных инструкций и решения проблем.

