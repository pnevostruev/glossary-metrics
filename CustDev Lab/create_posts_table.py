#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Создание таблицы со всеми постами канала CustDev Lab
Формат: Номер поста, Заголовок, Текст, Хештеги, Ссылки на другие посты
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Set
import csv

# Конфигурация
PROJECT_DIR = Path(__file__).parent
POSTS_DIR = PROJECT_DIR / 'content' / 'posts'
OUTPUT_CSV = PROJECT_DIR / 'posts_table.csv'
OUTPUT_MD = PROJECT_DIR / 'posts_table.md'
OUTPUT_JSON = PROJECT_DIR / 'posts_table.json'


def extract_post_number(markdown_content: str, filename: str) -> str:
    """Извлекает номер поста - сначала из метаданных, потом из заголовка, потом из имени файла"""
    
    # 1. Пытаемся извлечь из строки **Номер поста:** #104
    meta_pattern = r'\*\*Номер поста:\*\*\s*#?(\d+)'
    meta_match = re.search(meta_pattern, markdown_content, re.IGNORECASE)
    if meta_match:
        return meta_match.group(1)
    
    # 2. Пытаемся извлечь из заголовка (первая строка с # и номером)
    title_patterns = [
        r'^#\s*\*\*(\d+)\.',  # # **104.
        r'^#\s*Пост\s*#?(\d+)',  # # Пост #104
        r'^#\s*(\d+)\.',  # # 104.
        r'^\*\*(\d+)\.',  # **104.
        r'^(\d+)\.\s+',  # 104.
    ]
    
    lines = markdown_content.split('\n')
    for line in lines[:10]:  # Проверяем первые 10 строк
        for pattern in title_patterns:
            match = re.search(pattern, line.strip(), re.IGNORECASE)
            if match:
                return match.group(1)
    
    # 3. Пытаемся извлечь из текста (любое упоминание "Пост #231" в начале)
    text_patterns = [
        r'Пост\s*#(\d+)',
        r'#(\d+)',
    ]
    for pattern in text_patterns:
        match = re.search(pattern, markdown_content[:500], re.IGNORECASE)  # Первые 500 символов
        if match:
            return match.group(1)
    
    # 4. Пытаемся извлечь из имени файла
    filename_match = re.search(r'post_(\d+)_', filename)
    if filename_match:
        return filename_match.group(1)
    
    return None


def extract_publication_date(markdown_content: str) -> Optional[str]:
    """Извлекает дату публикации и конвертирует в формат ДД.ММ.ГГГГ ЧЧ:ММ"""
    
    # Ищем строку **Дата:** 2024-02-20 15:54:56
    date_pattern = r'\*\*Дата:\*\*\s*(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):\d{2}'
    date_match = re.search(date_pattern, markdown_content)
    
    if date_match:
        year = date_match.group(1)
        month = date_match.group(2)
        day = date_match.group(3)
        hour = date_match.group(4)
        minute = date_match.group(5)
        # Конвертируем в формат ДД.ММ.ГГГГ ЧЧ:ММ
        return f"{day}.{month}.{year} {hour}:{minute}"
    
    # Альтернативный формат: только дата без времени
    date_pattern_no_time = r'\*\*Дата:\*\*\s*(\d{4})-(\d{2})-(\d{2})'
    date_match_no_time = re.search(date_pattern_no_time, markdown_content)
    
    if date_match_no_time:
        year = date_match_no_time.group(1)
        month = date_match_no_time.group(2)
        day = date_match_no_time.group(3)
        return f"{day}.{month}.{year} 00:00"
    
    # Альтернативные форматы (уже в формате ДД.ММ.ГГГГ)
    alt_pattern = r'Дата[:\s]+(\d{2})\.(\d{2})\.(\d{4})'
    alt_match = re.search(alt_pattern, markdown_content, re.IGNORECASE)
    if alt_match:
        day, month, year = alt_match.groups()
        return f"{day}.{month}.{year} 00:00"
    
    return None


def extract_title(markdown_content: str) -> str:
    """Извлекает заголовок из markdown файла"""
    # Ищем первую строку с # (заголовок)
    lines = markdown_content.split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('# '):
            # Убираем # и номер поста если есть
            title = line[2:].strip()
            # Убираем префикс "Пост #231:" если есть
            title = re.sub(r'^Пост\s*#?\d+\s*:\s*', '', title, flags=re.IGNORECASE)
            return title
    return "Без заголовка"


def extract_text(markdown_content: str) -> str:
    """Извлекает текст поста (без метаданных и заголовка)"""
    lines = markdown_content.split('\n')
    text_lines = []
    skip_until_content = True
    in_content = False
    
    for line in lines:
        stripped = line.strip()
        
        # Пропускаем заголовок
        if stripped.startswith('# '):
            skip_until_content = True
            continue
        
        # Пропускаем метаданные (строки с **)
        if skip_until_content:
            if stripped.startswith('**') and ':**' in stripped:
                continue
            if stripped.startswith('---'):
                continue
            if not stripped:
                continue
        
        # Начинаем собирать текст после метаданных
        if skip_until_content:
            if stripped and not stripped.startswith('**') and not stripped.startswith('---'):
                skip_until_content = False
                in_content = True
        
        if in_content:
            # Пропускаем последние строки с источником
            if stripped.startswith('*Источник:') or stripped.startswith('*Спарсено:'):
                break
            if stripped == '---' and '*' in '\n'.join(lines[lines.index(line):]):
                break
            
            text_lines.append(line)
    
    # Убираем пустые строки в начале и конце
    while text_lines and not text_lines[0].strip():
        text_lines.pop(0)
    while text_lines and not text_lines[-1].strip():
        text_lines.pop()
    
    text = '\n'.join(text_lines).strip()
    
    # Ограничиваем длину текста (для таблицы)
    if len(text) > 2000:
        text = text[:2000] + "... [обрезано]"
    
    return text


def extract_dry_summary(markdown_content: str) -> str:
    """Извлекает раздел 'В сухом остатке' из поста"""
    # Паттерны для поиска "В сухом остатке"
    patterns = [
        # __**В сухом остатке**: текст__
        r'__\*\*В\s+сухом\s+остатке\*\*:\s*([^_]+)__',
        # **В сухом остатке:** текст
        r'\*\*В\s+сухом\s+остатке\*\*:\s*([^\n]+)',
        # ## Сухой остаток + текст ниже
        r'##\s*Сухой\s+остаток\s*\n+([^\n#]+(?:\n[^\n#]+)*)',
        # "В сухом остатке": текст (без форматирования)
        r'В\s+сухом\s+остатке\s*[:\-]\s*([^\n]+)',
        # "Сухой остаток": текст
        r'Сухой\s+остаток\s*[:\-]\s*([^\n]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, markdown_content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if match:
            summary = match.group(1).strip()
            # Очищаем от markdown форматирования
            summary = re.sub(r'\*\*([^*]+)\*\*', r'\1', summary)  # **текст** -> текст
            summary = re.sub(r'__([^_]+)__', r'\1', summary)  # __текст__ -> текст
            summary = re.sub(r'\*([^*]+)\*', r'\1', summary)  # *текст* -> текст
            summary = re.sub(r'_([^_]+)_', r'\1', summary)  # _текст_ -> текст
            summary = re.sub(r'\n+', ' ', summary)  # Множественные переносы -> пробел
            summary = summary.strip()
            
            if summary and len(summary) > 10:  # Проверяем, что это не пустая строка
                return summary
    
    return ''


def extract_hashtags(markdown_content: str) -> List[str]:
    """Извлекает хештеги из markdown файла"""
    hashtags = []
    
    # Ищем в строке с тегами: **Теги:** #tag1 #tag2
    tag_pattern = r'\*\*Теги?:\*\*\s*(.+)'
    tag_match = re.search(tag_pattern, markdown_content, re.IGNORECASE)
    if tag_match:
        tag_line = tag_match.group(1).strip()
        # Извлекаем все хештеги
        hashtags.extend(re.findall(r'#[\w@]+', tag_line))
    
    # Также ищем хештеги в тексте поста
    all_hashtags = re.findall(r'#[\w@]+', markdown_content)
    hashtags.extend(all_hashtags)
    
    # Удаляем дубликаты и сортируем
    hashtags = sorted(list(set(hashtags)))
    
    return hashtags


def extract_references(markdown_content: str) -> List[str]:
    """Извлекает ссылки на другие посты"""
    references = []
    found_numbers = set()
    
    # 1. Ищем прямые ссылки в формате https://t.me/custdevlab/{номер}
    url_pattern = r'https?://t\.me/custdevlab/(\d+)'
    url_matches = re.findall(url_pattern, markdown_content, re.IGNORECASE)
    for match in url_matches:
        if match.isdigit() and match not in found_numbers:
            found_numbers.add(match)
            references.append(f"Пост #{match}")
    
    # 2. Ищем упоминания других постов через @custdevlab
    # Например: "@custdevlab/231" или "см. пост #231"
    patterns = [
        r'@custdevlab[/\s]*#?(\d+)',
        r'пост\s*#?(\d+)',
        r'пост\s*№\s*(\d+)',
        r'post\s*#?(\d+)',
        r'#(\d+)@custdevlab',
        r'см\.?\s*пост\s*#?(\d+)',
        r'см\.?\s*[^.]*пост\s*#?(\d+)',
        r'\[([^\]]+)\]\(https?://t\.me/custdevlab/(\d+)\)',  # [текст](ссылка)
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, markdown_content, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                # Если кортеж, берем номер поста (обычно второй элемент)
                post_num = match[1] if len(match) > 1 else match[0]
            else:
                post_num = match
            
            if post_num and post_num.isdigit() and post_num not in found_numbers:
                found_numbers.add(post_num)
                references.append(f"Пост #{post_num}")
    
    # 3. Ищем упоминания в тексте вида "в посте 231" или "(см. 146)"
    text_patterns = [
        r'в\s+посте\s+(\d+)',
        r'\(см\.?\s*(\d+)\)',
        r'\(пост\s*(\d+)\)',
        r'\[([^\]]*)\]\(https?://t\.me/custdevlab/(\d+)\)',
    ]
    
    for pattern in text_patterns:
        matches = re.findall(pattern, markdown_content, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                post_num = match[1] if len(match) > 1 and match[1].isdigit() else (match[0] if match[0].isdigit() else None)
            else:
                post_num = match if match.isdigit() else None
            
            if post_num and post_num not in found_numbers:
                found_numbers.add(post_num)
                references.append(f"Пост #{post_num}")
    
    return sorted(references, key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else 9999)


def parse_post_file(filepath: Path) -> Dict:
    """Парсит один файл поста"""
    try:
        content = filepath.read_text(encoding='utf-8')
    except Exception as e:
        print(f"⚠ Ошибка чтения {filepath.name}: {e}")
        return None
    
    # Извлекаем данные
    post_number = extract_post_number(content, filepath.name)
    publication_date = extract_publication_date(content)
    
    title = extract_title(content)
    text = extract_text(content)
    hashtags = extract_hashtags(content)
    references = extract_references(content)
    dry_summary = extract_dry_summary(content)
    
    return {
        'post_number': post_number if post_number else 'нет номера поста',
        'publication_date': publication_date or '',
        'title': title,
        'text': text,
        'hashtags': ', '.join(hashtags) if hashtags else '',
        'references': ', '.join(references) if references else '',
        'dry_summary': dry_summary,
        'filename': filepath.name,
    }


def create_csv_table(posts: List[Dict]):
    """Создает CSV таблицу"""
    print(f"\nСоздание CSV таблицы...")
    
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        
        # Заголовки
        writer.writerow([
            'Номер поста',
            'Дата публикации',
            'Заголовок поста',
            'Текст поста',
            'Хештеги',
            'Ссылки на другие посты',
            'В сухом остатке'
        ])
        
        # Данные
        for post in posts:
            writer.writerow([
                post['post_number'],
                post['publication_date'],
                post['title'],
                post['text'],
                post['hashtags'],
                post['references'],
                post['dry_summary']
            ])
    
    print(f"✓ CSV таблица сохранена: {OUTPUT_CSV}")


def create_markdown_table(posts: List[Dict]):
    """Создает Markdown таблицу"""
    print(f"\nСоздание Markdown таблицы...")
    
    md_content = ["# Таблица постов канала @custdevlab\n"]
    md_content.append(f"Всего постов: {len(posts)}\n")
    md_content.append("| Номер поста | Дата публикации | Заголовок | Текст | Хештеги | Ссылки | В сухом остатке |\n")
    md_content.append("|-------------|-----------------|-----------|-------|---------|--------|-----------------|\n")
    
    for post in posts:
        # Экранируем специальные символы для Markdown
        title = post['title'].replace('|', '\\|').replace('\n', ' ')
        text = post['text'].replace('|', '\\|').replace('\n', ' ')[:200]
        if len(post['text']) > 200:
            text += "..."
        summary = post['dry_summary'].replace('|', '\\|').replace('\n', ' ')[:150]
        if len(post['dry_summary']) > 150:
            summary += "..."
        
        md_content.append(
            f"| {post['post_number']} | {post['publication_date']} | {title} | {text} | {post['hashtags']} | {post['references']} | {summary} |\n"
        )
    
    OUTPUT_MD.write_text(''.join(md_content), encoding='utf-8')
    print(f"✓ Markdown таблица сохранена: {OUTPUT_MD}")
    
    # Также создаем компактную версию
    create_compact_markdown_table(posts)


def create_compact_markdown_table(posts: List[Dict]):
    """Создает компактную Markdown таблицу (без полного текста)"""
    md_content = ["# Таблица постов канала @custdevlab (компактная версия)\n"]
    md_content.append(f"Всего постов: {len(posts)}\n")
    md_content.append("| № | Дата публикации | Заголовок | Хештеги | Ссылки | В сухом остатке |\n")
    md_content.append("|---|-----------------|-----------|---------|--------|-----------------|\n")
    
    for post in posts:
        title = post['title'].replace('|', '\\|')[:100]
        if len(post['title']) > 100:
            title += "..."
        summary = post['dry_summary'].replace('|', '\\|')[:100]
        if len(post['dry_summary']) > 100:
            summary += "..."
        
        md_content.append(
            f"| {post['post_number']} | {post['publication_date']} | {title} | {post['hashtags']} | {post['references']} | {summary} |\n"
        )
    
    compact_output = PROJECT_DIR / 'posts_table_compact.md'
    compact_output.write_text(''.join(md_content), encoding='utf-8')
    print(f"✓ Компактная таблица сохранена: {compact_output}")


def create_json_table(posts: List[Dict]):
    """Создает JSON таблицу"""
    print(f"\nСоздание JSON таблицы...")
    
    output_data = {
        'total_posts': len(posts),
        'posts': posts
    }
    
    OUTPUT_JSON.write_text(
        json.dumps(output_data, ensure_ascii=False, indent=2),
        encoding='utf-8'
    )
    print(f"✓ JSON таблица сохранена: {OUTPUT_JSON}")


def main():
    """Основная функция"""
    print("=" * 60)
    print("Создание таблицы постов CustDev Lab")
    print("=" * 60)
    
    # Находим все файлы постов
    post_files = sorted(POSTS_DIR.glob('*.md'))
    print(f"\nНайдено файлов постов: {len(post_files)}")
    
    if not post_files:
        print("❌ Файлы постов не найдены!")
        return
    
    # Парсим все посты
    print("\nПарсинг постов...")
    posts = []
    for i, post_file in enumerate(post_files, 1):
        post_data = parse_post_file(post_file)
        if post_data:
            posts.append(post_data)
            if i % 50 == 0:
                print(f"  Обработано: {i}/{len(post_files)}")
    
    print(f"\n✓ Обработано постов: {len(posts)}")
    
    # Сортируем по дате публикации
    def sort_key(post):
        date_str = post['publication_date']
        if not date_str:
            return (1, '9999-12-31 23:59')  # Без даты в конец
        
        try:
            # Парсим дату из формата ДД.ММ.ГГГГ ЧЧ:ММ
            # Конвертируем в формат для сортировки: ГГГГ-ММ-ДД ЧЧ:ММ
            parts = date_str.split(' ')
            if len(parts) == 2:
                date_part, time_part = parts
                day, month, year = date_part.split('.')
                return (0, f"{year}-{month}-{day} {time_part}")
            else:
                day, month, year = date_str.split('.')
                return (0, f"{year}-{month}-{day} 00:00")
        except:
            return (1, '9999-12-31 23:59')
    
    posts.sort(key=sort_key)
    
    # Создаем таблицы
    create_csv_table(posts)
    create_markdown_table(posts)
    create_json_table(posts)
    
    # Статистика
    posts_with_numbers = sum(1 for p in posts if p['post_number'] != 'НН')
    posts_with_hashtags = sum(1 for p in posts if p['hashtags'])
    posts_with_references = sum(1 for p in posts if p['references'])
    posts_with_summary = sum(1 for p in posts if p['dry_summary'])
    
    print("\n" + "=" * 60)
    print("📊 Статистика:")
    print(f"  Всего постов: {len(posts)}")
    print(f"  С номерами: {posts_with_numbers}")
    print(f"  С хештегами: {posts_with_hashtags}")
    print(f"  Со ссылками: {posts_with_references}")
    print(f"  С 'В сухом остатке': {posts_with_summary}")
    print("=" * 60)
    print("\n✓ Готово! Таблицы созданы:")
    print(f"  - {OUTPUT_CSV} (для Excel)")
    print(f"  - {OUTPUT_MD} (Markdown)")
    print(f"  - {OUTPUT_JSON} (JSON)")
    print("=" * 60)


if __name__ == '__main__':
    main()

