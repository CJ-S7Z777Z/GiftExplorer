
import requests
from bs4 import BeautifulSoup
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.client import Config
from hashlib import md5

# Инициализация клиента Yandex Object Storage
def init_yandex_client(yandex_config):
    yandex_client = boto3.client(
        's3',
        aws_access_key_id=yandex_config['id'],
        aws_secret_access_key=yandex_config['key'],
        endpoint_url='https://storage.yandexcloud.net',  # Endpoint для Yandex Object Storage
        config=Config(signature_version='s3v4'),
    )
    return yandex_client

# Функция для загрузки файла в Yandex Object Storage
def upload_to_yandex(yandex_client, bucket_name, file_path, object_key):
    try:
        yandex_client.upload_file(file_path, bucket_name, object_key)
        print(f"Файл загружен: {object_key}")
    except Exception as e:
        print(f"Ошибка загрузки файла {object_key}: {e}")

# Функция для получения хеша содержимого подарка
def get_content_hash(gift_data):
    hash_md5 = md5()
    hash_md5.update(json.dumps(gift_data, sort_keys=True).encode('utf-8'))
    return hash_md5.hexdigest()

# Функция для проверки изменений
def has_changed(old_hash, new_hash):
    return old_hash != new_hash

# Функции извлечения и парсинга данных
def fetch_gift_page(url):
    """Получает HTML-содержимое страницы подарка."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/115.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении страницы {url}: {e}")
        return None

def parse_gift_table(html_content):
    """Извлекает данные из таблицы подарков."""
    soup = BeautifulSoup(html_content, 'lxml')
    gift_table_wrap = soup.find('div', class_='tgme_gift_table_wrap')
    if not gift_table_wrap:
        print("Контейнер с таблицей подарков не найден.")
        return {}
    
    gift_table = gift_table_wrap.find('table', class_='tgme_gift_table')
    if not gift_table:
        print("Таблица с информацией о подарке не найдена.")
        return {}
    
    data = {}
    rows = gift_table.find('tbody').find_all('tr')
    for row in rows:
        header = row.find('th')
        value = row.find('td')
        if header and value:
            key = header.get_text(strip=True)
            if key == "Owner":
                owner_html = value.decode_contents()
                soup_owner = BeautifulSoup(owner_html, 'html.parser')
                img_tag = soup_owner.find('img')
                data['Owner_avatar'] = img_tag['src'].strip() if img_tag and img_tag.has_attr('src') else "https://i.getgems.io/pa4IG9_bFDXTUAXXqwq1M2OBNrplmfVaecyHGHoY3Po/rs:fill:512:512:1/g:ce/czM6Ly9nZXRnZW1zLXMzL3VzZXItbWVkaWEvZ2Vtcy80Ni53ZWJw"
                span_tag = soup_owner.find('span')
                data['Owner'] = span_tag.get_text(strip=True) if span_tag else "User"
            else:
                val = value.get_text(separator=" ", strip=True)
                mark_tag = value.find('mark')
                if mark_tag:
                    percent = mark_tag.get_text(strip=True).replace('%', '').strip()
                    name = val.replace(mark_tag.get_text(), '').strip()
                    try:
                        data[key] = {"trait_type": key, "value": name, "percent": float(percent)}
                    except ValueError:
                        data[key] = {"trait_type": key, "value": name, "percent": 0.0}
                else:
                    data[key] = {"trait_type": key, "value": val, "percent": 0.0}
    return data

def process_gift_data(gift_id, collection_name):
    """Собирает и обрабатывает все данные о подарке."""
    # URL для данных из fragment.com
    fragment_url = f"https://nft.fragment.com/gift/{collection_name.lower()}-{gift_id}"
    
    # URL для данных из t.me
    telegram_url = f"https://t.me/nft/{collection_name}-{gift_id}"
    
    # Получаем JSON данные из fragment.com
    try:
        response = requests.get(fragment_url, timeout=10)
        response.raise_for_status()
        fragment_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении данных с {fragment_url}: {e}")
        return {"error": f"Не удалось получить данные с fragment.com для {gift_id}"}
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON с {fragment_url}")
        return {"error": f"Неверный формат JSON с fragment.com для {gift_id}"}
    
    # Инициализируем gift_data
    gift_data = {}
    gift_data['name'] = fragment_data.get('name', '')
    gift_data['description'] = fragment_data.get('description', '')
    gift_data['image'] = fragment_data.get('image', '')
    gift_data['lottie'] = fragment_data.get('lottie', '')
    
    # Извлекаем атрибуты из fragment.com
    attributes = fragment_data.get('attributes', [])
    processed_attributes = []  # Новый список для атрибутов с процентами
    for attr in attributes:
        trait_type = attr.get('trait_type', '')
        value = attr.get('value', '')
        if trait_type and value:
            processed_attributes.append({
                'trait_type': trait_type,
                'value': value,
                'percent': 0.0  # Изначально без процентов
            })
    gift_data['attributes'] = processed_attributes  # Сохраняем для генерации страницы подарка
        
    # Извлекаем информацию о владельце из fragment.com
    original_details = fragment_data.get('original_details', {})
    gift_data['sender_name'] = original_details.get('sender_name', '')
    gift_data['sender_telegram_id'] = original_details.get('sender_telegram_id', '')
    gift_data['recipient_name'] = original_details.get('recipient_name', '')
    gift_data['recipient_telegram_id'] = original_details.get('recipient_telegram_id', '')
    gift_data['date'] = original_details.get('date', '')
    
    # Теперь всегда пытаемся получить данные из Telegram
    print(f"Пытаемся получить данные из Telegram для подарка {gift_id}...")
    html_content = fetch_gift_page(telegram_url)
    if html_content:
        telegram_data = parse_gift_table(html_content)
        gift_data['Owner'] = telegram_data.get('Owner', gift_data.get('recipient_name', 'User'))
        gift_data['Owner_avatar'] = telegram_data.get('Owner_avatar', "https://i.getgems.io/pa4IG9_bFDXTUAXXqwq1M2OBNrplmfVaecyHGHoY3Po/rs:fill:512:512:1/g:ce/czM6Ly9nZXRnZW1zLXMzL3VzZXItbWVkaWEvZ2Vtcy80Ni53ZWJw")
        
        # Интегрируем атрибуты из Telegram в gift_data['attributes']
        telegram_attributes = [v for k, v in telegram_data.items() if k not in ["Owner", "Owner_avatar"]]
        
        # Обновляем проценты редкости в существующих атрибутах
        for telegram_attr in telegram_attributes:
            trait_type = telegram_attr.get('trait_type', '')
            value = telegram_attr.get('value', '')
            percent = telegram_attr.get('percent', 0.0)
            # Ищем соответствующий trait_type и value в gift_data['attributes'] и обновляем процент
            for attr in gift_data['attributes']:
                if attr['trait_type'] == trait_type and attr['value'] == value:
                    attr['percent'] = percent
                    break
            else:
                # Если trait_type и value не найдены, добавляем новый атрибут
                gift_data['attributes'].append(telegram_attr)
    else:
        # Если не удалось получить данные из Telegram, используем данные из fragment.com для владельца
        gift_data['Owner'] = gift_data.get('recipient_name', 'User')
        gift_data['Owner_avatar'] = gift_data.get('Owner_avatar', "https://i.getgems.io/pa4IG9_bFDXTUAXXqwq1M2OBNrplmfVaecyHGHoY3Po/rs:fill:512:512:1/g:ce/czM6Ly9nZXRnZW1zLXMzL3VzZXItbWVkaWEvZ2Vtcy80Ni53ZWJw")
    
    # Добавляем ссылку на страницу подарка
    gift_data['gift_page'] = f"gifts/{collection_name}_{gift_id}.html"
    
    return gift_data

# Генерация главной страницы коллекции
def generate_main_page(gift_data, collection_name, output_file):
    """Генерирует главную страницу со списком подарков."""
    html_template_start = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>{collection_name}</title>
        <style>
            /* Стили для тёмной темы */
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #121212;
                color: #e0e0e0;
                margin: 0;
                padding: 0;
            }}
            .header {{
                background-color: #1f1f1f;
                padding: 20px;
                text-align: center;
                box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                position: relative;
            }}
            .header h1 {{
                margin: 0;
                color: #ffffff;
                font-size: 2em;
                letter-spacing: 2px;
            }}
            .controls {{
                display: flex;
                justify-content: center;
                gap: 20px;
                padding: 20px;
                flex-wrap: wrap;
            }}
            .controls input, .controls select {{
                padding: 10px;
                border-radius: 8px;
                border: none;
                font-size: 16px;
            }}
            .gift-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); /* Адаптивные колонки */
                gap: 25px;
                padding: 20px;
            }}
            .gift-card {{
                background-color: #1f1f1f;
                border-radius: 15px;
                box-shadow: 0 8px 16px rgba(0,0,0,0.3);
                overflow: hidden;
                text-align: center;
                transition: transform 0.3s, box-shadow 0.3s;
                cursor: pointer;
            }}
            .gift-card:hover {{
                transform: scale(1.05);
                box-shadow: 0 12px 24px rgba(0,0,0,0.4);
            }}
            .gift-card img {{
                width: 100%;
                height: auto;
            }}
            .gift-card h3 {{
                margin: 0;
                padding: 10px;
                color: #ffffff;
            }}
            a {{
                text-decoration: none;
                color: inherit;
            }}
            /* Адаптив для маленьких экранов */
            @media (max-width: 600px) {{
                .controls {{
                    flex-direction: column;
                    align-items: center;
                }}
                .gift-grid {{
                    grid-template-columns: repeat(2, minmax(100px, 1fr)); /* Адаптивные колонки */
                }}
            }}
        </style>
    </head>
    <body>
    <div class="header">
        <h1>{collection_name}</h1>
    </div>
    <div class="controls">
        <input type="text" id="searchInput" placeholder="Поиск по номеру подарка (например, {collection_name}-1)">
        <select id="sortSelect">
            <option value="default">Сортировка по редкости</option>
            <option value="asc">Редкость ↑</option>
            <option value="desc">Редкость ↓</option>
        </select>
    </div>
        <div class="gift-grid" id="giftGrid">
    """
    html_template_end = """
        </div>
        <script>
            const giftGrid = document.getElementById('giftGrid');
            const searchInput = document.getElementById('searchInput');
            const sortSelect = document.getElementById('sortSelect');

            // Функция для получения средней редкости подарка
            function getRarity(giftCard) {
                return parseFloat(giftCard.getAttribute('data-rarity')) || 0;
            }

            // Событие поиска
            searchInput.addEventListener('input', function() {
                const query = this.value.toLowerCase();
                const giftCards = giftGrid.getElementsByClassName('gift-card');
                Array.from(giftCards).forEach(function(card) {
                    const title = card.getAttribute('data-id').toLowerCase();
                    if (title.includes(query)) {
                        card.style.display = '';
                    } else {
                        card.style.display = 'none';
                    }
                });
            });

            // Событие сортировки
            sortSelect.addEventListener('change', function() {
                const giftCards = Array.from(giftGrid.getElementsByClassName('gift-card'));
                if (this.value === 'asc') {
                    giftCards.sort((a, b) => getRarity(a) - getRarity(b));
                } else if (this.value === 'desc') {
                    giftCards.sort((a, b) => getRarity(b) - getRarity(a));
                } else {
                    // По умолчанию сортировка по порядку
                    giftCards.sort((a, b) => a.getAttribute('data-original-order') - b.getAttribute('data-original-order'));
                }

                // Удаляем текущие карточки
                while (giftGrid.firstChild) {
                    giftGrid.removeChild(giftGrid.firstChild);
                }

                // Добавляем отсортированные карточки
                giftCards.forEach(function(card) {
                    giftGrid.appendChild(card);
                });
            });
        </script>
    </body>
    </html>
    """
    # Фильтрация ключей: исключаем те, которые содержат '_hash' или равны 'hash'
    filtered_gift_keys = [k for k in gift_data.keys() if not k.endswith('_hash') and k != 'hash']
    
    # Сортировка ключей
    try:
        sorted_gift_keys = sorted(filtered_gift_keys, key=lambda x: int(x.split('_')[-1]))
    except ValueError as e:
        print(f"Ошибка при сортировке ключей: {e}")
        sorted_gift_keys = [k for k in filtered_gift_keys]  # Без сортировки
    
    gift_cards_html = ""
    for index, key in enumerate(sorted_gift_keys):
        data = gift_data[key]
        if "error" in data:
            print(f"Пропуск подарка {key} из-за ошибки: {data['error']}")
            continue
        image_url = data.get('image', '')
        gift_page = data.get('gift_page', '#')
        name = data.get('name', 'Подарок')
        # Вычисляем средний процент редкости
        total_percent = 0
        count = 0
        for attr in data.get('attributes', []):
            percent = attr.get('percent', 0.0)
            total_percent += percent
            count += 1
        average_rarity = total_percent / count if count > 0 else 0
        # Добавляем data-атрибуты для сортировки и поиска
        gift_card_html = f"""
            <div class="gift-card" data-id="{key}" data-rarity="{average_rarity}" data-original-order="{index}">
                <a href="{gift_page}">
                    <img src="{image_url}" alt="{name}">
                    <h3>{name}</h3>
                </a>
            </div>
        """
        gift_cards_html += gift_card_html
    full_html = html_template_start + gift_cards_html + html_template_end
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(full_html)
    print(f"Главная страница создана или обновлена: {output_file}")

# Генерация отдельных страниц подарков
def generate_gift_pages(gift_data, collection_name, yandex_client, bucket_name):
    """Генерирует отдельные страницы для каждого подарка и загружает их на Yandex."""
    for gift_id, data in gift_data.items():
        if "error" in data:
            print(f"Пропуск подарка {gift_id} из-за ошибки: {data['error']}")
            continue
        gift_page = data.get('gift_page', '')
        if not gift_page:
            continue
        output_file = gift_page
        name = data.get('name', 'Подарок')
        description = data.get('description', '')
        owner_name = data.get('Owner', 'User')
        owner_avatar = data.get('Owner_avatar', 'https://i.getgems.io/pa4IG9_bFDXTUAXXqwq1M2OBNrplmfVaecyHGHoY3Po/rs:fill:512:512:1/g:ce/czM6Ly9nZXRnZW1zLXMzL3VzZXItbWVkaWEvZ2Vtcy80Ni53ZWJw')
        image_url = data.get('image', '')
        lottie_url = data.get('lottie', '')
        attributes = data.get('attributes', [])
        # back_button = '<a href="../index.html" class="back-button">Назад</a>'
        html_content = f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
            <meta charset="UTF-8">
            <title>{name}</title>
            <style>
                /* Ваши стили */
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background-color: #121212;
                    color: #e0e0e0;
                    margin: 0;
                    padding: 20px;
                }}
                .container {{
                    max-width: 800px;
                    margin: auto;
                    background-color: #1f1f1f;
                    padding: 20px;
                    border-radius: 15px;
                    box-shadow: 0 4px 8px rgba(0,0,0,0.3);
                    text-align: center;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                }}
                .back-button {{
                    background-color: #ff562200;
                    color: #fff;
                    padding: 10px 20px;
                    border-radius: 8px;
                    cursor: pointer;
                    font-size: 16px;
                    margin-bottom: 20px;
                    transition: background-color 0.3s;
                    border: 2px solid #585858;
                    text-decoration: none;
                    align-self: flex-start;
                }}
                .back-button:hover {{
                    background-color: #000000;
                }}
                .gift-title {{
                    color: #ffffff;
                    font-size: 2em;
                    margin: 20px 0;
                }}
                .animation-container {{
                    width: 400px;
                    height: 400px;
                    margin: auto;
                    position: relative;
                    overflow: hidden;
                }}
                .animation-container::before {{
                    content: '';
                    position: absolute;
                    top: -20px;
                    left: -20px;
                    right: -20px;
                    bottom: -20px;
                    background: radial-gradient(circle, rgba(255,255,255,0.1), rgba(0,0,0,0));
                    filter: blur(20px);
                    z-index: -1;
                }}
                .owner-box {{
                    background-color: #ff562200;
                    color: #fff;
                    padding: 10px 15px;
                    border-radius: 8px;
                    margin-top: 20px;
                    border: 2px solid #585858;
                }}
                .owner {{
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }}
                .owner img {{
                    width: 60px;
                    height: 60px;
                    border-radius: 50%;
                    object-fit: cover;
                    margin-right: 10px;
                }}
                .owner-name {{
                    font-size: 1.2em;
                    font-weight: bold;
                }}
                .description {{
                    margin-top: 20px;
                    font-size: 18px;
                    text-align: left;
                }}
                .attributes {{
                    display: flex;
                    justify-content: center;
                    flex-wrap: wrap;
                    margin-top: 20px;
                    margin-bottom: 20px;
                }}
                .attribute-box {{
                    background-color: #ff562200;
                    color: #fff;
                    padding: 10px 15px;
                    border-radius: 8px;
                    margin: 5px;
                    min-width: 100px;
                    text-align: center;
                    font-size: 16px;
                    border: 2px solid #585858;
                }}
                .attribute-box span {{
                    color: #FFD700; /* Золотой цвет для процентов */
                    font-weight: bold;
                }}
                @media (max-width: 600px) {{
                    .animation-container {{
                        width: 200px;
                        height: 200px;
                    }}
                    .description {{
                        font-size: 16px;
                    }}
                    .attribute-box {{
                        font-size: 14px;
                        min-width: 80px;
                    }}
                }}
            </style>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/lottie-web/5.7.13/lottie.min.js"></script>
        </head>
        <body>
            <div class="container">
                <a href="../{collection_name}.html" class="back-button">Назад</a>
                <h1 class="gift-title">{name}</h1>
                <div id="animationContainer" class="animation-container"></div>
                <p class="description">{description}</p>
                <div class="owner-box">
                    <div class="owner">
                        <img src="{owner_avatar}" alt="Аватар">
                        <div class="owner-name">{owner_name}</div>
                    </div>
                </div>
                <div class="attributes">
        """
        # Добавляем атрибуты
        for attr in attributes:
            trait_type = attr.get('trait_type', '')
            value = attr.get('value', '')
            percent = attr.get('percent', None)
            if percent is not None and percent != 0.0:
                html_content += f"""
                <div class="attribute-box">
                    <strong>{trait_type}</strong><br>{value}<br><span>{percent}%</span>
                </div>
                """
            else:
                html_content += f"""
                <div class="attribute-box">
                    <strong>{trait_type}</strong><br>{value}
                </div>
                """
    
        html_content += f"""
                </div>
            </div>
            <script>
                var animation = lottie.loadAnimation({{
                    container: document.getElementById('animationContainer'),
                    renderer: 'svg',
                    loop: true,
                    autoplay: true,
                    path: '{lottie_url}'
                }});
            </script>
        </body>
        </html>
        """
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"Страница подарка создана или обновлена: {output_file}")
        
        # Загрузка файла на Yandex Object Storage
        upload_to_yandex(yandex_client, bucket_name, output_file, gift_page)
        
        # Удаление локального файла после загрузки (опционально)
        os.remove(output_file)

# Генерация JSON-файлов для будущего использования
def generate_json_files(gift_data, collection_name, yandex_client, bucket_name):
    """Сохраняет данные подарков в JSON-файлы и загружает их на Yandex."""
    json_output_dir = 'json'
    os.makedirs(json_output_dir, exist_ok=True)
    for gift_id, data in gift_data.items():
        if "error" in data:
            continue
        json_file = os.path.join(json_output_dir, f"{collection_name}_{gift_id}.json")
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        # Загрузка JSON-файла на Yandex
        object_key = f"json/{collection_name}_{gift_id}.json"
        upload_to_yandex(yandex_client, bucket_name, json_file, object_key)
        # Удаление локального JSON-файла после загрузки (опционально)
        os.remove(json_file)

def main():
    # Загрузка конфигурации
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    collections = config.get('collections', [])
    yandex_config = config.get('yandex', {})
    yandex_id = yandex_config.get('id')
    yandex_key = yandex_config.get('key')
    bucket_name = yandex_config.get('bucket_name')
    interval_seconds = config.get('interval_seconds', 60)
    thread_workers = config.get('thread_workers', 20)
    
    # Проверка наличия необходимых параметров
    if not all([yandex_id, yandex_key, bucket_name]):
        print("Отсутствуют необходимые параметры для Yandex Cloud в config.json.")
        return
    
    # Инициализируем Yandex клиент
    yandex_client = init_yandex_client(yandex_config)
    
    # Загрузка или инициализация данных
    data_file = "all_collections_data.json"
    if os.path.exists(data_file):
        with open(data_file, "r", encoding="utf-8") as f:
            all_data = json.load(f)
        print(f"Загружено данные из {data_file}.")
    else:
        all_data = {}
        print(f"Файл данных {data_file} не найден. Начинаем с пустого набора данных.")
    
    while True:
        print("\nНачинается цикл проверки и парсинга коллекций...")
        with ThreadPoolExecutor(max_workers=thread_workers) as executor:
            future_to_gift = {}
            for collection in collections:
                collection_name = collection.get('name')
                start_id = collection.get('start_id')
                end_id = collection.get('end_id')
                
                # Создание ключа для коллекции
                collection_key = collection_name
                
                # Инициализация данных коллекции если необходимо
                if collection_key not in all_data:
                    all_data[collection_key] = {}
                
                for gift_id in range(start_id, end_id + 1):
                    key = f"{collection_name}_{gift_id}"
                    future = executor.submit(process_gift_data, gift_id, collection_name)
                    future_to_gift[future] = (collection_key, key)
            
            for future in as_completed(future_to_gift):
                collection_key, key = future_to_gift[future]
                try:
                    gift_data = future.result()
                    if gift_data and "error" not in gift_data:
                        new_hash = get_content_hash(gift_data)
                        old_hash = all_data[collection_key].get(f"{key}_hash", "")
                        if has_changed(old_hash, new_hash):
                            all_data[collection_key][key] = gift_data
                            all_data[collection_key][f"{key}_hash"] = new_hash
                            print(f"Обновление подарка: {key}")
                        else:
                            print(f"Подарок не изменился: {key}")
                    else:
                        print(f"Ошибка при получении данных для {key}: {gift_data.get('error', 'Неизвестная ошибка')}")
                except Exception as e:
                    print(f"Исключение при обработке подарка {key}: {e}")
        
        # Сохранение обновлённых данных
        with open(data_file, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        print(f"Данные сохранены в {data_file}.")
        
        # Генерация страниц и загрузка на Yandex
        for collection in collections:
            collection_name = collection.get('name')
            collection_key = collection_name
            gift_data = all_data.get(collection_key, {})
            # Генерация главной страницы
            main_page_file = f"{collection_name}.html"
            generate_main_page(gift_data, collection_name, main_page_file)
            # Загрузка главной страницы на Yandex
            upload_to_yandex(yandex_client, bucket_name, main_page_file, f"{collection_name}.html")
            os.remove(main_page_file)  # Удаление локального файла после загрузки
            
            # Генерация страниц подарков
            generate_gift_pages(gift_data, collection_name, yandex_client, bucket_name)
            
            # Генерация JSON-файлов
            generate_json_files(gift_data, collection_name, yandex_client, bucket_name)
        
        print(f"Ожидание {interval_seconds} секунд до следующей проверки...")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    main()
