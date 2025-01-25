
import requests
from bs4 import BeautifulSoup
import json
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading

def load_config(config_path="config.yaml"):
    """Загружает конфигурацию из YAML файла и переменных окружения."""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # Перезаписываем конфигурации Yandex Cloud переменными окружения для безопасности
    config['yandex_cloud']['access_key_id'] = os.getenv('YC_ACCESS_KEY_ID', config['yandex_cloud']['access_key_id'])
    config['yandex_cloud']['secret_access_key'] = os.getenv('YC_SECRET_ACCESS_KEY', config['yandex_cloud']['secret_access_key'])
    config['yandex_cloud']['bucket_name'] = os.getenv('YC_BUCKET_NAME', config['yandex_cloud']['bucket_name'])
    config['yandex_cloud']['region'] = os.getenv('YC_REGION', config['yandex_cloud'].get('region', 'ru-central1'))
    config['yandex_cloud']['gifts_folder'] = os.getenv('YC_GIFTS_FOLDER', config['yandex_cloud'].get('gifts_folder', 'gifts'))
    config['yandex_cloud']['json_folder'] = os.getenv('YC_JSON_FOLDER', config['yandex_cloud'].get('json_folder', 'json_data'))
    
    return config

def initialize_s3_client(yc_config):
    """Инициализирует клиента S3 для Yandex Cloud Storage."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=yc_config['access_key_id'],
        aws_secret_access_key=yc_config['secret_access_key'],
        endpoint_url="https://storage.yandexcloud.net",  # Endpoint YCS
        region_name=yc_config.get('region', 'ru-central1')
    )
    return s3_client

def upload_file_to_ycs(s3_client, file_path, bucket_name, object_name):
    """Загружает файл в Yandex Cloud Storage."""
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Файл {file_path} успешно загружен в {bucket_name}/{object_name}.")
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except NoCredentialsError:
        print("Не удалось найти учетные данные для Yandex Cloud Storage.")
    except ClientError as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")

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
                data['Owner_avatar'] = img_tag['src'].strip() if img_tag and img_tag.has_attr('src') else "https://default-avatar.url/placeholder.png"
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
        print(f"Ошибка при декодировании JSON с {fragment_url}")
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
        gift_data['Owner_avatar'] = telegram_data.get('Owner_avatar', "https://default-avatar.url/placeholder.png")
        
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
        gift_data['Owner_avatar'] = gift_data.get('Owner_avatar', "https://default-avatar.url/placeholder.png")
    
    # Добавляем ссылку на страницу подарка
    gift_data['gift_page'] = f"gifts/{collection_name}_{gift_id}.html"
    
    return gift_data

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
            .back-button {{
                position: absolute;
                left: 20px;
                top: 20px;
                background-color: #ff562200;
                color: #fff;
                padding: 10px 20px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 16px;
                transition: background-color 0.3s;
                border: 2px solid #585858;
                text-decoration: none;
            }}
            
            .back-button:hover {{
                background-color: #000000; /* Цвет кнопки при наведении */
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
                display: grid;
                grid-template-columns: repeat(2, minmax(100px, 1fr)); /* Адаптивные колонки */
                gap: 25px;
                padding: 20px;
            }}
            }}
        </style>
    </head>
    <body>
    <div class="header">
        <h1>{collection_name}</h1>
        <a href="index.html" class="back-button">Назад</a> <!-- Кнопка "Назад" -->
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
    gift_cards_html = ""
    sorted_gift_keys = sorted(gift_data.keys(), key=lambda x: int(x.split('_')[-1]))
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
    print(f"Главная страница создана: {output_file}")

def generate_gift_page(gift_data, collection_name, json_folder='json', gifts_folder='gifts'):
    """Генерирует отдельную страницу для одного подарка и сохраняет JSON."""
    gift_id = gift_data.get('gift_id')
    if not gift_id:
        print("Отсутствует ID подарка. Пропуск.")
        return
    
    gift_page = gift_data.get('gift_page', '')
    if not gift_page:
        print("Отсутствует ссылка на страницу подарка. Пропуск.")
        return
    
    output_file = os.path.join(gifts_folder, f"{collection_name}_{gift_id}.html")
    name = gift_data.get('name', 'Подарок')
    description = gift_data.get('description', '')
    owner_name = gift_data.get('Owner', 'User')
    owner_avatar = gift_data.get('Owner_avatar', 'https://default-avatar.url/placeholder.png')
    image_url = gift_data.get('image', '')
    lottie_url = gift_data.get('lottie', '')
    attributes = gift_data.get('attributes', [])
    
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
    
    # Сохранение JSON-файла
    json_data = gift_data.copy()
    json_data.pop('gift_page', None)  # Исключаем ссылку на страницу из JSON
    json_output_file = os.path.join(json_folder, f"{collection_name}_{gift_id}.json")
    with open(json_output_file, "w", encoding="utf-8") as jf:
        json.dump(json_data, jf, ensure_ascii=False, indent=4)
    print(f"JSON-файл сохранен: {json_output_file}")

def generate_main_page_parallel(gift_data, collection_name, output_file, s3_client, bucket_name, main_folder):
    """Генерирует главную страницу и загружает ее в Yandex Cloud Storage."""
    generate_main_page(gift_data, collection_name, output_file)
    # Загрузка главной страницы в YCS
    upload_file_to_ycs(s3_client, output_file, bucket_name, f"{main_folder}/{os.path.basename(output_file)}")

def generate_gift_page_parallel(gift_data, collection_name, json_folder, gifts_folder, s3_client, bucket_name, gifts_remote_folder, json_remote_folder):
    """Генерирует страницу подарка, сохраняет JSON и загружает их в YCS."""
    generate_gift_page(gift_data, collection_name, json_folder, gifts_folder)
    gift_id = gift_data.get('gift_id')
    if not gift_id:
        return
    gift_page_filename = f"{collection_name}_{gift_id}.html"
    gift_page_path = os.path.join(gifts_folder, gift_page_filename)
    json_filename = f"{collection_name}_{gift_id}.json"
    json_path = os.path.join(json_folder, json_filename)
    
    # Загрузка HTML страницы
    upload_file_to_ycs(s3_client, gift_page_path, bucket_name, f"{gifts_remote_folder}/{gift_page_filename}")
    # Загрузка JSON файла
    upload_file_to_ycs(s3_client, json_path, bucket_name, f"{json_remote_folder}/{json_filename}")

def process_collection(collection, s3_client, bucket_name, main_folder, gifts_folder, json_folder, max_workers=100):
    """Обрабатывает всю коллекцию с использованием многопоточности."""
    collection_name = collection['name']
    start_id = collection['start_id']
    end_id = collection['end_id']
    data_file = os.path.join(json_folder, f"{collection_name}_gift_data.json")
    
    # Загрузка существующих данных, если есть
    if os.path.exists(data_file):
        with open(data_file, "r", encoding="utf-8") as f:
            try:
                existing_data = json.load(f)
                print(f"Загружено {len(existing_data)} подарков из {data_file}.")
            except json.JSONDecodeError:
                print(f"Ошибка декодирования JSON из {data_file}. Создание нового.")
                existing_data = {}
    else:
        print(f"Файл данных {data_file} не найден. Начинается создание нового.")
        existing_data = {}
    
    # Подготовка списка задач
    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_gift_id = {}
        for gift_id in range(start_id, end_id + 1):
            key = f"{collection_name}_{gift_id}"
            future = executor.submit(process_gift_data, gift_id, collection_name)
            future_to_gift_id[future] = key
        
        for future in as_completed(future_to_gift_id):
            key = future_to_gift_id[future]
            try:
                gift_data = future.result()
                if gift_data and "error" not in gift_data:
                    # Добавляем gift_id для дальнейшего использования
                    gift_data['gift_id'] = gift_id
                    existing_data[key] = gift_data
                    # Генерация и загрузка страницы и JSON
                    executor.submit(
                        generate_gift_page_parallel,
                        gift_data,
                        collection_name,
                        json_folder,
                        gifts_folder,
                        s3_client,
                        bucket_name,
                        'gifts',   # Папка в YCS для подарков
                        'json_data'  # Папка в YCS для JSON
                    )
                else:
                    print(f"Ошибка при парсинге подарка {key}: {gift_data.get('error', 'Неизвестная ошибка')}")
            except Exception as exc:
                print(f"Подарок {key} сгенерировал исключение: {exc}")
    
    # Сохранение обновленных данных
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=4)
    print(f"Данные сохранены в {data_file}.")
    
    # Генерация и загрузка главной страницы
    main_page_file = f"{collection_name}.html"
    generate_main_page_parallel(existing_data, collection_name, main_page_file, s3_client, bucket_name, main_folder)

def main():
    config = load_config()
    yc_config = config['yandex_cloud']
    s3_client = initialize_s3_client(yc_config)
    bucket_name = yc_config['bucket_name']
    main_folder = yc_config.get('main_folder', 'main_pages')
    gifts_folder = yc_config.get('gifts_folder', 'gifts')
    json_folder = yc_config.get('json_folder', 'json_data')
    
    # Создание локальных папок, если они не существуют
    os.makedirs(gifts_folder, exist_ok=True)
    os.makedirs(json_folder, exist_ok=True)
    
    collections = config['collections']
    
    for collection in collections:
        print(f"Начинаем обработку коллекции {collection['name']}...")
        start_time = time.time()
        process_collection(collection, s3_client, bucket_name, main_folder, gifts_folder, json_folder, max_workers=100)
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"Обработка коллекции {collection['name']} завершена за {elapsed/60:.2f} минут.")
    
    print("Все коллекции обработаны.")

if __name__ == "__main__":
    main()
