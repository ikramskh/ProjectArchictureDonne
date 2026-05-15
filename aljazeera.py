import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os
import xml.etree.ElementTree as ET
import re
from kafka import KafkaProducer

HEADERS = {'User-Agent': 'Mozilla/5.0'}

SOURCE = {
    "name": "Al Jazeera",
    "base_url": "https://www.aljazeera.com",
    "feed_url": "https://www.aljazeera.com/news-sitemap.xml"
}

#OUTPUT_FOLDER = "data/raw/aljazeera"
#STATE_FILE = "data/raw/aljazeera_state.json"
#os.makedirs(OUTPUT_FOLDER, exist_ok=True)
BASE_DIR = "/opt/airflow/data/raw"
os.makedirs(BASE_DIR, exist_ok=True)
STATE_FILE = os.path.join(BASE_DIR, "aljazeera_state.json")

# =========================================
# INITIALISATION KAFKA
# =========================================
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    TOPIC_NAME = "news_articles"
    print("Connexion Kafka réussie !")
except Exception as e:
    print(f"Erreur Kafka : {e}")
    exit(1)

def get_last_processed_date():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f).get("last_date", "")
    return ""

def save_last_processed_date(date_str):
    with open(STATE_FILE, 'w') as f:
        json.dump({"last_date": date_str}, f)

def get_latest_links_sitemap(source):
    try:
        response = requests.get(source["feed_url"], headers=HEADERS, timeout=10)
        response.raise_for_status()
        root = ET.fromstring(response.content)

        def strip_ns(tag):
            return tag.split('}')[-1] if '}' in tag else tag

        entries = []
        for url_el in root.iter():
            if strip_ns(url_el.tag) == 'url':
                loc = ''
                pub_date = ''
                for child in url_el.iter():
                    tag = strip_ns(child.tag)
                    if tag == 'loc' and child.text:
                        loc = child.text.strip()
                    if tag == 'publication_date' and child.text:
                        pub_date = child.text.strip()
                    if tag == 'lastmod' and child.text and not pub_date:
                        pub_date = child.text.strip()
                if loc and pub_date:
                    entries.append((pub_date, loc))

        # Trier du plus récent au plus ancien
        entries.sort(key=lambda x: x[0], reverse=True)
        return entries
    except Exception as e:
        print(f"Erreur sitemap {source['name']} : {e}")
        return []

def scrape_article(url, source_name):
    # (Logique de scraping inchangée par rapport à ton code original)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        titre = ""
        title_tag = soup.find('h1')
        if title_tag:
            titre = title_tag.get_text(strip=True)

        auteur = "Anonyme"
        meta_author = soup.find('meta', attrs={'name': 'author'})
        if meta_author and meta_author.get('content'):
            auteur = meta_author['content']
        else:
            author_elem = (
                soup.find(class_='article-author-name') or
                soup.find(class_='author-link') or
                soup.find(class_='article__author-name') or
                soup.select_one('[class*="ArticleAuthor"]') or
                soup.find(attrs={'data-testid': 'article-author'})
            )
            if author_elem:
                auteur = author_elem.get_text(strip=True)
            else:
                ld_tag = soup.find('script', type='application/ld+json')
                if ld_tag:
                    try:
                        ld = json.loads(ld_tag.string)
                        if isinstance(ld, list): ld = ld[0]
                        author = ld.get('author', {})
                        if isinstance(author, list):
                            auteur = ', '.join(a.get('name', '') for a in author if a.get('name'))
                        elif isinstance(author, dict):
                            auteur = author.get('name')
                    except:
                        pass

        # =====================================
        # DATE (VERSION ULTRA ROBUSTE)
        # =====================================
        
        date_pub = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        raw_date = None

        # 1. Chercher dans les balises meta classiques
        meta_date = (
            soup.find('meta', property='article:published_time') or
            soup.find('meta', attrs={'itemprop': 'datePublished'}) or
            soup.find('meta', attrs={'name': 'pubdate'}) or
            soup.find('meta', attrs={'name': 'DC.date.issued'})
        )
        if meta_date and meta_date.get('content'):
            raw_date = meta_date['content']

        # 2. Chercher dans la balise HTML <time>
        if not raw_date:
            time_tag = soup.find('time')
            if time_tag and time_tag.get('datetime'):
                raw_date = time_tag['datetime']

        # 3. Parcourir TOUS les scripts JSON-LD de la page (indépendant de la section auteur)
        if not raw_date:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    ld_content = json.loads(script.string)
                    # Cas où le JSON est une liste
                    if isinstance(ld_content, list):
                        for item in ld_content:
                            if isinstance(item, dict) and 'datePublished' in item:
                                raw_date = item['datePublished']
                                break
                    # Cas où le JSON est un dictionnaire (gère le @graph des Live Blogs Al Jazeera)
                    elif isinstance(ld_content, dict):
                        if 'datePublished' in ld_content:
                            raw_date = ld_content['datePublished']
                        elif '@graph' in ld_content:
                            for item in ld_content['@graph']:
                                if 'datePublished' in item:
                                    raw_date = item['datePublished']
                                    break
                except:
                    continue
                if raw_date:
                    break

        if raw_date:
            match = re.search(r'(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2}:\d{2})', raw_date)
            if match:
                date_pub = f"{match.group(1)} {match.group(2)}"
            else:
                date_pub = raw_date

        categorie = "news"
        try:
            parts = url.split('/')
            if len(parts) > 3: categorie = parts[3]
        except:
            pass

        paragraphes = soup.select('div.article__content p') or soup.find_all('p')
        contenu = ' '.join(p.get_text(strip=True) for p in paragraphes)

        return {
            "titre": titre,
            "auteur": auteur,
            "date_publication": date_pub,
            "categorie": categorie,
            "contenu": contenu,
            "source": source_name,
            "url": url
        }
    except Exception as e:
        print(f"Erreur scraping {url}: {e}")
        return None

if __name__ == "__main__":
    print("===== Al Jazeera (Incremental Fetch) =====")
    last_date = get_last_processed_date()
    entries = get_latest_links_sitemap(SOURCE)
    
    new_articles = []
    newest_date = last_date

    for pub_date, url in entries:
        # On ne traite que les articles plus récents que la dernière exécution
        if pub_date <= last_date:
            break # Puisque c'est trié, on s'arrête dès qu'on tombe sur un ancien article

        print(f"Nouvel article détecté : {url}")
        data = scrape_article(url, SOURCE["name"])
        
        if data:
            new_articles.append(data)

            producer.send(TOPIC_NAME, value=data)
            print(f"Envoyé à Kafka : {data['titre'][:60]}...")

            if pub_date > newest_date:
                newest_date = pub_date

    if new_articles:
        #output_file = os.path.join(OUTPUT_FOLDER, f"aljazeera_news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        #with open(output_file, 'w', encoding='utf-8') as f:
        #    json.dump(new_articles, f, ensure_ascii=False, indent=4)
        
        save_last_processed_date(newest_date)
        producer.flush()
        print(f"{len(new_articles)} nouveaux articles sauvegardés.")
    else:
        print("Aucun nouvel article depuis la dernière exécution.")