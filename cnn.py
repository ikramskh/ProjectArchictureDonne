import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import time
import os
import xml.etree.ElementTree as ET
from kafka import KafkaProducer
from prometheus_client import start_http_server, Gauge

HEADERS = {'User-Agent': 'Mozilla/5.0'}

SOURCE = {
    "name": "CNN",
    "base_url": "https://edition.cnn.com",
    "feed_url": "https://www.cnn.com/sitemap/news.xml"
}

#OUTPUT_FOLDER = "data/raw/cnn_stream"
#os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# =========================================
# INITIALISATION KAFKA
# =========================================
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'], # Utilise kafka:9092 pour Docker
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    TOPIC_NAME = "news_articles"
    print("Connexion Kafka réussie !")
except Exception as e:
    print(f"Erreur Kafka : {e}")
    exit(1)

def scrape_article(url, source_name):
    # (Logique de scraping inchangée)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        titre = "Sans titre"
        title_tag = soup.find('h1')
        if title_tag:
            titre = title_tag.get_text(strip=True)

        auteur = "Anonyme"
        meta_author = soup.find('meta', attrs={'name': 'author'})
        if meta_author and meta_author.get('content'):
            auteur = meta_author['content']
        else:
            author_elem = (
                soup.select_one('[data-component-name="byline"] .byline__name') or
                soup.select_one('.byline__name') or
                soup.select_one('.headline__sub-description') or
                soup.select_one('[class*="byline"]') or
                soup.find('span', class_=lambda c: c and 'author' in c.lower()) or
                soup.find(attrs={'data-testid': 'byline'})
            )
            if author_elem:
                auteur = author_elem.get_text(strip=True)

        date_pub = datetime.now().isoformat()
        meta_date = soup.find('meta', attrs={'itemprop': 'datePublished'}) or soup.find('meta', property='article:published_time')
        if meta_date and meta_date.get('content'):
            date_pub = meta_date['content']

        categorie = "news"
        try:
            parts = url.split('/')
            if len(parts) > 6: categorie = parts[6]
        except:
            pass

        paragraphes = soup.select('div.article__content p') or soup.find_all('p')
        contenu = ' '.join(p.get_text(strip=True) for p in paragraphes)

        ARTICLES_PROCESSED.inc()

        return {
            "titre": titre,
            "auteur": auteur,
            "date_publication": date_pub,
            "categorie": categorie,
            "contenu": contenu,
            "source": source_name,
            "url": url,
            "scraped_at": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Erreur scraping {url}: {e}")
        return None

def get_latest_links_sitemap(source, limit=None):
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
                for child in url_el.iter():
                    if strip_ns(child.tag) == 'loc' and child.text:
                        loc = child.text.strip()
                        break  
                        
                if loc and not any(ext in loc.lower() for ext in ['.jpg', '.jpeg', '.png', 'media.cnn.com']):
                    entries.append(loc)
        
        if limit is not None:
            return entries[:limit]
        return entries

    except Exception as e:
        print(f"Erreur sitemap {source['name']} : {e}")
        return []

def process_articles(links, seen_urls):
    """Traite, scrape et envoie vers Kafka une liste d'URLs si elles ne sont pas déjà vues."""
    for url in links:
        if url not in seen_urls:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping : {url}")
            data = scrape_article(url, SOURCE["name"])
            
            if data:
                # =========================================
                # ENVOI KAFKA
                # =========================================
                producer.send(TOPIC_NAME, value=data)
                print(f"Envoyé à Kafka : {data['titre'][:60]}...")

                # =========================================
                # SAUVEGARDE JSON (DÉSACTIVÉE / EN COMMENTAIRE)
                # =========================================
                """
                # Génération du nom de fichier
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"cnn_{timestamp}_{len(seen_urls)}.json" # Ajout d'un index pour éviter les collisions
                output_file = os.path.join(OUTPUT_FOLDER, filename)
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                
                print(f"Sauvegardé : {data['titre'][:60]}...")
                """
            
            seen_urls.add(url)
            time.sleep(1) # Petit délai pour être poli avec le serveur

# On crée une jauge qui vaut 1 si le script tourne, et on compte les articles envoyés
SCRIPT_STATUS = Gauge('news_script_status', 'Statut du script (1=Running)')
ARTICLES_PROCESSED = Gauge('news_articles_total', 'Nombre total d\'articles envoyés à Kafka')

if __name__ == "__main__":

    start_http_server(8001)  # Démarre le serveur Prometheus sur le port 8000
    SCRIPT_STATUS.set(1)

    print("===== CNN (Récupération de l'historique + Streaming) =====")
    seen_urls = set()
    
    # ==========================================================
    # ÉTAPE 1 : RÉCUPÉRATION DE L'HISTORIQUE (Initialisation)
    # ==========================================================
    print("Récupération des articles existants...")
    initial_links = get_latest_links_sitemap(SOURCE, limit=20) 
    process_articles(initial_links, seen_urls)

    # On vide le buffer Kafka pour être sûr que tout l'historique part maintenant
    producer.flush()
    print("Historique poussé vers Kafka avec succès.")

    print(f"\nInitialisation terminée : {len(seen_urls)} articles récupérés.")
    print("Passage en mode Streaming. En attente des Breaking News...\n")

    # ==========================================================
    # ÉTAPE 2 : MODE STREAMING (Boucle infinie)
    # ==========================================================
    while True:
        time.sleep(60)
        
        new_links = get_latest_links_sitemap(SOURCE, limit=10)
        process_articles(new_links, seen_urls)
