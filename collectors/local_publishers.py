from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import feedparser
import requests

from config.config import REQUEST_WORKERS


RSS_FEEDS = [
    # --- The Hindu (national + city) ---
    {"url": "https://www.thehindu.com/news/national/feeder/default.rss", "source": "the_hindu_national"},
    {"url": "https://www.thehindu.com/news/cities/Delhi/feeder/default.rss", "source": "the_hindu_delhi", "state_hint": "delhi", "district_hint": "new delhi"},
    {"url": "https://www.thehindu.com/news/cities/Hyderabad/feeder/default.rss", "source": "the_hindu_hyderabad", "state_hint": "telangana", "district_hint": "hyderabad"},
    {"url": "https://www.thehindu.com/news/cities/chennai/feeder/default.rss", "source": "the_hindu_chennai", "state_hint": "tamil nadu", "district_hint": "chennai"},
    {"url": "https://www.thehindu.com/news/cities/bangalore/feeder/default.rss", "source": "the_hindu_bengaluru", "state_hint": "karnataka", "district_hint": "bengaluru urban"},
    {"url": "https://www.thehindu.com/news/cities/Kozhikode/feeder/default.rss", "source": "the_hindu_kozhikode", "state_hint": "kerala", "district_hint": "kozhikode"},
    {"url": "https://www.thehindu.com/news/cities/Kochi/feeder/default.rss", "source": "the_hindu_kochi", "state_hint": "kerala", "district_hint": "ernakulam"},
    {"url": "https://www.thehindu.com/news/cities/Thiruvananthapuram/feeder/default.rss", "source": "the_hindu_tvm", "state_hint": "kerala", "district_hint": "thiruvananthapuram"},
    {"url": "https://www.thehindu.com/news/cities/Visakhapatnam/feeder/default.rss", "source": "the_hindu_vizag", "state_hint": "andhra pradesh", "district_hint": "visakhapatnam"},
    {"url": "https://www.thehindu.com/news/cities/Coimbatore/feeder/default.rss", "source": "the_hindu_coimbatore", "state_hint": "tamil nadu", "district_hint": "coimbatore"},
    {"url": "https://www.thehindu.com/news/cities/Madurai/feeder/default.rss", "source": "the_hindu_madurai", "state_hint": "tamil nadu", "district_hint": "madurai"},
    # --- Indian Express (national + city) ---
    {"url": "https://indianexpress.com/section/india/feed/", "source": "indian_express_india"},
    {"url": "https://indianexpress.com/section/cities/delhi/feed/", "source": "indian_express_delhi", "state_hint": "delhi", "district_hint": "new delhi"},
    {"url": "https://indianexpress.com/section/cities/mumbai/feed/", "source": "indian_express_mumbai", "state_hint": "maharashtra", "district_hint": "mumbai"},
    {"url": "https://indianexpress.com/section/cities/chandigarh/feed/", "source": "indian_express_chandigarh", "state_hint": "chandigarh", "district_hint": "chandigarh"},
    {"url": "https://indianexpress.com/section/cities/pune/feed/", "source": "indian_express_pune", "state_hint": "maharashtra", "district_hint": "pune"},
    {"url": "https://indianexpress.com/section/cities/kolkata/feed/", "source": "indian_express_kolkata", "state_hint": "west bengal", "district_hint": "kolkata"},
    {"url": "https://indianexpress.com/section/cities/ahmedabad/feed/", "source": "indian_express_ahmedabad", "state_hint": "gujarat", "district_hint": "ahmedabad"},
    {"url": "https://indianexpress.com/section/cities/lucknow/feed/", "source": "indian_express_lucknow", "state_hint": "uttar pradesh", "district_hint": "lucknow"},
    {"url": "https://indianexpress.com/section/cities/hyderabad/feed/", "source": "indian_express_hyderabad", "state_hint": "telangana", "district_hint": "hyderabad"},
    # --- Times of India (city feeds via known feed IDs) ---
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719161.cms", "source": "toi_india"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719148.cms", "source": "toi_delhi", "state_hint": "delhi", "district_hint": "new delhi"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms", "source": "toi_mumbai", "state_hint": "maharashtra", "district_hint": "mumbai"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/2647163.cms", "source": "toi_kolkata", "state_hint": "west bengal", "district_hint": "kolkata"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719168.cms", "source": "toi_chennai", "state_hint": "tamil nadu", "district_hint": "chennai"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/2971582.cms", "source": "toi_bengaluru", "state_hint": "karnataka", "district_hint": "bengaluru urban"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/1221313810.cms", "source": "toi_hyderabad", "state_hint": "telangana", "district_hint": "hyderabad"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/2894701.cms", "source": "toi_lucknow", "state_hint": "uttar pradesh", "district_hint": "lucknow"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719166.cms", "source": "toi_ahmedabad", "state_hint": "gujarat", "district_hint": "ahmedabad"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719171.cms", "source": "toi_patna", "state_hint": "bihar", "district_hint": "patna"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719175.cms", "source": "toi_bhopal", "state_hint": "madhya pradesh", "district_hint": "bhopal"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719178.cms", "source": "toi_jaipur", "state_hint": "rajasthan", "district_hint": "jaipur"},
    {"url": "https://timesofindia.indiatimes.com/rssfeeds/4719167.cms", "source": "toi_chandigarh", "state_hint": "chandigarh", "district_hint": "chandigarh"},
    # --- Hindustan Times ---
    {"url": "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml", "source": "hindustan_times_india"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/lucknow/rssfeed.xml", "source": "ht_lucknow", "state_hint": "uttar pradesh", "district_hint": "lucknow"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/delhi/rssfeed.xml", "source": "ht_delhi", "state_hint": "delhi", "district_hint": "new delhi"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/mumbai/rssfeed.xml", "source": "ht_mumbai", "state_hint": "maharashtra", "district_hint": "mumbai"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/pune/rssfeed.xml", "source": "ht_pune", "state_hint": "maharashtra", "district_hint": "pune"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/chandigarh/rssfeed.xml", "source": "ht_chandigarh", "state_hint": "chandigarh", "district_hint": "chandigarh"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/patna/rssfeed.xml", "source": "ht_patna", "state_hint": "bihar", "district_hint": "patna"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/ranchi/rssfeed.xml", "source": "ht_ranchi", "state_hint": "jharkhand", "district_hint": "ranchi"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/jaipur/rssfeed.xml", "source": "ht_jaipur", "state_hint": "rajasthan", "district_hint": "jaipur"},
    {"url": "https://www.hindustantimes.com/feeds/rss/cities/bhopal/rssfeed.xml", "source": "ht_bhopal", "state_hint": "madhya pradesh", "district_hint": "bhopal"},
    # --- NDTV ---
    {"url": "https://feeds.feedburner.com/ndtvnews-india-news", "source": "ndtv_india"},
    {"url": "https://feeds.feedburner.com/ndtvnews-cities-news", "source": "ndtv_cities"},
    # --- Deccan Herald (Karnataka) ---
    {"url": "https://www.deccanherald.com/rss-feeds-dh", "source": "deccan_herald_karnataka", "state_hint": "karnataka"},
    # --- The Tribune (Punjab/Haryana/HP/Chandigarh) ---
    {"url": "https://www.tribuneindia.com/rss/feed/1", "source": "tribune_news", "state_hint": "punjab"},
    # --- Scroll.in ---
    {"url": "https://scroll.in/rss", "source": "scroll_india"},
    # --- The Wire ---
    {"url": "https://thewire.in/rss", "source": "the_wire"},
    # --- Firstpost ---
    {"url": "https://www.firstpost.com/rss/india.xml", "source": "firstpost_india"},
    # --- Outlook India ---
    {"url": "https://www.outlookindia.com/rss/main/news", "source": "outlook_india"},
    # --- News18 regional ---
    {"url": "https://www.news18.com/rss/india.xml", "source": "news18_india"},
    # --- Mathrubhumi (Kerala) ---
    {"url": "https://english.mathrubhumi.com/rss/news", "source": "mathrubhumi_kerala", "state_hint": "kerala"},
    # --- Deccan Chronicle (Andhra/Telangana) ---
    {"url": "https://www.deccanchronicle.com/rss_feed/", "source": "deccan_chronicle", "state_hint": "andhra pradesh"},
    # --- Telangana Today ---
    {"url": "https://telanganatoday.com/feed", "source": "telangana_today", "state_hint": "telangana"},
    # --- Hans India (AP/Telangana) ---
    {"url": "https://www.thehansindia.com/rss/india.xml", "source": "hans_india", "state_hint": "andhra pradesh"},
    # --- NE Now (Northeast India) ---
    {"url": "https://nenow.in/feed", "source": "ne_now_northeast", "state_hint": "assam"},
    # --- Eastmojo (Northeast) ---
    {"url": "https://www.eastmojo.com/feed/", "source": "eastmojo_northeast", "state_hint": "assam"},
    # --- The Shillong Times (Meghalaya) ---
    {"url": "https://www.theshillongtimes.com/feed/", "source": "shillong_times", "state_hint": "meghalaya"},
    # --- Nagaland Post ---
    {"url": "https://www.nagalandpost.com/feed/", "source": "nagaland_post", "state_hint": "nagaland"},
    # --- Sikkim Express ---
    {"url": "https://sikkimexpress.com/feed/", "source": "sikkim_express", "state_hint": "sikkim"},
    # --- The Sentinel (Assam) ---
    {"url": "https://www.sentinelassam.com/feeds/", "source": "sentinel_assam", "state_hint": "assam"},
    # --- Tripura Tribune ---
    {"url": "https://tripuratribune.in/feed/", "source": "tripura_tribune", "state_hint": "tripura"},
    # --- Manipur Tribune ---
    {"url": "https://www.manipurtribune.com/feed/", "source": "manipur_tribune", "state_hint": "manipur"},
    # --- Goa Chronicle ---
    {"url": "https://www.goacom.com/rss/", "source": "goa_chronicle", "state_hint": "goa"},
    # --- Business Standard (regional finance/governance) ---
    {"url": "https://www.business-standard.com/rss/politics-and-governance-107.rss", "source": "bs_governance"},
    {"url": "https://www.business-standard.com/rss/economy-policy-102.rss", "source": "bs_economy"},
    # --- LiveMint (state policy) ---
    {"url": "https://www.livemint.com/rss/news", "source": "livemint_news"},
]


def _to_iso8601(struct_time_value):

    if struct_time_value is None:
        return None

    return datetime(*struct_time_value[:6]).isoformat() + "Z"


def _fetch_feed(feed_config):

    articles = []

    try:
        response = requests.get(feed_config["url"], timeout=15)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
    except requests.RequestException:
        return articles

    if getattr(feed, "bozo", False):
        return articles

    for entry in feed.entries:
        articles.append(
            {
                "title": entry.get("title"),
                "content": entry.get("summary") or entry.get("description"),
                "url": entry.get("link"),
                "source": feed_config["source"],
                "published_at": _to_iso8601(entry.get("published_parsed") or entry.get("updated_parsed")),
                "state_hint": feed_config.get("state_hint"),
                "district_hint": feed_config.get("district_hint"),
            }
        )

    return articles


def fetch_local_publishers():

    articles = []

    with ThreadPoolExecutor(max_workers=min(REQUEST_WORKERS, len(RSS_FEEDS))) as executor:
        futures = [executor.submit(_fetch_feed, feed_config) for feed_config in RSS_FEEDS]

        for future in as_completed(futures):
            try:
                articles.extend(future.result())
            except Exception:
                continue

    return articles