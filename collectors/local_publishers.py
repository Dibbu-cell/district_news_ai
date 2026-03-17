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

    # --- Regional publisher coverage via Google News RSS site queries (district/state hinted) ---
    {"url": "https://news.google.com/rss/search?q=site%3Ajagran.com%20lucknow%20uttar%20pradesh%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_jagran_lucknow", "state_hint": "uttar pradesh", "district_hint": "lucknow"},
    {"url": "https://news.google.com/rss/search?q=site%3Ajagran.com%20kanpur%20uttar%20pradesh%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_jagran_kanpur", "state_hint": "uttar pradesh", "district_hint": "kanpur nagar"},
    {"url": "https://news.google.com/rss/search?q=site%3Abhaskar.com%20jaipur%20rajasthan%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_bhaskar_jaipur", "state_hint": "rajasthan", "district_hint": "jaipur"},
    {"url": "https://news.google.com/rss/search?q=site%3Abhaskar.com%20kota%20rajasthan%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_bhaskar_kota", "state_hint": "rajasthan", "district_hint": "kota"},
    {"url": "https://news.google.com/rss/search?q=site%3Apatrika.com%20udaipur%20rajasthan%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_patrika_udaipur", "state_hint": "rajasthan", "district_hint": "udaipur"},
    {"url": "https://news.google.com/rss/search?q=site%3Apatrika.com%20jodhpur%20rajasthan%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_patrika_jodhpur", "state_hint": "rajasthan", "district_hint": "jodhpur"},
    {"url": "https://news.google.com/rss/search?q=site%3Aeenadu.net%20vijayawada%20andhra%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_eenadu_vijayawada", "state_hint": "andhra pradesh", "district_hint": "ntr"},
    {"url": "https://news.google.com/rss/search?q=site%3Aeenadu.net%20visakhapatnam%20andhra%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_eenadu_vizag", "state_hint": "andhra pradesh", "district_hint": "visakhapatnam"},
    {"url": "https://news.google.com/rss/search?q=site%3Aandhrajyothy.com%20guntur%20andhra%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_aj_guntur", "state_hint": "andhra pradesh", "district_hint": "guntur"},
    {"url": "https://news.google.com/rss/search?q=site%3Asakshi.com%20tirupati%20andhra%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_sakshi_tirupati", "state_hint": "andhra pradesh", "district_hint": "tirupati"},
    {"url": "https://news.google.com/rss/search?q=site%3Anamasthetelangana.com%20warangal%20telangana%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_nt_warangal", "state_hint": "telangana", "district_hint": "hanumakonda"},
    {"url": "https://news.google.com/rss/search?q=site%3Anewstodaynet.com%20chennai%20tamil%20nadu%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_newstoday_chennai", "state_hint": "tamil nadu", "district_hint": "chennai"},
    {"url": "https://news.google.com/rss/search?q=site%3Adinamalar.com%20madurai%20tamil%20nadu%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_dinamalar_madurai", "state_hint": "tamil nadu", "district_hint": "madurai"},
    {"url": "https://news.google.com/rss/search?q=site%3Adinamani.com%20coimbatore%20tamil%20nadu%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_dinamani_coimbatore", "state_hint": "tamil nadu", "district_hint": "coimbatore"},
    {"url": "https://news.google.com/rss/search?q=site%3Aoneindia.com%20mysuru%20karnataka%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_oneindia_mysuru", "state_hint": "karnataka", "district_hint": "mysuru"},
    {"url": "https://news.google.com/rss/search?q=site%3Astarofmysore.com%20mysuru%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_som_mysuru", "state_hint": "karnataka", "district_hint": "mysuru"},
    {"url": "https://news.google.com/rss/search?q=site%3Audayavani.com%20mangaluru%20karnataka%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_udayavani_mangaluru", "state_hint": "karnataka", "district_hint": "dakshina kannada"},
    {"url": "https://news.google.com/rss/search?q=site%3Aasianetnews.com%20kozhikode%20kerala%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_asianet_kozhikode", "state_hint": "kerala", "district_hint": "kozhikode"},
    {"url": "https://news.google.com/rss/search?q=site%3Amanoramaonline.com%20kottayam%20kerala%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_manorama_kottayam", "state_hint": "kerala", "district_hint": "kottayam"},
    {"url": "https://news.google.com/rss/search?q=site%3Amathrubhumi.com%20thrissur%20kerala%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_mathrubhumi_thrissur", "state_hint": "kerala", "district_hint": "thrissur"},
    {"url": "https://news.google.com/rss/search?q=site%3Aanandabazar.com%20howrah%20west%20bengal%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_abp_howrah", "state_hint": "west bengal", "district_hint": "howrah"},
    {"url": "https://news.google.com/rss/search?q=site%3Atelegraphindia.com%20siliguri%20west%20bengal%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_telegraph_siliguri", "state_hint": "west bengal", "district_hint": "darjeeling"},
    {"url": "https://news.google.com/rss/search?q=site%3Asangbadpratidin.in%20asansol%20west%20bengal%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_sp_asansol", "state_hint": "west bengal", "district_hint": "paschim bardhaman"},
    {"url": "https://news.google.com/rss/search?q=site%3Aprabhatkhabar.com%20ranchi%20jharkhand%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_pk_ranchi", "state_hint": "jharkhand", "district_hint": "ranchi"},
    {"url": "https://news.google.com/rss/search?q=site%3Aprabhatkhabar.com%20dhanbad%20jharkhand%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_pk_dhanbad", "state_hint": "jharkhand", "district_hint": "dhanbad"},
    {"url": "https://news.google.com/rss/search?q=site%3Ainextlive.com%20patna%20bihar%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_inext_patna", "state_hint": "bihar", "district_hint": "patna"},
    {"url": "https://news.google.com/rss/search?q=site%3Alivehindustan.com%20muzaffarpur%20bihar%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_livehindustan_muzaffarpur", "state_hint": "bihar", "district_hint": "muzaffarpur"},
    {"url": "https://news.google.com/rss/search?q=site%3Asandesh.com%20surat%20gujarat%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_sandesh_surat", "state_hint": "gujarat", "district_hint": "surat"},
    {"url": "https://news.google.com/rss/search?q=site%3Adivyabhaskar.co.in%20rajkot%20gujarat%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_db_rajkot", "state_hint": "gujarat", "district_hint": "rajkot"},
    {"url": "https://news.google.com/rss/search?q=site%3Asakal.com%20nashik%20maharashtra%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_sakal_nashik", "state_hint": "maharashtra", "district_hint": "nashik"},
    {"url": "https://news.google.com/rss/search?q=site%3Alokmat.com%20nagpur%20maharashtra%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_lokmat_nagpur", "state_hint": "maharashtra", "district_hint": "nagpur"},
    {"url": "https://news.google.com/rss/search?q=site%3Apunemirror.com%20pune%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_punemirror_pune", "state_hint": "maharashtra", "district_hint": "pune"},
    {"url": "https://news.google.com/rss/search?q=site%3Aorissapost.com%20cuttack%20odisha%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_orissapost_cuttack", "state_hint": "orissa", "district_hint": "cuttack"},
    {"url": "https://news.google.com/rss/search?q=site%3Asambadenglish.com%20puri%20odisha%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_sambad_puri", "state_hint": "orissa", "district_hint": "puri"},
    {"url": "https://news.google.com/rss/search?q=site%3Atribuneindia.com%20ludhiana%20punjab%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_tribune_ludhiana", "state_hint": "punjab", "district_hint": "ludhiana"},
    {"url": "https://news.google.com/rss/search?q=site%3Atribuneindia.com%20amritsar%20punjab%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_tribune_amritsar", "state_hint": "punjab", "district_hint": "amritsar"},
    {"url": "https://news.google.com/rss/search?q=site%3Adailyexcelsior.com%20jammu%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_excelsior_jammu", "state_hint": "jammu and kashmir", "district_hint": "jammu"},
    {"url": "https://news.google.com/rss/search?q=site%3Agreaterkashmir.com%20srinagar%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_gk_srinagar", "state_hint": "jammu and kashmir", "district_hint": "srinagar"},
    {"url": "https://news.google.com/rss/search?q=site%3Aassamtribune.com%20guwahati%20assam%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_assamtribune_guwahati", "state_hint": "assam", "district_hint": "kamrup metropolitan"},
    {"url": "https://news.google.com/rss/search?q=site%3Aarunachaltimes.in%20itanagar%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_arunachaltimes_itanagar", "state_hint": "arunachal pradesh", "district_hint": "papum pare"},
    {"url": "https://news.google.com/rss/search?q=site%3Amizorampost.com%20aizawl%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_mizorampost_aizawl", "state_hint": "mizoram", "district_hint": "aizawl"},
    {"url": "https://news.google.com/rss/search?q=site%3Atripurainfo.com%20agartala%20district%20news&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_tripurainfo_agartala", "state_hint": "tripura", "district_hint": "west tripura"},
    {"url": "https://news.google.com/rss/search?q=site%3Anewindianexpress.com%20district%20governance%20india&hl=en-IN&gl=IN&ceid=IN:en", "source": "gn_nie_district_governance"},
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