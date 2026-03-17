import requests
import logging
import json
import csv
import os
import re
import browser_cookie3
import time
import random
from collections import defaultdict
from collections import deque
from pathlib import Path
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse
import mysql.connector
from datetime import datetime

# from all_filters_api import run
# ================= CONFIG =================

BASE_URL = "https://octopart.com/distributors"
SEARCH_QUERY =  'query UseSearchQuery($country: String!, $currency: String!, $filters: Map, $in_stock_only: Boolean, $has_datasheet_only: Boolean, $limit: Int!, $q: String, $sort: String, $sort_dir: SortDirection, $start: Int, $isSpecsView: Boolean!, $includeBrokers: Boolean = false, $priceBreaks: [Int!], $sellerLimit: Int, $inference_rules: InferenceRules) {\n  search(\n    country: $country\n    currency: $currency\n    filters: $filters\n    in_stock_only: $in_stock_only\n    has_datasheet_only: $has_datasheet_only\n    limit: $limit\n    q: $q\n    sort: $sort\n    sort_dir: $sort_dir\n    start: $start\n    inference_rules: $inference_rules\n  ) {\n    ...SimilarPartsSerpData\n    topline_advert_v2 {\n      ...ToplineAdvertToplineAd\n    }\n    ...SerpAnalytics\n    ...SpecsViewAttributeGroup @include(if: $isSpecsView)\n    applied_category {\n      ancestors {\n        id\n        name\n        path\n      }\n      id\n      name\n      path\n      relevant_attributes {\n        id\n        name\n        shortname\n        group\n        short_displayname\n      }\n    }\n    results {\n      ...SpecsViewResults @include(if: $isSpecsView)\n      ...PricesViewResults @skip(if: $isSpecsView)\n      ...TowerAdvertResults\n    }\n    applied_filters {\n      display_values\n      name\n      shortname\n      values\n      units_symbol\n    }\n    suggested_categories {\n      category {\n        id\n        name\n      }\n    }\n    child_category_agg(size: 25) {\n      category {\n        id\n        name\n      }\n      count\n    }\n    hits\n  }\n}\n\nfragment SimilarPartsSerpData on PartResultSet {\n  hits\n  results {\n    part {\n      counts\n      id\n    }\n  }\n}\n\nfragment ToplineAdvertToplineAd on ToplineAdvertV2 {\n  is_co_op\n  click_url\n  text\n  price_breaks\n  seller {\n    company {\n      id\n      name\n    }\n  }\n  offer {\n    id\n    inventory_level\n    prices {\n      quantity\n      converted_price\n      converted_currency\n      conversion_rate\n    }\n  }\n  part {\n    mpn\n    manufacturer {\n      id\n      name\n    }\n  }\n}\n\nfragment SerpAnalytics on PartResultSet {\n  hits\n  suggested_categories {\n    category {\n      id\n    }\n  }\n  suggested_filters {\n    id\n  }\n  results {\n    part {\n      id\n    }\n  }\n}\n\nfragment SpecsViewAttributeGroup on PartResultSet {\n  specs_view_attribute_groups {\n    name\n    attributes {\n      id\n      name\n      shortname\n      group\n      short_displayname\n      units_symbol\n    }\n  }\n}\n\nfragment SpecsViewResults on PartResult {\n  part {\n    ...PdmcPart\n    ...SpecsViewPartCell\n    ...SpecsViewCompareCell\n    counts\n    id\n    mpn\n    estimated_factory_lead_days\n    slug\n    descriptions {\n      text\n    }\n    manufacturer {\n      name\n      is_verified\n    }\n    specs {\n      display_value\n      value\n      attribute {\n        id\n        name\n        shortname\n        group\n        short_displayname\n      }\n    }\n    median_price_1000 {\n      converted_price\n      converted_currency\n    }\n    cad_models {\n      has_symbol\n      has_footprint\n      has_3d_model\n    }\n    best_datasheet {\n      url\n    }\n    sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n      is_authorized\n      offers(priceBreaks: $priceBreaks) {\n        inventory_level\n      }\n    }\n    allSellers: sellers(include_brokers: $includeBrokers) {\n      is_authorized\n      is_broker\n    }\n    best_image {\n      url\n    }\n  }\n}\n\nfragment PdmcPart on Part {\n  id\n  mpn\n  slug\n  counts\n  cad {\n    footprint_image_url\n    symbol_image_url\n  }\n  manufacturer {\n    is_verified\n    name\n  }\n}\n\nfragment SpecsViewPartCell on Part {\n  ...PdmcPart\n  id\n  mpn\n  slug\n  specs {\n    display_value\n    value\n    attribute {\n      shortname\n    }\n  }\n  manufacturer {\n    name\n    is_verified\n    slug\n  }\n  best_datasheet {\n    url\n  }\n  descriptions {\n    text\n  }\n  cad_models {\n    has_symbol\n    has_footprint\n    has_3d_model\n  }\n}\n\nfragment SpecsViewCompareCell on Part {\n  id\n  mpn\n  manufacturer {\n    name\n  }\n  slug\n  best_image {\n    url\n  }\n}\n\nfragment PricesViewResults on PartResult {\n  ...PricesViewPartResult\n  part {\n    ...PricesViewPart\n    id\n    slug\n    sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n      company {\n        id\n        is_distributorapi\n      }\n      offers(priceBreaks: $priceBreaks) {\n        id\n      }\n    }\n  }\n}\n\nfragment PricesViewPartResult on PartResult {\n  ...PricesViewHeaderPartResult\n}\n\nfragment PricesViewHeaderPartResult on PartResult {\n  aka_mpn\n  description\n}\n\nfragment PricesViewPart on Part {\n  id\n  ...PricesViewHeaderPart\n  ...OfferTableListPart\n  ...PricesViewFooterPart\n  sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n    ...OfferTableListSeller\n  }\n}\n\nfragment PricesViewHeaderPart on Part {\n  id\n  mpn\n  slug\n  v3uid\n  free_sample_url\n  manufacturer_url\n  manufacturer {\n    name\n    is_verified\n  }\n  median_price_1000 {\n    converted_price\n    converted_currency\n  }\n  best_image {\n    url\n  }\n  ...SegmentAltPartEvent\n  ...Series\n  ...DatasheetButton\n  ...CadModelsLink\n  ...PdmcPart\n}\n\nfragment SegmentAltPartEvent on Part {\n  id\n  mpn\n  slug\n  manufacturer {\n    id\n    name\n  }\n}\n\nfragment Series on Part {\n  series {\n    url\n    name\n  }\n}\n\nfragment DatasheetButton on Part {\n  best_datasheet {\n    url\n  }\n  manufacturer {\n    name\n  }\n  id\n  mpn\n  slug\n}\n\nfragment CadModelsLink on Part {\n  slug\n  cad_models {\n    has_symbol\n    has_footprint\n    has_3d_model\n    highest_level\n  }\n}\n\nfragment OfferTableListPart on Part {\n  ...OfferTablePart\n  slug\n}\n\nfragment OfferTablePart on Part {\n  id\n  ...SellerPart\n  ...SummaryPart\n  ...OfferPart\n}\n\nfragment SellerPart on Part {\n  ...OfferPart\n}\n\nfragment OfferPart on Part {\n  id\n  v3uid\n  mpn\n  manufacturer {\n    name\n  }\n  sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n    company {\n      id\n    }\n  }\n}\n\nfragment SummaryPart on Part {\n  id\n  v3uid\n  mpn\n  manufacturer {\n    name\n  }\n  sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n    company {\n      id\n    }\n  }\n}\n\nfragment PricesViewFooterPart on Part {\n  slug\n  ...PdmcPart\n}\n\nfragment OfferTableListSeller on PartSeller {\n  ...OfferTableSeller\n  is_authorized\n  is_broker\n  is_rfq\n  company {\n    id\n    name\n  }\n  offers(priceBreaks: $priceBreaks) {\n    id\n    inventory_level\n    prices {\n      converted_price\n      quantity\n      conversion_rate\n    }\n  }\n}\n\nfragment OfferTableSeller on PartSeller {\n  ...Seller\n  ...OfferSeller\n  ...SummarySeller\n  company {\n    id\n  }\n  offers(priceBreaks: $priceBreaks) {\n    ...Offer\n    id\n    click_url\n  }\n}\n\nfragment Seller on PartSeller {\n  ...OfferSeller\n  offers(priceBreaks: $priceBreaks) {\n    id\n    ...Offer\n  }\n}\n\nfragment OfferSeller on PartSeller {\n  ...IsAuthSeller\n  is_rfq\n  is_authorized\n  is_broker\n  company {\n    id\n    name\n    is_distributorapi\n  }\n}\n\nfragment IsAuthSeller on PartSeller {\n  is_authorized\n  is_broker\n}\n\nfragment Offer on Offer {\n  id\n  updated\n  inventory_level\n  click_url\n  moq\n  packaging\n  sku\n  prices {\n    price\n    quantity\n    currency\n    converted_price\n    converted_currency\n    conversion_rate\n  }\n}\n\nfragment SummarySeller on PartSeller {\n  ...IsAuthSeller\n  is_authorized\n  is_rfq\n  is_broker\n  company {\n    id\n    name\n    is_distributorapi\n  }\n  offers(priceBreaks: $priceBreaks) {\n    id\n    click_url\n    inventory_level\n    updated\n    prices {\n      price\n      quantity\n      converted_price\n      converted_currency\n      conversion_rate\n      currency\n    }\n  }\n}\n\nfragment TowerAdvertResults on PartResult {\n  aka_mpn\n  part {\n    v3uid\n    category {\n      id\n    }\n    manufacturer {\n      id\n    }\n    sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n      company {\n        id\n      }\n      offers(priceBreaks: $priceBreaks) {\n        inventory_level\n        prices {\n          currency\n        }\n      }\n    }\n  }\n}',
  # keep your full query string here  # move huge query to class variable

CHECKPOINT_FILE = "crawler_checkpoint.json"
OUTPUT_FILE = "parts.csv"
LOG_FILE = "crawler.log"
REQUEST_DELAY = 3.5
TIMEOUT = 30
BASE_DIR = Path(__file__).resolve().parent
# COOKIES_FILE = BASE_DIR / "cookies.json"
# HEADERS_FILE = BASE_DIR / "headers.json"
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    # 'cookie': 'session=.eJxcy9GqwjAMANB_yfPtJVvapd3PlNhmGNw6we5BxH8XRBB8PXAekOXoZ23dinTbW-77RRvMEDUmoVCmEQmRFREp-CQhIjNLgT847du_rN2OLVuFGT5U7XZd5Z6bbPrVxVb9oXfCSkk8Bjcxk_O0kEulVEeDjyVyHUaN8HwFAAD__zJ4Mbg.abFZqA.k315ksSAAck02ByT7f2DDh_ZLiE; _pxvid=300b81dd-1d42-11f1-87d0-b8f697e6725f; _gcl_au=1.1.1508108542.1773371387; _ga_SNYD338KXX=GS2.1.s1773371399$o1$g0$t1773371399$j60$l0$h0; _ga=GA1.2.528479484.1773371387; _pxhd=TEXXlrBbeCrDkeJg7n-GJlWnHjiwS9bkQZw1koJryYiHrb94-YZ1X7EojXPxcb3YcFywhe-jAomZ8xbIca26ZQ==:8VjKnwuIy-KtjKNVsj9ibQ9V-4ymCeqLCwaCpfdXwHFqYn1nyF8-Q3bWynkr9GJv/jX4jaslHsY6TxEo0vFqUl3xqqeHalxGYTegY4y65-MPMDPth-SC-2KuWVcyH3VZSd9tkDYMx3SJEFSAQx0GBQ==; cf_clearance=FlL39phCGaSKnXNVihJMF0l_Ze96y3x7bC.xtWXDQk8-1773460014-1.2.1.1-aZ00kvrFEVx462HT_z8akYtZidOXrv61pDQlObsA.cVaQwWY_LZgG8.Wssa9.7EV9xWRxtxcCiuP2odVVAbLz7SEsfF98UFDs1Te9IsaGUoGJqxMEcDyEnrIf2NQhFJ5JX225MWtUofyhW2kR00LOX2CCGcq.ARPN0u0K.unr1cotEGCfliU0x49C21rMXMEnh9asf23rL9hNC5ARSirFWicLSDTshpttl51iWtslLk; pxcts=719a6c71-1f58-11f1-b9b2-1b853012e90e; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Mar+14+2026+09%3A16%3A55+GMT%2B0530+(India+Standard+Time)&version=202403.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=e83af073-9404-4724-86db-983734ae8c36&interactionCount=1&isAnonUser=1&landingPath=https%3A%2F%2Foctopart.com%2F&groups=C0001%3A1%2CC0003%3A0%2CC0002%3A0%2CC0004%3A0&hosts=H114%3A0%2CH29%3A0%2CH6%3A0%2CH11%3A0%2CH13%3A0%2CH14%3A0%2CH115%3A0%2CH16%3A0%2CH18%3A0%2CH25%3A0%2CH30%3A0&genVendors=; _px=dYIISGs8UKO6P2KHZpyOGpauiBf0JGk70Yb9rRasIAU9LvcIxr6lOhXGecJAtPgLFU3oKpehc0yCefj/sqNK9Q==:1000:wZWRhJ3v6kFfV6EpUmQnGsQ68MbKVgqnuWBYnqGvlf9rUrkcCAaq/dKB758UDyEHc3gVIkanvD/BLgRryne32BjwYTRMPniVLZovxd1zd85Lvch5dtHHG0Rl7VzmKleAWOa0PWS7XZtVCKTP8IhBD36ENJzWAkJr4Z6CW3BZKPLBpOrAScELy/bIu+xANbpqUMlvptj2U9zPmCaBXhguP+gxFqZ6dNijtEx/tKKcVeAjFYocOrwYZJetstV4xeag5sJwTpCMOxUO271iNcVX5w==; g_state={"i_l":0,"i_ll":1773460016385,"i_b":"5D5Y89pzd5da9RqSkwMQJb1ZMT+OR1L4GB+DDyMwnGg","i_e":{"enable_itp_optimization":0}}; _gid=GA1.2.751769094.1773460016; __insp_wid=940536992; __insp_slim=1773460017601; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly9vY3RvcGFydC5jb20v; __insp_targlpt=RWxlY3Ryb25pYyBDb21wb25lbnRzIFNlYXJjaCBFbmdpbmUgfCBGaW5kIFBhcnRzIEZhc3QgfCBPY3RvcGFydA%3D%3D; _uetsid=73914f401f5811f18710bdc5523fb42e; _uetvid=1758cb401e8a11f19cae6ff263d9ce9e; __insp_norec_sess=true',
}
# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()   # console output
    ]
)

logger = logging.getLogger("octopart")


print("=== SCRIPT STARTED ===")


# ================= MAIN CLASS =================

class OctopartCrawler:

    def __init__(self):
        print("Initializing crawler...")

        self.session = self.get_auto_session()
        print(self.session)
        # ----------------------------
        # Recursion Protection
        # ----------------------------
        self.visited_categories = set()

        # ----------------------------
        # Load Filters
        # ----------------------------
        # self.all_filters = self.load_all_filters()
        # self.build_company_maps()

        # ----------------------------
        # Category Mappings
        # ----------------------------
        self.category_name_map = {}
        self.category_path_map = {}
        self.category_slug_map = {}

        self.load_category_mapping()

        # ----------------------------
        # Stats + Resume
        # ----------------------------
        self.crawl_stats = defaultdict(int)
        self.existing_parts = self.load_existing_parts()
        self.checkpoint = self.load_checkpoint() or {}

        logger.info("Crawler Initialized")

    def slug_to_category_id(self,slug):
        with open("categories.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        # Use .get() safely
        categories = data.get("data", {}).get("categories", [])

        # Find matching IDs
        matching_ids = [
            cat.get("id") for cat in categories if slug in cat.get("path", "")
        ]

        print(matching_ids)
        print(15*"!@#$%")

    # # Local Db
    # def db_connector(self):
    #     connection = mysql.connector.connect(
    #     host="127.0.0.1",
    #     port=3306,
    #     user="root",
    #     password="Softage@123456",
    #     database='octopart')
    #     return connection

    # Live Db
    def db_connector(self):
        while True:
            try:
                connection = mysql.connector.connect(
                host="13.201.205.150",
                port=3306,
                user="gd_data",
                password="GD@2025@softage",
                database='octopart',
                connection_timeout=10)
                # return connection
    
                if connection.is_connected():
                    print("Connected to MySQL")
                    return connection

            except mysql.connector.Error as e:
                print("Database connection failed:", e)
                print("Retrying in 10 seconds...")
                time.sleep(10)



    def get_auto_session(self):
        """
        Return a requests.Session pre‑populated with cookies from one of the
        local browsers.  If the first verification request returns 403 we loop
        (re‑loading cookies) up to `max_attempts` times.

        Raises RuntimeError if we never obtain a 200 response.
        """
        # max_attempts = 3
        loaders = [
            ("Firefox", browser_cookie3.firefox),
            ("Chrome",  browser_cookie3.chrome),
            ("Edge",    browser_cookie3.edge),
        ]

        # for attempt in range(1, max_attempts + 1):
        while True:
            s = requests.Session()
            s.headers.update(HEADERS)

            # load cookies from whichever browser has them
            cj = None
            for name, loader in loaders:
                try:
                    tmp = loader(domain_name="octopart.com")
                    if tmp:
                        cj = tmp
                        logger.info("cookies loaded from %s (%d total)", name, len(tmp))
                        break
                except Exception as ex:
                    logger.debug("could not load cookies from %s: %s", name, ex)

            if not cj:
                logger.error("no octopart cookies found in any browser")
            else:
                s.cookies.update(cj)

            # verify by hitting a page that normally returns 200
            try:
                r = s.get("https://octopart.com/distributors", timeout=15)
            except Exception as ex:
                logger.warning("verification request failed: %s", ex)
                r = None
                
            status = r.status_code

            if r and r.status_code == 200:
                logger.info("session verified on attempt %d")
                return s
            else:
                # status = r.status_code if r else "none"
                logger.warning("attempt got  retrying (%s)", status)
                time.sleep(5)

        # all retries exhausted
        # raise RuntimeError("unable to obtain a working Octopart session (403 on all attempts)")
    
    # session = get_auto_session()

    def get_soup(self, url):
        while True:
            try:
                # session = self.get_auto_session()
                r = self.session.get(url, timeout=TIMEOUT)
                r.raise_for_status()

                # Slow down between requests to avoid 403
                time.sleep(random.uniform(2.5, 5.0))

                return bs(r.text, "html.parser")

            except Exception as e:
                logger.error(f"Request Failed → {url} | {e}")
                if r.status_code == 404:
                    logger.warning(f"Page not found (404) → {url}")
                    return None

                print('Retrying...!!!')
                session = self.get_auto_session()  # refresh session and cookies
                self.session = session  # update session in crawler instance
                time.sleep(10)


    def extract_path_parts(self, page_url):

        path = urlparse(page_url).path.strip("/")
        parts = path.split("/")

        if parts and parts[0] == "distributors":
            parts = parts[1:]

        parts += [""] * (4 - len(parts))
        return parts[:4]
    # ================= DATA =================

    def load_existing_parts(self):
        data = set()
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data.add((row["distributor"],row["category"],row["child_category"],row["sub_child_category"],
                              row["title"],row["part_url"],row["page_url"]))

        logger.info(f"Loaded {len(data)} existing parts")
        return data


    def save_parts_sql(self, rows):

        if not rows:
            print("No data in rows")
            return

        query = """
            INSERT IGNORE INTO crawled_data (
                distributor,
                category,
                part_id,
                title,
                part_url,
                page_url,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        values = []

        for r in rows:
            values.append((
                r.get("distributor", ""),
                r.get("category", ""),
                r.get("part_id", ""),
                r.get("title", ""),
                r.get("part_url", ""),
                r.get("page_url", ""),
                datetime.now()
            ))

        while True:  # keep trying forever

            conn = None
            cursor = None

            try:

                conn = self.db_connector()
                cursor = conn.cursor()

                cursor.executemany(query, values)
                conn.commit()

                print("Attempted:", len(values))
                print("Inserted:", cursor.rowcount)

                break   # success → exit retry loop

            except mysql.connector.Error as e:

                logger.warning(f"DB error → retrying in 30 sec | {e}")

                time.sleep(30)

            except Exception as e:

                logger.error(f"Unexpected DB error → retrying | {e}")

                time.sleep(30)

            finally:

                try:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
                except:
                    pass
            # cursor.close()
            # conn.close()


    def insert_update_distributors(self, rows, retries=3):

        if not rows or not all(k in rows[0] for k in ("id", "name", "url")):
            logger.warning("No valid distributor data received")
            return

        values = [
                (r["id"], r["name"], r["url"], "pending", datetime.now())
                for r in rows
            ]

        while retries > 0:

            conn = None
            cursor = None

            try:
                conn = self.db_connector()
                cursor = conn.cursor()

                query = """
                    INSERT IGNORE INTO distributor_crawling (
                        id,
                        name,
                        url,
                        crawling_status,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """

                cursor.executemany(query, values)

                conn.commit()

                logger.info(f"Attempted: {len(values)}")
                logger.info(f"Inserted: {cursor.rowcount}")

                return

            except Exception as e:

                # retries -= 1

                logger.error(
                    f"DB error in insert_update_distributors: {e}"
                )

                time.sleep(5)

            finally:

                try:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
                except:
                    pass

        logger.error("Failed to insert distributors after retries")

    def fetch_distributor_list(self, retries=5):
        while retries > 0:
            conn = None
            cursor = None

            try:
                conn = self.db_connector()
                cursor = conn.cursor()

                # query = """
                # SELECT id, name, url
                # FROM distributor_crawling
                # WHERE name = 'amar-radio-corporation';
                # """
                query = """
                SELECT id, name, url
                FROM distributor_crawling
                WHERE crawling_status = 'pending'
                LIMIT 1
                """

                cursor.execute(query)
                result = cursor.fetchone()

                if not result:
                    logger.info("No pending distributors found")
                    return None

                dist_id = result[0]
                name = result[1]

                logger.info(f"Fetched distributor: {name}")

                update_query = """
                UPDATE distributor_crawling
                SET crawling_status = 'processing'
                WHERE id = %s
                """

                cursor.execute(update_query, (dist_id,))
                conn.commit()

                return result

            except Exception as e:

                # retries -= 1
                logger.error(f"DB error in fetch_distributor_list: {e}")

                time.sleep(5)

            finally:

                try:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
                except:
                    pass

        logger.error("Failed to fetch distributor after retries")

        return None

    def mark_distributor_completed(self, name):

        while True:

            try:
                conn = self.db_connector()
                cursor = conn.cursor()

                query = """
                UPDATE distributor_crawling
                SET crawling_status = 'completed'
                WHERE name = %s
                """

                cursor.execute(query, (name,))
                conn.commit()

                cursor.close()
                conn.close()

                logger.info(f"Distributor completed → {name}")
                break
            except:
                print("Retry...!!! mark_distributor_completed")

    def mark_distributor_failed(self, name):
        while True:
            try:

                conn = self.db_connector()
                cursor = conn.cursor()

                query = """
                UPDATE distributor_crawling
                SET crawling_status = 'failed'
                WHERE name = %s
                """

                cursor.execute(query, (name,))
                conn.commit()

                cursor.close()
                conn.close()
                break
        
            except:
                print("Retry...!!! mark_distributor_failed")

    def mark_category_completed(self, url):

        cp = self.load_checkpoint()

        if "completed_categories" not in cp:
            cp["completed_categories"] = []

        if url not in cp["completed_categories"]:
            cp["completed_categories"].append(url)

        with open("crawler_checkpoint.json", "w") as f:
            json.dump(cp, f, indent=2)


    def set_current_category(self, url):

        cp = self.load_checkpoint()

        cp["current_category_url"] = url

        with open("crawler_checkpoint.json", "w") as f:
            json.dump(cp, f, indent=2)


    # ================= SCRAPER =================

    def get_distributors(self):
        while True:
            url = "https://octopart.com/api/v4/internal?operation=AllDistributors"

            payload = {
                        "operationName": "AllDistributors",
                        "variables": {},
                        "query": """
                        query AllDistributors {
                        sellers {
                        id
                        name
                        homepage_url
                        }
                        }
                        """
            }
            try:
                response = self.session.post(url, headers=HEADERS, json=payload)

                data = response.json()

                distributors=[]
                for dist in data.get("data", {}).get("sellers", []):
                    distributors.append({
                        'id': dist.get("id", ""),
                        'name': dist.get("name", ""),
                        'url': dist.get("homepage_url", "")
                    })
                return distributors
            except Exception as e:
                logger.error(f"Failed to fetch distributors: {e}")
                logger.error(f"Trying again in 5 seconds...")
                time.sleep(5)
    

    def get_categories(self, distributor):
        url = f"{BASE_URL}/{distributor}"
        time.sleep(random.uniform(.5, 2.5))  # delay before request
        soup = self.get_soup(url)
        if not soup:
            logger.warning(f"Failed to load distributor page → {distributor}")
            return []
        cats = []
        for a in soup.find_all("a"):
            href = a.get("href", "")
            if href.startswith("/distributors/") and href.count("/") > 2:
                cats.append("https://octopart.com" + href)

        logger.info(f"{distributor} → {len(cats)} categories")
        return cats


    def get_pages(self, url):
        soup = self.get_soup(url)
        if not soup:
            return 0
        span = soup.find("span", {"data-testid": "serp-result-count"})
        if not span:
            return 0
        text = span.get_text()
        m = re.search(r"[\d,]+", text)
        if not m:
            return 0
        total = int(m.group().replace(",", ""))
        pages = total // 10
        logger.info(f"{url} → {total} results ({pages} pages)")
        return pages


    def scrape_page(self, soup,page_url):
        parts = soup.find_all(
            "div",
            {"data-testid": "prices-view-part"}
        )

        new_rows = []
        for part in parts:
            img = part.find("img")
            a = part.find("a")
            title = img["title"] if img else ""

            href = a["href"] if a else ""
            for a in part.find_all("a", href=True): 
                href = a["href"] 
                if "/part/" in href:
                    part_url = "https://octopart.com" + href
            distributor, category, child_category, sub_child_category = self.extract_path_parts(page_url)
            key = (distributor, category, child_category, sub_child_category, title, part_url, page_url)

            if key not in self.existing_parts:
                self.existing_parts.add(key)
                distributor, category, child_category, sub_child_category = \
                    self.extract_path_parts(page_url)
                
                new_rows.append({
                    "distributor":distributor,
                    "category":category,
                    "child_category":child_category,
                    "sub_child_category":sub_child_category,
                    "title": title,
                    "part_url": part_url,
                    "page_url":page_url,
                })
        
        self.save_parts_sql(new_rows)

        logger.info(f"Saved {len(new_rows)} parts")


    def load_checkpoint(self):

        if os.path.exists(CHECKPOINT_FILE):

            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)

        return {
            "distributor": None,
            "category_index": 0
        }


    def save_checkpoint(self, distributor, category_index):

        data = {
            "distributor": distributor,
            "category_index": category_index
        }

        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(data, f, indent=2)


    def clear_checkpoint(self):

        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)


    def run(self):
        checkpoint = self.load_checkpoint()
        distributors = self.get_distributors()
        self.insert_update_distributors(distributors)

        resume = checkpoint.get("distributor") is None

        while True:
            distributor_data = self.fetch_distributor_list()

            if not distributor_data:
                logger.info("No more distributors left to crawl")
                break

            distributor = distributor_data[1].replace(' ', '-').lower()  # convert name to slug format
            self.distributor_id = distributor_data[0] 
            if checkpoint.get("distributor") and not resume:
                if distributor != checkpoint["distributor"]:
                    continue
                resume = True

            self.current_distributor = distributor
            logger.info(f"Distributor: {distributor}")

            try:
                categories = self.get_categories(distributor)

                if not categories:
                    logger.warning(f"No categories found for {distributor}")
                    self.mark_distributor_failed(distributor)
                    continue

                start_index = checkpoint.get("category_index", 0)

                self.visited_categories = set()

                for i in range(start_index, len(categories)):

                    cat = categories[i]
                    full_url = cat

                    logger.info(f"Category: {cat}")

                    depth = 0
                    self.crawl_category_recursively(distributor, self.distributor_id, full_url, depth)

                    self.save_checkpoint(distributor, i)

                self.mark_distributor_completed(distributor)
                self.clear_checkpoint()

            except Exception:
                logger.exception(f"Distributor failed → {distributor}")
                self.mark_distributor_failed(distributor)


    def crawl_category_recursively(self, distributor, dist_id, url=None, depth=0, breadcrumbs=None):
        """
        Args:
            distributor: slug string
            dist_id: the internal ID
            url: current crawling URL
            depth: integer for indentation
            breadcrumbs: list of category names (managed automatically)
        """
        base_url = "https://octopart.com"
        dist_path = f"/distributors/{distributor}"

        # --- CRITICAL FIX: Initialize breadcrumbs if None ---
        if breadcrumbs is None:
            breadcrumbs = []

        # Handle starting URL
        if url is None:
            url = f"{base_url}{dist_path}"

        if url in self.visited_categories:
            return
        
        soup = self.get_soup(url)
        if not soup:
            return

        # Slash counting logic
        current_rel_path = url.replace(base_url, "")
        current_slash_count = current_rel_path.count("/")

        # Find category links
        sub_category_spans = soup.find_all(
            "span", 
            {"class": "text-slate-800 group-hover:text-lapiz-500"}
        )

        found_sub = False
        
        for span in sub_category_spans:
            parent_a = span.find_parent("a")
            if parent_a and parent_a.has_attr("href"):
                href = parent_a["href"]
                
                # Navigate exactly one level deeper
                if href.startswith(dist_path) and href.count("/") == current_slash_count + 1:
                    if href != current_rel_path:
                        found_sub = True
                        category_name = span.get_text(strip=True)
                        
                        # Recursive Call - Passing all 5 arguments explicitly
                        self.crawl_category_recursively(
                            distributor=distributor, 
                            dist_id=dist_id, 
                            url=base_url + href, 
                            depth=depth + 1, 
                            breadcrumbs=breadcrumbs + [category_name]
                        )

        # --- LEAF NODE LOGIC ---
        if not found_sub:
            # Prevent processing the root distributor page as a leaf
            if breadcrumbs:
                self.process_leaf_node(url, depth, breadcrumbs)
                self.visited_categories.add(url)

    def process_leaf_node(self, url, depth, breadcrumbs):
        # Visual indent logic
        indent = "  " * (depth - 1) if depth > 0 else ""
        print(f"{indent}└── 📍 LEAF REACHED: {' > '.join(breadcrumbs)}")
        # This will now correctly show: Electronics > Passive Components > Capacitors

        # API Logic...
        category_id = self.get_category_id(url)
        if category_id:
            logger.info(f"Resolved category_id → {category_id}")
            # self.distributor_id = self.find_dist_id_by_name(self.current_distributor, category_id)
            print(self.current_distributor, self.distributor_id)
            self.run_category(category_id,self.distributor_id)
            self.api_get_request(category_id, url)
            self.mark_category_completed(url)

    # def crawl_category_recursively(self, distributor, url=None, depth=0):
    #     """
    #     Recursively crawl categories for a distributor,
    #     print hierarchy, and call API at leaf categories.

    #     Args:
    #         distributor (str): Distributor slug (e.g., 'hisco')
    #         url (str, optional): Category URL to start from.
    #         depth (int, optional): Recursion depth for printing.
    #     """
    #     import re  # make sure re is imported if not already
    #     base_url = "https://octopart.com/distributors"

    #     # Build the starting URL if not provided
    #     if url is None:
    #         url = f"{base_url}/{distributor}"

    #     # Skip already visited categories
    #     if url in self.visited_categories:
    #         return
    #     self.visited_categories.add(url)

    #     # Indentation for hierarchy visualization
    #     indent = "  " * depth
    #     print(f"{indent}Visiting: {url}")

    #     # Fetch and parse the page
    #     soup = self.get_soup(url)
    #     if not soup:
    #         print(f"{indent}Failed to fetch page")
    #         return

    #     # ---------------------------------------------
    #     # FIND SUB-CATEGORIES
    #     # ---------------------------------------------
    #     sub_categories = soup.find_all(
    #         "span",
    #         {"class": "text-slate-800 group-hover:text-lapiz-500"}
    #     )

    #     found_sub = False
    #     current_depth = url.replace("https://octopart.com", "").count("/")

    #     for sub in sub_categories:
    #         parent_a = sub.find_parent("a")
    #         if not parent_a or not parent_a.has_attr("href"):
    #             continue

    #         href = parent_a["href"]

    #         # Stay inside the current distributor
    #         if not href.startswith(f"/distributors/{distributor}"):
    #             continue

    #         # Only follow deeper categories
    #         if href.count("/") > current_depth:
    #             found_sub = True
    #             full_url = "https://octopart.com" + href
    #             category_name = sub.get_text(strip=True)

    #             print(f"{indent}  └── Child Category → {category_name}")

    #             # Recursive call for sub-category
    #             self.crawl_category_recursively(distributor, full_url, depth + 1)

    #     # ---------------------------------------------
    #     # LEAF NODE
    #     # This is where you call the API
    #     # ---------------------------------------------
    #     if not found_sub:
    #         print(f"{indent}  *** LEAF CATEGORY *** {url}")

    #         # Extract result count
    #         result = soup.find("span", {"data-testid": "serp-result-count"})
    #         count = 0
    #         if result:
    #             text = result.get_text(strip=True)
    #             m = re.search(r"\d[\d,]*", text)
    #             if m:
    #                 count = int(m.group().replace(",", ""))

    #         print(f"{indent}  Result count: {count}")

    #         # Skip empty categories
    #         if count == 0:
    #             print(f"{indent}  Skipping empty leaf")
    #             return

    #         # -------------------------------------------------
    #         # CALL API FUNCTION
    #         # -------------------------------------------------
    #         # -----------------------------------------
    #         # Get category_id using slug
    #         # -----------------------------------------

    #         category_id = self.get_category_id(url)
    #         print(category_id)
    #         if not category_id:
    #             logger.warning(f"Category ID not found → {url}")
    #             return

    #         logger.info(f"Resolved category_id → {category_id}")
    #         self.distributor_id = self.find_dist_id_by_name(self.current_distributor, category_id)
    #         print(self.current_distributor, self.distributor_id)
    #         self.run_category(category_id,self.distributor_id)
    #         self.api_get_request(category_id, url)
    #         self.mark_category_completed(url)


    def get_category_id(self, url):
        print(url)
        slug = url.split("?")[0].rstrip("/").split("/")[-1].lower()
        with open("categories.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        # Use .get() safely
        categories = data.get("data", {}).get("categories", [])

        # Find matching IDs
        matching_ids = [
            cat.get("id") for cat in categories if slug in cat.get("path", "")
        ]

        print(matching_ids)
        print(15*"!@#$%")
        return matching_ids

    def get_all_filters(self,category_id, distributor_id):

        params = {
            'operation': 'getFacets',
        }

        json_data = {
            'query': 'query getFacets($searchTerm: String, $country: String!, $filters: Map, $attributeNames: [String!]!, $needsDistributor: Boolean!, $needsManufacturer: Boolean!, $needsCadLevel: Boolean = false, $aggSize: Int!, $in_stock_only: Boolean, $has_datasheet_only: Boolean) {\n  search(\n    q: $searchTerm\n    country: $country\n    filters: $filters\n    in_stock_only: $in_stock_only\n    has_datasheet_only: $has_datasheet_only\n  ) {\n    manufacturer_agg(size: $aggSize) @include(if: $needsManufacturer) {\n      ...CompanyBucket\n    }\n    distributor_agg(size: $aggSize) @include(if: $needsDistributor) {\n      ...CompanyBucket\n    }\n    spec_aggs(attribute_names: $attributeNames, size: $aggSize) {\n      ...SpecAgg\n    }\n    cad_level_agg @include(if: $needsCadLevel) {\n      level\n      display_name\n      count\n    }\n  }\n}\n\nfragment CompanyBucket on CompanyBucket {\n  company {\n    id\n    name\n  }\n  count\n}\n\nfragment SpecAgg on SpecAgg {\n  attribute {\n    shortname\n    name\n    value_type\n    units_symbol\n  }\n  buckets {\n    display_value\n    float_value\n    count\n  }\n  min\n  max\n}',
            'variables': {
                'needsDistributor': False,
                'needsManufacturer': True,
                'needsCadLevel': False,
                'attributeNames': [
                    'rohs',
                    'lifecyclestatus',
                    'gender',
                    'numberofpositions',
                    'mount',
                    'termination',
                    'fasteningtype',
                    'orientation',
                    'contactplating',
                    'voltagerating',
                    'currentrating',
                ],
                'country': 'IN',
                'aggSize': 500,
                'filters': {
                    'category_id': category_id,
                    'distributor_id': [
                        distributor_id,
                    ],
                },
                'in_stock_only': False,
                'has_datasheet_only': False,
            },
            'operationName': 'getFacets',
            }

        response = self.session.post('https://octopart.com/api/v4/internal', params=params, headers=HEADERS, json=json_data)
        time.sleep(3)
        return response

    def load_json(self, path, default):

        path = Path(path)   # convert string → Path object

        if not path.exists():
            return default

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def run_category(self,CATEGORY_ID, distributor_id):
        # processed_keys = load_checkpoint()
        response = self.get_all_filters(CATEGORY_ID, distributor_id)

        with open('all_filters.json','w') as jj:
            json.dump(response.json(), jj, indent=2)

        output_data = self.load_json(
            "all_filters.json",
            {"category_id": CATEGORY_ID, "results_by_key": {}},
        )

        # Validate structure
        if "results_by_key" not in output_data or not isinstance(output_data["results_by_key"], dict):
            output_data = {
                "category_id": CATEGORY_ID,
                "results_by_key": {},
                "legacy_output": output_data,
            }

    def extract_spec_filters(self):

        with open("all_filters.json", encoding="utf-8") as f:
            search = json.load(f).get("data", {}).get("search", {})

        spec_filters = {}

        for spec in search["spec_aggs"]:
            key = spec["attribute"]["shortname"]
            # if key not in self.TARGET_SPECS:
            #     continue

            values = [
                bucket["display_value"]
                for bucket in spec.get("buckets", [])
                ]
            if values:
                spec_filters[key] = values

        return spec_filters


    def find_id_by_path(self, data, substring):
        # case 1 → list
        if isinstance(data, list):
            iterable = data

        # case 2 → dict containing list
        elif isinstance(data, dict):
            # auto detect list inside json
            iterable = next((v for v in data.values() if isinstance(v, list)),[])
        else:
            return None

        for entry in iterable:
            if not isinstance(entry, dict):
                continue

            path = entry.get("path", "")
            if substring.lower() in path.lower():
                return entry.get("id")

        return None

    def safe_post(self, url, **kwargs):

        while True:
            # session = self.get_auto_session()

            try:
                response = self.session.post(
                    url,
                    timeout=(30, 90),
                    stream=False,
                    **kwargs
                )

                time.sleep(3)
                status = response.status_code

                # ---------- STATUS CHECK ----------
                if status != 200:
                    logger.warning(f"HTTP {status}")
                    print(response.text)

                    response.close()
                    session = self.get_auto_session()  # refresh session and cookies
                    self.session = session  # update session in crawler instance
                    time.sleep(3)
                    continue

                # ---------- JSON PARSE ----------
                try:
                    data = response.json()
                    response.close()
                    return data

                except ValueError:
                    logger.warning("Invalid JSON response")
                    response.close()
                    time.sleep(3)
                    continue

            # ---------- TIMEOUT ----------
            except requests.exceptions.ReadTimeout:
                logger.warning("Read timeout")

            # ---------- CONNECTION ERROR ----------
            except requests.exceptions.ConnectionError:
                logger.warning("Connection error → recreating session")

            # ---------- ANY OTHER ERROR ----------
            except Exception as e:
                logger.warning(f"Unexpected error → {e}")

            # ---------- RETRY ----------
            print("Retrying...!!!")
            time.sleep(3)
            
    def reset_session(self):

        try:
            self.session.close()
        except:
            pass

        self.session = requests.Session()

        self.session.headers.update({
            "User-Agent": self.USER_AGENT
        })

        logger.info("Session recreated")

    def get_distributor_list(self,c_id):

        params = {
            'operation': 'getFacets',
        }

        json_data = {
            'query': 'query getFacets($searchTerm: String, $country: String!, $filters: Map, $attributeNames: [String!]!, $needsDistributor: Boolean!, $needsManufacturer: Boolean!, $needsCadLevel: Boolean = false, $aggSize: Int!, $in_stock_only: Boolean, $has_datasheet_only: Boolean) {\n  search(\n    q: $searchTerm\n    country: $country\n    filters: $filters\n    in_stock_only: $in_stock_only\n    has_datasheet_only: $has_datasheet_only\n  ) {\n    manufacturer_agg(size: $aggSize) @include(if: $needsManufacturer) {\n      ...CompanyBucket\n    }\n    distributor_agg(size: $aggSize) @include(if: $needsDistributor) {\n      ...CompanyBucket\n    }\n    spec_aggs(attribute_names: $attributeNames, size: $aggSize) {\n      ...SpecAgg\n    }\n    cad_level_agg @include(if: $needsCadLevel) {\n      level\n      display_name\n      count\n    }\n  }\n}\n\nfragment CompanyBucket on CompanyBucket {\n  company {\n    id\n    name\n  }\n  count\n}\n\nfragment SpecAgg on SpecAgg {\n  attribute {\n    shortname\n    name\n    value_type\n    units_symbol\n  }\n  buckets {\n    display_value\n    float_value\n    count\n  }\n  min\n  max\n}',
            'variables': {
                'needsDistributor': True,
                'needsManufacturer': True,
                'needsCadLevel': False,
                'attributeNames': [
                    'rohs',
                    'lifecyclestatus',
                ],
                'country': 'IN',
                'aggSize': 500,
                'filters': {
                    'category_id': c_id,
                },
            },
            'operationName': 'getFacets',
        }

        response = self.session.post('https://octopart.com/api/v4/internal', params=params, headers=HEADERS, json=json_data)
        with open('distributors_new.json','w') as dd:
            json.dump(response.json(),dd,indent=2)



    # def find_dist_id_by_name(self, substring, category_id):
    #     self.get_distributor_list(category_id)
    #     with open("distributors_new.json", "r", encoding="utf-8") as f:
    #         data = json.load(f)

        # Use .get() safely
        # distributor = data.get("data", {}).get("search", {}).get("distributor_agg", [])
        # substring = substring.replace('-',' ')
        # # Find matching IDs
        # matching_ids = [ m.get("company", {}).get("id") 
        #                 for m in distributor 
        #                 if substring in m.get("company", {}).get("name", "").lower() ]

        # print(matching_ids)
        # print(15*"!@#$%")
        # return matching_ids

    def find_manufacturer_id_by_name(self, category_id):
        with open("all_filters.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        # Use .get() safely
        manufacturers = data.get("data", {}).get("search", {}).get("manufacturer_agg", [])
         # Build list of (id, name) 
        manufacturer_list = [ {"id": m.get("company", {}).get("id"), "name": m.get("company", {}).get("name")} for m in manufacturers ]

        return manufacturer_list


    def api_get_request(self, category_id, page_url):
        base_filter = {
            "category_id": [str(category_id[0])],
            "distributor_id": [str(self.distributor_id)]
        }

        # 1️⃣ Manufacturer loop

        manufacturers = self.find_manufacturer_id_by_name(category_id[0])
        if len(manufacturers)> 0:
            for manu_id in manufacturers:
                filters = base_filter.copy()
                filters["manufacturer_id"] = [str(manu_id['id'])]
                print(filters)

                self.run_filter_with_pagination(filters, page_url)

        # 2️⃣ Spec loop
        spec_filters = self.extract_spec_filters()

        for spec_key, values in spec_filters.items():

            for value in values:
                
                filters = base_filter.copy()
                filters[spec_key] = [value.split(' ')[0]]
                print(filters)
                self.run_filter_with_pagination(filters, page_url)

            self.checkpoint.setdefault("completed_filters", [])

            if filters not in self.checkpoint["completed_filters"]:
                self.checkpoint["completed_filters"].append(filters)

            self.checkpoint["current_filter"] = None
            self.checkpoint["start"] = 0

            self.save_checkpoint(self.checkpoint)

    def run_filter_with_pagination(self, filters, page_url):
        time.sleep(2)
        logger.info("run_filter_with_pagination ")
        completed_filters = self.checkpoint.get("completed_filters", [])
        if filters in completed_filters:
            logger.info(f"Skipping completed filter: {filters}")
            return

        resume_filter = self.checkpoint.get("current_filter")
        resume_start = self.checkpoint.get("start", 0)

        LIMIT = 15
        MAX_PAGES = 1020

        start = 0
        page_count = 0
        if resume_filter == filters:
            start = resume_start
            logger.info(f"Resuming from {start}")

        # Resume from checkpoint
        if self.checkpoint:
            cp_filters = self.checkpoint.get("filters")

            if cp_filters == filters:
                start = self.checkpoint.get("start", 0)
                print("Resuming from checkpoint start:", start)

        logger.info(f"Starting filter crawl → {filters}")
        print("REQUEST FILTERS:", filters)
        print("START:", start)

        while True:
            params = {
                'operation': 'UseSearchQuery',
            }
            print(filters)
            json_data = {
                'query': 'query UseSearchQuery($country: String!, $currency: String!, $filters: Map, $in_stock_only: Boolean, $has_datasheet_only: Boolean, $limit: Int!, $q: String, $sort: String, $sort_dir: SortDirection, $start: Int, $isSpecsView: Boolean!, $includeBrokers: Boolean = false, $priceBreaks: [Int!], $sellerLimit: Int, $inference_rules: InferenceRules) {\n  search(\n    country: $country\n    currency: $currency\n    filters: $filters\n    in_stock_only: $in_stock_only\n    has_datasheet_only: $has_datasheet_only\n    limit: $limit\n    q: $q\n    sort: $sort\n    sort_dir: $sort_dir\n    start: $start\n    inference_rules: $inference_rules\n  ) {\n    ...SimilarPartsSerpData\n    topline_advert_v2 {\n      ...ToplineAdvertToplineAd\n    }\n    ...SerpAnalytics\n    ...SpecsViewAttributeGroup @include(if: $isSpecsView)\n    applied_category {\n      ancestors {\n        id\n        name\n        path\n      }\n      id\n      name\n      path\n      relevant_attributes {\n        id\n        name\n        shortname\n        group\n        short_displayname\n      }\n    }\n    results {\n      ...SpecsViewResults @include(if: $isSpecsView)\n      ...PricesViewResults @skip(if: $isSpecsView)\n      ...TowerAdvertResults\n    }\n    applied_filters {\n      display_values\n      name\n      shortname\n      values\n      units_symbol\n    }\n    suggested_categories {\n      category {\n        id\n        name\n      }\n    }\n    child_category_agg(size: 25) {\n      category {\n        id\n        name\n      }\n      count\n    }\n    hits\n  }\n}\n\nfragment SimilarPartsSerpData on PartResultSet {\n  hits\n  results {\n    part {\n      counts\n      id\n    }\n  }\n}\n\nfragment ToplineAdvertToplineAd on ToplineAdvertV2 {\n  is_co_op\n  click_url\n  text\n  price_breaks\n  seller {\n    company {\n      id\n      name\n    }\n  }\n  offer {\n    id\n    inventory_level\n    prices {\n      quantity\n      converted_price\n      converted_currency\n      conversion_rate\n    }\n  }\n  part {\n    mpn\n    manufacturer {\n      id\n      name\n    }\n  }\n}\n\nfragment SerpAnalytics on PartResultSet {\n  hits\n  suggested_categories {\n    category {\n      id\n    }\n  }\n  suggested_filters {\n    id\n  }\n  results {\n    part {\n      id\n    }\n  }\n}\n\nfragment SpecsViewAttributeGroup on PartResultSet {\n  specs_view_attribute_groups {\n    name\n    attributes {\n      id\n      name\n      shortname\n      group\n      short_displayname\n      units_symbol\n    }\n  }\n}\n\nfragment SpecsViewResults on PartResult {\n  part {\n    ...PdmcPart\n    ...SpecsViewPartCell\n    ...SpecsViewCompareCell\n    counts\n    id\n    mpn\n    estimated_factory_lead_days\n    slug\n    descriptions {\n      text\n    }\n    manufacturer {\n      name\n      is_verified\n    }\n    specs {\n      display_value\n      value\n      attribute {\n        id\n        name\n        shortname\n        group\n        short_displayname\n      }\n    }\n    median_price_1000 {\n      converted_price\n      converted_currency\n    }\n    cad_models {\n      has_symbol\n      has_footprint\n      has_3d_model\n    }\n    best_datasheet {\n      url\n    }\n    sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n      is_authorized\n      offers(priceBreaks: $priceBreaks) {\n        inventory_level\n      }\n    }\n    allSellers: sellers(include_brokers: $includeBrokers) {\n      is_authorized\n      is_broker\n    }\n    best_image {\n      url\n    }\n  }\n}\n\nfragment PdmcPart on Part {\n  id\n  mpn\n  slug\n  counts\n  cad {\n    footprint_image_url\n    symbol_image_url\n  }\n  manufacturer {\n    is_verified\n    name\n  }\n}\n\nfragment SpecsViewPartCell on Part {\n  ...PdmcPart\n  id\n  mpn\n  slug\n  specs {\n    display_value\n    value\n    attribute {\n      shortname\n    }\n  }\n  manufacturer {\n    name\n    is_verified\n    slug\n  }\n  best_datasheet {\n    url\n  }\n  descriptions {\n    text\n  }\n  cad_models {\n    has_symbol\n    has_footprint\n    has_3d_model\n  }\n}\n\nfragment SpecsViewCompareCell on Part {\n  id\n  mpn\n  manufacturer {\n    name\n  }\n  slug\n  best_image {\n    url\n  }\n}\n\nfragment PricesViewResults on PartResult {\n  ...PricesViewPartResult\n  part {\n    ...PricesViewPart\n    id\n    slug\n    sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n      company {\n        id\n        is_distributorapi\n      }\n      offers(priceBreaks: $priceBreaks) {\n        id\n      }\n    }\n  }\n}\n\nfragment PricesViewPartResult on PartResult {\n  ...PricesViewHeaderPartResult\n}\n\nfragment PricesViewHeaderPartResult on PartResult {\n  aka_mpn\n  description\n}\n\nfragment PricesViewPart on Part {\n  id\n  ...PricesViewHeaderPart\n  ...OfferTableListPart\n  ...PricesViewFooterPart\n  sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n    ...OfferTableListSeller\n  }\n}\n\nfragment PricesViewHeaderPart on Part {\n  id\n  mpn\n  slug\n  v3uid\n  free_sample_url\n  manufacturer_url\n  manufacturer {\n    name\n    is_verified\n  }\n  median_price_1000 {\n    converted_price\n    converted_currency\n  }\n  best_image {\n    url\n  }\n  ...SegmentAltPartEvent\n  ...Series\n  ...DatasheetButton\n  ...CadModelsLink\n  ...PdmcPart\n}\n\nfragment SegmentAltPartEvent on Part {\n  id\n  mpn\n  slug\n  manufacturer {\n    id\n    name\n  }\n}\n\nfragment Series on Part {\n  series {\n    url\n    name\n  }\n}\n\nfragment DatasheetButton on Part {\n  best_datasheet {\n    url\n  }\n  manufacturer {\n    name\n  }\n  id\n  mpn\n  slug\n}\n\nfragment CadModelsLink on Part {\n  slug\n  cad_models {\n    has_symbol\n    has_footprint\n    has_3d_model\n    highest_level\n  }\n}\n\nfragment OfferTableListPart on Part {\n  ...OfferTablePart\n  slug\n}\n\nfragment OfferTablePart on Part {\n  id\n  ...SellerPart\n  ...SummaryPart\n  ...OfferPart\n}\n\nfragment SellerPart on Part {\n  ...OfferPart\n}\n\nfragment OfferPart on Part {\n  id\n  v3uid\n  mpn\n  manufacturer {\n    name\n  }\n  sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n    company {\n      id\n    }\n  }\n}\n\nfragment SummaryPart on Part {\n  id\n  v3uid\n  mpn\n  manufacturer {\n    name\n  }\n  sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n    company {\n      id\n    }\n  }\n}\n\nfragment PricesViewFooterPart on Part {\n  slug\n  ...PdmcPart\n}\n\nfragment OfferTableListSeller on PartSeller {\n  ...OfferTableSeller\n  is_authorized\n  is_broker\n  is_rfq\n  company {\n    id\n    name\n  }\n  offers(priceBreaks: $priceBreaks) {\n    id\n    inventory_level\n    prices {\n      converted_price\n      quantity\n      conversion_rate\n    }\n  }\n}\n\nfragment OfferTableSeller on PartSeller {\n  ...Seller\n  ...OfferSeller\n  ...SummarySeller\n  company {\n    id\n  }\n  offers(priceBreaks: $priceBreaks) {\n    ...Offer\n    id\n    click_url\n  }\n}\n\nfragment Seller on PartSeller {\n  ...OfferSeller\n  offers(priceBreaks: $priceBreaks) {\n    id\n    ...Offer\n  }\n}\n\nfragment OfferSeller on PartSeller {\n  ...IsAuthSeller\n  is_rfq\n  is_authorized\n  is_broker\n  company {\n    id\n    name\n    is_distributorapi\n  }\n}\n\nfragment IsAuthSeller on PartSeller {\n  is_authorized\n  is_broker\n}\n\nfragment Offer on Offer {\n  id\n  updated\n  inventory_level\n  click_url\n  moq\n  packaging\n  sku\n  prices {\n    price\n    quantity\n    currency\n    converted_price\n    converted_currency\n    conversion_rate\n  }\n}\n\nfragment SummarySeller on PartSeller {\n  ...IsAuthSeller\n  is_authorized\n  is_rfq\n  is_broker\n  company {\n    id\n    name\n    is_distributorapi\n  }\n  offers(priceBreaks: $priceBreaks) {\n    id\n    click_url\n    inventory_level\n    updated\n    prices {\n      price\n      quantity\n      converted_price\n      converted_currency\n      conversion_rate\n      currency\n    }\n  }\n}\n\nfragment TowerAdvertResults on PartResult {\n  aka_mpn\n  part {\n    v3uid\n    category {\n      id\n    }\n    manufacturer {\n      id\n    }\n    sellers(include_brokers: $includeBrokers, limit: $sellerLimit) {\n      company {\n        id\n      }\n      offers(priceBreaks: $priceBreaks) {\n        inventory_level\n        prices {\n          currency\n        }\n      }\n    }\n  }\n}',
                'variables': {
                        
                        # Filters generate like below
                        #'filters':{'category_id': ['6339'], 'distributor_id': ['10079'], 'manufacturer_id': ['24']},
                        'filters':filters,
                        'in_stock_only': False,
                        'has_datasheet_only': False,
                        'currency': 'USD',
                        'q': '',
                        'start': start,
                        'country': 'IN',
                        'limit': LIMIT,
                        'isSpecsView': False,
                        "priceBreaks": [1,10,100,1000,10000],
                        'sellerLimit': 6,
                    },
                "operationName": "UseSearchQuery",
            }
            data = self.safe_post('https://octopart.com/api/v4/internal', params=params, json=json_data)
            time.sleep(5)            # data = self.safe_post("https://octopart.com/api/v4/internal", params=params, headers=headers, cookies=cookies, json=json_data)
            # print(LIMIT)
            # -----------------------------
            # HARD GUARD
            # -----------------------------
            if not data or not isinstance(data, dict):
                logger.warning(
                    f"Bad API response → filter={filters} start={start}"
                )
                break

            search = (data.get("data") or {}).get("search")

            if not search:
                logger.warning(
                    f"Search missing → filter={filters} start={start}"
                )
                break

            results = search.get("results") or []
            total_hits = search.get("hits", 0)
            print("Results Count: ", len(results))
            # -----------------------------
            # NO RESULTS
            # -----------------------------
            if not results:
                logger.info(f"No results → filter={filters} start={start}")
                break

            # -----------------------------
            # PARSE RESULTS
            # -----------------------------
            rows = self.parse_api_results(
                results,
                filters,
                page_url
            )

            if rows:
                self.save_parts_sql(rows)

            logger.info(
                f"Filter={filters} | start={start} "
                f"| saved={len(rows)} | total={total_hits}"
            )

            # -------- SAVE CHECKPOINT --------
            self.checkpoint["current"] = {
                "filters": filters,
                "start": start
                    }

            # self.save_checkpoint(filters, start, page_url)

            # -----------------------------
            # NEXT PAGE
            # -----------------------------
            start += len(results)
            # start += LIMIT
            page_count += 1
            self.checkpoint["current"] = {
                "filters": filters,
                "start": start
                    }
            # Reached last page
            if start >= total_hits:
                logger.info("Reached last page")

                if os.path.exists("checkpoint.json"):
                    os.remove("checkpoint.json")
                break

            # Safety break
            if page_count >= MAX_PAGES:
                logger.warning(
                    f"Pagination safety break → {filters}"
                )
                break              
        self.checkpoint["completed"] = filters

    def parse_api_results(self, data, filters, substring):

        rows = []
        for item in data:
            part = item.get("part", {})
            part_id = part.get("id", "")
            mpn = part.get("mpn", "")
            slug = part.get("slug", "")
            part_url = f"https://octopart.com{slug}" if slug else ""

            # distributor =  self.current_distributor # filters.get("distributor_id", [""])[0]
            # manufacturer = filters.get("manufacturer_id", [""])[0]

            category_id = filters.get("category_id", [""])
            category_id = category_id[0] if category_id else ""

            category = self.get_category_name(category_id)
            page_url = self.get_category_path(category_id)

            rows.append({
                "distributor": self.current_distributor,
                "category": category,
                "part_id": part_id,
                "title": mpn,
                "part_url": part_url,
                "page_url": substring
            })

        with open('test.log','a', encoding='utf-8') as fx:
            fx.write(str(rows))

        return rows
    
    def get_category_name(self, category_id):
        return self.category_name_map.get(str(category_id), "")

    def get_category_path(self, category_id):
        return self.category_path_map.get(str(category_id), "")

    def load_category_mapping(self):

        with open("categories.json", encoding="utf-8") as f:
            data = json.load(f)

        categories = data["data"]["categories"]

        # id → name
        self.category_name_map = {
            str(c["id"]): c["name"]
            for c in categories
        }

        # slug → id  ✅ IMPORTANT
        self.category_slug_map = {}

        for c in categories:

            path = c.get("path", "")

            slug = path.strip("/").split("/")[-1]

            self.category_slug_map[slug] = c["id"]


    def scrape_child_categories(self, url):

        # ---------- RESUME SUPPORT ----------
        start_page = 0
        if self.checkpoint.get("page") is not None:
            start_page = int(self.checkpoint["page"])

        pages = self.get_pages(url)

        if pages >= 100:
            total = 130
        else:
            total = pages + 1

        for page in range(start_page, total):
            start = page * 10   # ✅ define start correctly
            page_url = f"{url}?start={start}"
            logger.info(f"Scraping: {page_url}")
            soup = self.get_soup(page_url)
            if not soup:
                continue

            self.scrape_page(soup, page_url)

            # ---------- SAFE PATH EXTRACTION ----------
            parts = (
                page_url
                .replace("https://octopart.com/distributors/", "")
                .split("?")[0]
                .split("/")
            )

            parts += [""] * (4 - len(parts))

    def delete_checkpoint(self):
        if os.path.exists("checkpoint.json"):
            os.remove("checkpoint.json")
            logger.info("Checkpoint deleted")
    
    def mark_distributor_pending(self):
        conn = self.db_connector()
        cursor = conn.cursor()
        query="UPDATE distributor_crawling SET crawling_status='pending' WHERE crawling_status!='pending'"
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def main(self):
        while True:
            self.delete_checkpoint()
            # self.mark_distributor_pending()
            self.run()
            self.delete_checkpoint()
            self.mark_distributor_pending()
            time.sleep(3600)  # Sleep for 1 hour before next crawl cycle
            logger.info("Restarting crawl cycle after 1 hour sleep")


while True:
    if __name__ == "__main__":
        try:
            crawler = OctopartCrawler()
            crawler.main()
            
        except Exception as e:
            logger.exception("FATAL ERROR")
            print("Program crashed. Check crawler.log")
            crawler.delete_checkpoint()
            crawler.mark_distributor_pending()