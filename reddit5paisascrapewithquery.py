import requests
import pandas as pd
import time
import random
from collections import defaultdict
import os
import signal
from datetime import datetime, timedelta

# ================================================================
# ✅ CONFIGURATION
# ================================================================
MONTHS_TO_SCRAPE = 12

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

def get_headers():
    return {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         "https://www.reddit.com/",
        "DNT":             "1",
        "Connection":      "keep-alive",
    }

# ================================================================
# ✅ BRAND SETUP
# ================================================================
OUR_BRAND   = "5paisa"
COMPETITORS = ["Zerodha", "Groww"]
BRANDS      = [OUR_BRAND] + COMPETITORS

def comment_brand_filter(text_lower: str) -> bool:
    """Save comment if it mentions ANY of the brands."""
    return any(b.lower() in text_lower for b in BRANDS)

# ================================================================
# ✅ SUBREDDITS & SEARCHES
# ================================================================
SUBREDDITS = [
    "IndiaFinance",
    "IndiaInvestments",
    "indiaStockMarket",
    "NSEbets",
    "IndianStockMarket",
    "IndianStreetBets",
    "IndianStocks",
]

SEARCHES = [
    ("mutual funds",            "IndiaFinance"),
    ("mutual funds",            "IndiaInvestments"),
    ("mutual funds",            "indiaStockMarket"),
    ("mutual funds",            "IndianStockMarket"),
    ("mutual funds",            "IndianStocks"),
    ("IPO",                     "IndiaFinance"),
    ("IPO",                     "IndiaInvestments"),
    ("IPO",                     "indiaStockMarket"),
    ("IPO",                     "NSEbets"),
    ("IPO",                     "IndianStockMarket"),
    ("IPO",                     "IndianStreetBets"),
    ("IPO",                     "IndianStocks"),
    ("F&O",                     "IndiaFinance"),
    ("F&O",                     "IndiaInvestments"),
    ("F&O",                     "indiaStockMarket"),
    ("F&O",                     "NSEbets"),
    ("F&O",                     "IndianStockMarket"),
    ("F&O",                     "IndianStreetBets"),
    ("F&O",                     "IndianStocks"),
    ("derivatives",             "IndiaFinance"),
    ("derivatives",             "IndiaInvestments"),
    ("derivatives",             "indiaStockMarket"),
    ("derivatives",             "NSEbets"),
    ("derivatives",             "IndianStockMarket"),
    ("derivatives",             "IndianStreetBets"),
    ("derivatives",             "IndianStocks"),
    ("algorithmic trading",     "IndiaFinance"),
    ("algorithmic trading",     "IndiaInvestments"),
    ("algorithmic trading",     "indiaStockMarket"),
    ("algorithmic trading",     "NSEbets"),
    ("algorithmic trading",     "IndianStockMarket"),
    ("algorithmic trading",     "IndianStreetBets"),
    ("algorithmic trading",     "IndianStocks"),
    ("SIP",                     "IndiaFinance"),
    ("SIP",                     "IndiaInvestments"),
    ("SIP",                     "indiaStockMarket"),
    ("SIP",                     "IndianStockMarket"),
    ("SIP",                     "IndianStocks"),
    ("commodities",             "IndiaFinance"),
    ("commodities",             "IndiaInvestments"),
    ("commodities",             "indiaStockMarket"),
    ("commodities",             "NSEbets"),
    ("commodities",             "IndianStockMarket"),
    ("commodities",             "IndianStreetBets"),
    ("commodities",             "IndianStocks"),
]

# ================================================================
# ✅ SAVE PATH
# ================================================================
BASE_SAVE_PATH = r"C:\Users\omkar\OneDrive\Desktop\scraper_output1"

def get_unique_save_path(base_path):
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_path = f"{base_path}_{timestamp}"
    counter     = 1
    while os.path.exists(unique_path):
        unique_path = f"{base_path}_{timestamp}_{counter}"
        counter += 1
    os.makedirs(unique_path)
    return unique_path

SAVE_PATH = get_unique_save_path(BASE_SAVE_PATH)

all_data        = []
mention_counts  = defaultdict(lambda: {"posts": 0, "comments": 0})
seen_post_ids   = set()

session = requests.Session()

SCRAPE_UNTIL_DATE      = datetime.utcnow() - timedelta(days=MONTHS_TO_SCRAPE * 30)
SCRAPE_UNTIL_TIMESTAMP = SCRAPE_UNTIL_DATE.timestamp()

# ================================================================
# ✅ GRACEFUL STOP
# ================================================================
stop_requested = False

def handle_stop(sig, frame):
    global stop_requested
    print("\n\n⚠️  Stop requested! Finishing current task then saving...")
    stop_requested = True

signal.signal(signal.SIGINT, handle_stop)


# ================================================================
# ✅ SAFE GET — rotate headers + exponential backoff
# ================================================================
def safe_get(url, retries=4):
    wait = 60
    for attempt in range(retries):
        session.headers.update(get_headers())
        time.sleep(random.uniform(2, 5))   # jitter before every request
        try:
            res = session.get(url, timeout=25)

            if res.status_code == 200:
                return res

            if res.status_code == 429:
                print(f"  ⏳ Rate limited (attempt {attempt+1}/{retries}) — waiting {wait}s...")
                time.sleep(wait + random.uniform(0, 15))
                wait *= 2
                continue

            if res.status_code in (403, 404):
                print(f"  ❌ HTTP {res.status_code} — skipping URL")
                return None

            print(f"  ⚠ HTTP {res.status_code} — retrying...")
            time.sleep(wait)
            wait *= 2

        except Exception as e:
            print(f"  ⚠ Request error: {e} — retrying in {wait}s...")
            time.sleep(wait + random.uniform(0, 10))
            wait *= 2

    print("  ❌ Failed after all retries.")
    return None


# ================================================================
# ✅ SAVE FUNCTION
# ================================================================
def save_results():
    print(f"\n💾 Saving to: {SAVE_PATH}")
    df_all = pd.DataFrame(all_data)

    if df_all.empty:
        print("❌ No data scraped — nothing to save.")
        return

    df_posts    = df_all[df_all["type"] == "post"]
    df_comments = df_all[df_all["type"] == "comment"]

    df_all.to_csv(      os.path.join(SAVE_PATH, "all_data.csv"),          index=False, encoding="utf-8-sig")
    df_posts.to_csv(    os.path.join(SAVE_PATH, "posts.csv"),             index=False, encoding="utf-8-sig")
    df_comments.to_csv( os.path.join(SAVE_PATH, "comments.csv"),          index=False, encoding="utf-8-sig")

    # ---- SOV ----
    rows = []
    for brand in BRANDS:
        p_c = mention_counts[brand]["posts"]
        c_c = mention_counts[brand]["comments"]
        rows.append({
            "brand":            brand,
            "post_mentions":    p_c,
            "comment_mentions": c_c,
            "total":            p_c + c_c
        })

    df_sov = pd.DataFrame(rows)
    total  = df_sov["total"].sum()
    df_sov["SOV_%"] = (df_sov["total"] / total * 100).round(1) if total > 0 else 0
    df_sov = df_sov.sort_values("SOV_%", ascending=False)
    df_sov.to_csv(os.path.join(SAVE_PATH, "sov.csv"), index=False, encoding="utf-8-sig")

    print(f"\n✅ SAVED!")
    print(f"   Posts:        {len(df_posts)}")
    print(f"   Comments:     {len(df_comments)}")
    print(f"   Total rows:   {len(df_all)}")
    print(f"   Unique posts: {len(seen_post_ids)}")
    print(f"\n📊 SOV Results:")
    print(df_sov.to_string(index=False))
    print(f"\n📁 Files saved to: {SAVE_PATH}")


# ================================================================
# ✅ PARSE COMMENTS — brand filter applied here
# ================================================================
def parse_comments(nodes, post_id, post_title, subreddit, post_permalink):
    for node in nodes:
        if node.get("kind") == "more":
            continue

        c           = node["data"]
        created_utc = c.get("created_utc", 0)

        if created_utc < SCRAPE_UNTIL_TIMESTAMP:
            continue

        body = c.get("body", "")
        if not body or body in ("[deleted]", "[removed]"):
            continue

        body_lower = body.lower()
        comment_id = c.get("id", "")

        # ✅ OR filter — save if ANY brand is mentioned
        if comment_brand_filter(body_lower):
            comment_url = f"https://reddit.com/comments/{post_id}/_/{comment_id}/"
            post_url    = f"https://reddit.com{post_permalink}"

            all_data.append({
                "type":                 "comment",
                "post_id":              post_id,
                "post_title":           post_title,
                "subreddit":            subreddit,
                "author":               c.get("author", "[deleted]"),
                "content":              body,
                "score":                c.get("score", 0),
                "num_comments":         "",
                "created_utc":          pd.to_datetime(created_utc, unit="s"),
                "comment_url":          comment_url,
                "post_url":             post_url,
                "comment_id":           comment_id,
                "parent_id":            c.get("parent_id", ""),
                "is_top_level":         c.get("parent_id", "").startswith("t3_"),
                "brands_mentioned":     ", ".join(
                    b for b in BRANDS if b.lower() in body_lower
                ),
                **{f"mentions_{b.replace(' ', '_').replace(chr(39), '')}":
                   body_lower.count(b.lower()) for b in BRANDS}
            })

            for brand in BRANDS:
                cnt = body_lower.count(brand.lower())
                if cnt:
                    mention_counts[brand]["comments"] += cnt

        # Always recurse into replies
        replies = c.get("replies")
        if isinstance(replies, dict):
            parse_comments(
                replies["data"]["children"],
                post_id, post_title, subreddit, post_permalink
            )


# ================================================================
# ✅ SCRAPE
# ================================================================
def scrape(query, subreddit=None):
    global stop_requested

    if subreddit:
        url   = (f"https://old.reddit.com/r/{subreddit}/search.json"
                 f"?q={requests.utils.quote(query)}"
                 f"&restrict_sr=1&sort=new&t=all&limit=25&raw_json=1")
        label = f"r/{subreddit} → \"{query}\""
    else:
        url   = (f"https://old.reddit.com/search.json"
                 f"?q={requests.utils.quote(query)}"
                 f"&sort=new&t=all&limit=25&raw_json=1")
        label = f"global → \"{query}\""

    page = 1
    while url:
        if stop_requested:
            break

        print(f"  📄 [{label}] Page {page}...", end=" ")

        res = safe_get(url)
        if res is None:
            break

        print(f"OK")

        try:
            data     = res.json()["data"]
            children = data["children"]
        except Exception as e:
            print(f"  ⚠ JSON parse error: {e}")
            break

        if not children:
            print(f"  ⚠ No results")
            break

        old_posts_count = 0

        for post in children:
            if stop_requested:
                break

            p           = post["data"]
            post_id     = p["id"]
            post_sub    = p.get("subreddit", "")
            created_utc = p.get("created_utc", 0)

            if created_utc < SCRAPE_UNTIL_TIMESTAMP:
                old_posts_count += 1
                continue

            if post_id in seen_post_ids:
                continue
            seen_post_ids.add(post_id)

            title     = p.get("title", "")
            selftext  = p.get("selftext", "")
            text      = (title + " " + selftext).lower()
            permalink = p["permalink"]
            post_url  = "https://reddit.com" + permalink

            print(f"     ✅ [r/{post_sub}] {title[:60]}")

            # Posts saved unconditionally (no filter on posts)
            all_data.append({
                "type":             "post",
                "post_id":          post_id,
                "post_title":       title,
                "subreddit":        post_sub,
                "author":           p.get("author", "[deleted]"),
                "content":          selftext if selftext else title,
                "score":            p.get("score", 0),
                "num_comments":     p.get("num_comments", 0),
                "created_utc":      pd.to_datetime(created_utc, unit="s"),
                "comment_url":      "",
                "post_url":         post_url,
                "comment_id":       "",
                "parent_id":        "",
                "is_top_level":     "",
                "search_query":     query,
                "brands_mentioned": ", ".join(
                    b for b in BRANDS if b.lower() in text
                ),
                **{f"mentions_{b.replace(' ', '_').replace(chr(39), '')}":
                   text.count(b.lower()) for b in BRANDS}
            })

            for brand in BRANDS:
                cnt = text.count(brand.lower())
                if cnt:
                    mention_counts[brand]["posts"] += cnt

            # Fetch comments
            c_res = safe_get(
                f"https://old.reddit.com{permalink}.json?raw_json=1"
            )

            if c_res and c_res.status_code == 200:
                try:
                    c_json = c_res.json()
                    before = len(all_data)
                    if len(c_json) > 1:
                        parse_comments(
                            c_json[1]["data"]["children"],
                            post_id, title, post_sub, permalink
                        )
                    added = len(all_data) - before
                    if added:
                        print(f"        💬 {added} brand comments saved")
                except Exception as e:
                    print(f"        ⚠ Comment error: {e}")

            time.sleep(random.uniform(4, 7))

        if old_posts_count == len(children):
            print(f"     ⏭ All posts older than {MONTHS_TO_SCRAPE} months — stopping pagination")
            break

        after = data.get("after")
        if after:
            base = url.split("&after=")[0]
            url  = base + f"&after={after}"
        else:
            url = None

        page += 1
        time.sleep(random.uniform(6, 10))

# ================================================================
# ✅ RUN
# ================================================================
print("\n" + "="*60)
print(f"🔍 Running {len(SEARCHES)} targeted searches")
print(f"🎯 Comment filter: ANY of {BRANDS}")
print(f"📅 Last {MONTHS_TO_SCRAPE} months (since {SCRAPE_UNTIL_DATE.strftime('%Y-%m-%d')})")
print("💡 Press Ctrl+C to stop and save")
print("="*60)

for i, (query, sub) in enumerate(SEARCHES, 1):
    if stop_requested:
        break
    print(f"\n[{i}/{len(SEARCHES)}]")
    scrape(query, sub)
    save_results()
    time.sleep(random.uniform(4, 8))

save_results()
