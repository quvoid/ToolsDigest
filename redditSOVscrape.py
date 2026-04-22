import requests
import pandas as pd
import time
from collections import defaultdict
import os
import signal
import sys

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# ================================================================
# ✅ BEAUTY SUBREDDITS ONLY
# ================================================================
SUBREDDITS = [
    "IndianMakeupAndBeauty", "IndianSkincareAddicts", "IndiaBeautyDeals",
    "MakeupAddiction", "Makeup", "MakeupLounge", "MakeupRehab",
    "SkincareAddiction", "AsianBeauty", "acne", "DIYBeauty", "NaturalBeauty",
    "drugstorebeauty", "MUAontheCheap", "Indiemakeupandmore",
    "brownbeauty", "BrownMakeup", "BeautyGuruChatter", "sephora",
    "elfcosmetics", "crueltyfreebeauty",
]

BRANDS = ["Sugar", "Kay Beauty", "Lakme", "Type Beauty", "NYX"]

# ================================================================
# ✅ KEYWORDS — with subreddit restriction
# ================================================================
SEARCHES = [
    ("Sugar concealer",                     "IndianMakeupAndBeauty"),
    ("Sugar concealer",                     "IndianSkincareAddicts"),
    ("Kay Beauty concealer",                "IndianMakeupAndBeauty"),
    ("Kay Beauty concealer",                "IndianSkincareAddicts"),
    ("Lakme concealer",                     "IndianMakeupAndBeauty"),
    ("Lakme concealer",                     "IndianSkincareAddicts"),
    ("Type Beauty concealer",               "IndianMakeupAndBeauty"),
    ("NYX concealer",                       "IndianMakeupAndBeauty"),
    ("NYX concealer",                       "MakeupAddiction"),
    ("NYX concealer",                       "SkincareAddiction"),
    ("Fit Me Spot Rescue Concealer",        "IndianMakeupAndBeauty"),
    ("Fit Me Spot Rescue Concealer",        "MakeupAddiction"),
    ("Fit Me Spot Rescue",                  "drugstorebeauty"),
    ("Vitamin C Salicylic Concealer",       "IndianMakeupAndBeauty"),
    ("Vitamin C Salicylic Concealer",       "IndianSkincareAddicts"),
    ("acne friendly concealer",             "IndianMakeupAndBeauty"),
    ("acne friendly concealer",             "SkincareAddiction"),
    ("acne friendly concealer",             "acne"),
    ("acne concealer india",                "IndianSkincareAddicts"),
    ("concealer dark circles",              "IndianMakeupAndBeauty"),
    ("concealer oily skin",                 "IndianMakeupAndBeauty"),
    ("concealer review",                    "IndianMakeupAndBeauty"),
    ("concealer recommendation",            "IndianMakeupAndBeauty"),
    ("best concealer",                      "IndianMakeupAndBeauty"),
    ("best concealer",                      "IndianSkincareAddicts"),
    ("drugstore concealer",                 "drugstorebeauty"),
    ("drugstore concealer",                 "MakeupAddiction"),
    ("concealer",                           "IndianMakeupAndBeauty"),
    ("concealer",                           "IndianSkincareAddicts"),
    ("Sugar Cosmetics",                     "IndianMakeupAndBeauty"),
    ("Sugar Cosmetics",                     "IndianSkincareAddicts"),
    ("Kay Beauty",                          "IndianMakeupAndBeauty"),
    ("Lakme",                               "IndianMakeupAndBeauty"),
    ("NYX",                                 "MakeupAddiction"),
    ("NYX",                                 "drugstorebeauty"),
]

# ================================================================
SAVE_PATH = r"C:\Users\omkar\OneDrive\Desktop\Placeapi\output"
os.makedirs(SAVE_PATH, exist_ok=True)

all_data       = []
mention_counts = defaultdict(lambda: {"posts": 0, "comments": 0})
seen_post_ids  = set()

session = requests.Session()
session.headers.update(HEADERS)

# ================================================================
# ✅ GRACEFUL STOP — press Ctrl+C to stop and auto-save
# ================================================================
stop_requested = False

def handle_stop(sig, frame):
    global stop_requested
    print("\n\n⚠️  Stop requested! Finishing current task then saving...")
    stop_requested = True

signal.signal(signal.SIGINT, handle_stop)


# ================================================================
# ✅ SAVE FUNCTION — reused on normal exit AND Ctrl+C
# ================================================================
def save_results():
    print(f"\n💾 Saving to: {SAVE_PATH}")
    df_all = pd.DataFrame(all_data)

    if df_all.empty:
        print("❌ No data scraped — nothing to save.")
        return

    df_posts    = df_all[df_all["type"] == "post"]
    df_comments = df_all[df_all["type"] == "comment"]

    df_all.to_csv(     f"{SAVE_PATH}\\all_data2.csv",  index=False, encoding="utf-8-sig")
    df_posts.to_csv(   f"{SAVE_PATH}\\posts2.csv",     index=False, encoding="utf-8-sig")
    df_comments.to_csv(f"{SAVE_PATH}\\comments2.csv",  index=False, encoding="utf-8-sig")

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
    df_sov.to_csv(f"{SAVE_PATH}\\sov.csv", index=False, encoding="utf-8-sig")

    print(f"\n✅ SAVED!")
    print(f"   Posts:        {len(df_posts)}")
    print(f"   Comments:     {len(df_comments)}")
    print(f"   Total rows:   {len(df_all)}")
    print(f"   Unique posts: {len(seen_post_ids)}")
    print(f"\n📊 SOV Results:")
    print(df_sov.to_string(index=False))
    print(f"\n📁 Files saved to: {SAVE_PATH}")


# ================================================================
# ✅ PARSE COMMENTS
#    - comment_url  : direct permalink to that specific comment
#    - post_url     : the parent post's URL
# ================================================================
def parse_comments(nodes, post_id, post_title, subreddit, post_permalink):
    for node in nodes:
        if node.get("kind") == "more":
            continue

        c    = node["data"]
        body = c.get("body", "")
        if not body or body in ("[deleted]", "[removed]"):
            continue

        body_lower = body.lower()
        comment_id = c.get("id", "")

        # ✅ Direct link to this specific comment
        comment_url = f"https://reddit.com/comments/{post_id}/_/{comment_id}/"

        # ✅ The post this comment belongs to
        post_url = f"https://reddit.com{post_permalink}"

        all_data.append({
            "type":         "comment",
            "post_id":      post_id,
            "post_title":   post_title,
            "subreddit":    subreddit,
            "author":       c.get("author", "[deleted]"),
            "content":      body,
            "score":        c.get("score", 0),
            "num_comments": "",
            "created_utc":  pd.to_datetime(c["created_utc"], unit="s"),
            "comment_url":  comment_url,   # ✅ direct link to comment
            "post_url":     post_url,      # ✅ parent post link
            "comment_id":   comment_id,
            "parent_id":    c.get("parent_id", ""),
            "is_top_level": c.get("parent_id", "").startswith("t3_"),
            **{f"mentions_{b.replace(' ', '_').replace(chr(39), '')}":
               body_lower.count(b.lower()) for b in BRANDS}
        })

        for brand in BRANDS:
            cnt = body_lower.count(brand.lower())
            if cnt:
                mention_counts[brand]["comments"] += cnt

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
                 f"&restrict_sr=1&sort=new&limit=25&raw_json=1")
        label = f"r/{subreddit} → \"{query}\""
    else:
        url   = (f"https://old.reddit.com/search.json"
                 f"?q={requests.utils.quote(query)}"
                 f"&sort=new&limit=25&raw_json=1")
        label = f"global → \"{query}\""

    page = 1
    while url:
        if stop_requested:
            break

        print(f"  📄 [{label}] Page {page}...", end=" ")
        try:
            res = session.get(url, timeout=20)
            print(f"Status: {res.status_code}")

            if res.status_code == 429:
                print("  ⏳ Rate limited! Waiting 60s...")
                time.sleep(60)
                continue
            if res.status_code != 200:
                print(f"  ❌ Blocked")
                break

            data     = res.json()["data"]
            children = data["children"]

            if not children:
                print(f"  ⚠ No results")
                break

            new_posts = 0
            for post in children:
                if stop_requested:
                    break

                p         = post["data"]
                post_id   = p["id"]
                post_sub  = p.get("subreddit", "")

                if post_id in seen_post_ids:
                    continue
                seen_post_ids.add(post_id)

                title     = p.get("title", "")
                selftext  = p.get("selftext", "")
                text      = (title + " " + selftext).lower()
                permalink = p["permalink"]   # e.g. /r/sub/comments/abc/title/
                post_url  = "https://reddit.com" + permalink

                print(f"     ✅ [r/{post_sub}] {title[:60]}")
                new_posts += 1

                all_data.append({
                    "type":         "post",
                    "post_id":      post_id,
                    "post_title":   title,
                    "subreddit":    post_sub,
                    "author":       p.get("author", "[deleted]"),
                    "content":      selftext if selftext else title,
                    "score":        p.get("score", 0),
                    "num_comments": p.get("num_comments", 0),
                    "created_utc":  pd.to_datetime(p["created_utc"], unit="s"),
                    "comment_url":  "",           # posts don't have a comment_url
                    "post_url":     post_url,     # ✅ full post permalink
                    "comment_id":   "",
                    "parent_id":    "",
                    "is_top_level": "",
                    "search_query": query,
                    **{f"mentions_{b.replace(' ', '_').replace(chr(39), '')}":
                       text.count(b.lower()) for b in BRANDS}
                })

                for brand in BRANDS:
                    cnt = text.count(brand.lower())
                    if cnt:
                        mention_counts[brand]["posts"] += cnt

                # ---- FETCH COMMENTS ----
                time.sleep(3)
                c_res = session.get(
                    f"https://old.reddit.com{permalink}.json?raw_json=1",
                    timeout=20
                )

                if c_res.status_code == 200:
                    try:
                        c_json = c_res.json()
                        before = len(all_data)
                        if len(c_json) > 1:
                            parse_comments(
                                c_json[1]["data"]["children"],
                                post_id, title, post_sub,
                                permalink   # ✅ pass permalink for post_url in comments
                            )
                        added = len(all_data) - before
                        if added:
                            print(f"        💬 {added} comments saved")
                    except Exception as e:
                        print(f"        ⚠ Comment error: {e}")
                else:
                    print(f"        ⚠ Comments {c_res.status_code}")

                time.sleep(3)

            if new_posts == 0:
                print(f"     ⏭ All duplicates, moving on")
                break

            after = data.get("after")
            if after:
                base = url.split("&after=")[0]
                url  = base + f"&after={after}"
            else:
                url = None

            page += 1
            time.sleep(5)

        except Exception as e:
            print(f"  ⚠ Error: {e}")
            break


# ================================================================
# ✅ RUN ALL SEARCHES
# ================================================================
print("\n" + "="*60)
print(f"🔍 Running {len(SEARCHES)} targeted searches")
print("💡 Press Ctrl+C at any time to stop and save what's collected")
print("="*60)

for i, (query, sub) in enumerate(SEARCHES, 1):
    if stop_requested:
        break
    print(f"\n[{i}/{len(SEARCHES)}]")
    scrape(query, sub)
    time.sleep(4)

# ================================================================
# ✅ SAVE — runs whether finished normally OR stopped via Ctrl+C
# ================================================================
save_results()
