# -*- coding: utf-8 -*-
"""
@author: satvi
"""

import argparse, json, sys, requests

def main():
    ap = argparse.ArgumentParser(description="Ask the Cricket Insight Agent")
    ap.add_argument("query", help="Your question in plain English, e.g., 'summary of the 1st match between CSK and MI in 2011'")
    ap.add_argument("--server", default="http://127.0.0.1:8000", help="API server base URL (default: %(default)s)")
    ap.add_argument("--raw", action="store_true", help="Also print raw JSON")
    args = ap.parse_args()

    url = args.server.rstrip("/") + "/ask"
    try:
        resp = requests.post(url, json={"query": args.query}, timeout=30)
    except Exception as e:
        print(f" Could not reach API at {url}: {e}")
        sys.exit(1)

    if resp.status_code != 200:
        print(f" API returned HTTP {resp.status_code}: {resp.text[:2000]}")
        sys.exit(1)

    data = resp.json()
    if args.raw:
        print("\n RAW JSON ")
        print(json.dumps(data, indent=2, ensure_ascii=False))

    # Prefer the server-provided pretty text
    text = data.get("answer_text")
    if text:
        print(text)
        sys.exit(0)

    # Fallback if server didn’t provide pretty text
    if not data.get("ok"):
        hint = data.get("hint")
        if hint:
            print(f" {hint}")
        else:
            print("Sorry, I couldn’t answer that.")
        sys.exit(1)

    print("Query succeeded, but no formatted text was returned.")
    sys.exit(0)

if __name__ == "__main__":
    main()
