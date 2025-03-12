import requests
import sqlite3
import json
import concurrent.futures
from itemKeyMaker import decode_item_bytes, create_item_key

def fetch_auctions(page, session):
    url = f"https://api.hypixel.net/skyblock/auctions?bin=true&page={page}"
    response = session.get(url)
    if response.status_code != 200:
        print(f"Error fetching page {page}: HTTP {response.status_code}")
        return []
    data = response.json()
    if not data.get("success"):
        print(f"API error on page {page}")
        return []
    return data.get("auctions", [])

def remove_outliers(prices):
    if len(prices) < 4:
        return prices
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(sorted_prices) // 4]
    q3 = sorted_prices[(3 * len(sorted_prices)) // 4]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return [p for p in prices if lower_bound <= p <= upper_bound]

def update_averages_db_and_json(key, plain_item, average, volume):
    conn = sqlite3.connect('currentAuctions.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS averages (
                    key TEXT PRIMARY KEY,
                    plain_item TEXT,
                    average REAL,
                    volume INTEGER
                )''')
    c.execute('''INSERT OR REPLACE INTO averages (key, plain_item, average, volume) VALUES (?, ?, ?, ?)''',
              (key, plain_item, average, volume))
    conn.commit()
    conn.close()

    try:
        with open('currentAuctions.json', 'r') as jf:
            averages_data = json.load(jf)
    except (FileNotFoundError, json.JSONDecodeError):
        averages_data = {}
    averages_data[key] = {
        "plain_item": plain_item,
        "average": average,
        "volume": volume
    }
    with open('currentAuctions.json', 'w') as jf:
        json.dump(averages_data, jf, indent=4)

def process_auctions(auctions, item_prices, options):
    for auction in auctions:
        if auction.get('item_bytes'):
            detail = decode_item_bytes(auction['item_bytes'])
            auction['detail'] = detail['i'][0]
            key = str(create_item_key(auction))
        else:
            plain_item = auction.get("item_name")
            if isinstance(plain_item, bytes):
                plain_item = plain_item.decode('utf-8')
            key = plain_item
        price = auction.get("starting_bid", 0)
        if key:
            if key not in item_prices:
                item_prices[key] = {"prices": [], "plain_item": auction.get("item_name")}
            item_prices[key]["prices"].append(price)

def main():
    with open('options.json', 'r') as f:
        options = json.load(f)

    session = requests.Session()
    temp_response = session.get("https://api.hypixel.net/skyblock/auctions?bin=true&page=0")
    if temp_response.status_code != 200:
        print(f"Error fetching total pages: HTTP {temp_response.status_code}")
        return
    total_pages = temp_response.json().get("totalPages", 0)
    print(f"Total pages: {total_pages}")

    item_prices = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        page_range = range(total_pages + 1)
        results = list(executor.map(lambda page: fetch_auctions(page, session), page_range))

    for auctions in results:
        if auctions:
            process_auctions(auctions, item_prices, options)

    for key, data in sorted(item_prices.items()):
        prices = data["prices"]
        cleaned_prices = remove_outliers(prices)
        if cleaned_prices:
            average = sum(cleaned_prices) / len(cleaned_prices)
            volume = len(cleaned_prices)
            update_averages_db_and_json(key, data["plain_item"], average, volume)
            # print(f"Updated {data['plain_item']} (key: {key}): average = {average:.2f} based on {volume} prices")
        else:
            # print(f"No valid prices for {data['plain_item']} (key: {key}) after removing outliers.")
            pass

if __name__ == "__main__":
    main()
