import requests
import json
import time

def fetch(url):
    response = requests.get(url)
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        print("Error: Received invalid JSON from", url)
        return {}

def main():
    # test fetch
    data = fetch("https://example.com/json")
    print(data)
    data0 = {}
    while not data0:
        data0 = fetch("https://api.hypixel.net/skyblock/auctions_ended")
        if not data0:
            print("Data not yet available; retrying...")
            time.sleep(1)
    with open('raw_auctions.json', 'w') as f:
        json.dump(data0, f, indent=4)

main()