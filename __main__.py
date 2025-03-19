import requests
import nbt
import io
import base64
import json
import sqlite3
import time
from nbt.nbt import TAG_List, TAG_Compound
import aiohttp
import asyncio

def decode_item_bytes(b):
    nbt_file = nbt.nbt.NBTFile(fileobj=io.BytesIO(base64.b64decode(b)))
    def unpack_nbt(tag):
        if isinstance(tag, TAG_List):
            return [unpack_nbt(i) for i in tag.tags]
        elif isinstance(tag, TAG_Compound):
            return {i.name: unpack_nbt(i) for i in tag.tags}
        else:
            return tag.value
    unpacked_nbt = unpack_nbt(nbt_file)
    # Convert bytearray to string
    for key, value in unpacked_nbt.items():
        if isinstance(value, bytearray):
            unpacked_nbt[key] = value.decode('utf-8')
    return unpacked_nbt

def main():
    print("Starting...")
    with open('options.json') as f:
        options = json.load(f)
    print("Getting auctions...")
    async def fetch_auctions():
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.hypixel.net/skyblock/auctions_ended") as response:
                try:
                    return await response.json()
                except Exception as e:
                    print("Error: Received invalid JSON", e)
                    return {}

    data0 = asyncio.run(fetch_auctions())
    print("Got auctions!")
    # testing raw data written to a file
    with open('raw_auctions.json', 'w') as f:
        json.dump(data0, f, indent=4)
    auctions = data0['auctions']
    auctions = [x for x in auctions if x['bin'] and x['buyer']]

    auctions = [{**x, 'detail': decode_item_bytes(x['item_bytes'])} for x in auctions]
    try:
        with open('auctions.json', 'w') as f:
            json.dump(auctions, f, indent=4)
    except Exception as e:
        print("Error: Failed to write auctions.json", e)

    auctions = [{**x, 'detail': x['detail']['i'][0]} for x in auctions]

    auctions = [{
        'timestamp': x['timestamp'],
        'unitprice': x['price'] / x['detail']['Count'],
        'count': x['detail']['Count'],
        'ench1': x['detail']['tag'].get('ench'),
        'ench2': x['detail']['tag']['ExtraAttributes'].get('enchantments'),
        'recomb': x['detail']['tag']['ExtraAttributes'].get('rarity_upgrades'),
        'color': x['detail']['tag']['display'].get('color'),
        'attributes': x['detail']['tag']['ExtraAttributes'].get('attributes'),
        'lore': [l.replace('§.', '') for l in x['detail']['tag']['display'].get('Lore', [])],
        'name': x['detail']['tag']['display'].get('Name'),
        'id': x['detail']['tag']['ExtraAttributes'].get('id')
    } for x in auctions]

    auctions = [{
        **x,
        'key': x['id'] + '.' +
        (','.join([
            f"{e}={x['ench2'][e]}" for e in options['relevant_enchants']
            if x.get('ench2') and e in x['ench2'] and x['ench2'][e] in options['relevant_enchants'][e]
        ]) if x.get('ench2') else ''
        ) +
        '+' + ','.join([
            r for r in options['rarities']
            if any(l for l in x['lore'] if r in l)
        ]) +
        '+' + ','.join([
            r for r in options['reforges'] if r in x['name']
        ]) +
        ('+rarity_upgrade' if x['recomb'] else '') +
        ('+color' if x['color'] else '') +
        ('+' + ','.join([
            f"{a}={x['attributes'][a]}" for a in x['attributes']
        ]) if x.get('attributes') else '')
    } for x in auctions]

    # print(auctions)
    try:
        with open('auctions2.json', 'w') as f:
            json.dump(auctions, f, indent=4)
    except Exception as e:
        print("Error: Failed to write auctions2.json", e)

    auctions3 = [{k: x[k] for k in 'timestamp,key,unitprice'.split(',')} for x in auctions]
    with open('auctions3.json', 'w') as f:
        json.dump(auctions3, f, indent=4)

    sql = "INSERT INTO prices (timestamp, itemkey, price) VALUES (?, ?, ?)"
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS prices (timestamp INTEGER, itemkey TEXT, price REAL)")
    for auction in auctions3:
        cursor.execute(sql, (auction['timestamp'], auction['key'], auction['unitprice']))
    conn.commit()
    cursor.close()
    conn.close()

def get_prices():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, itemkey, COUNT(DISTINCT timestamp) volume, ROUND(AVG(price), 2) averageprice FROM prices GROUP BY itemkey HAVING volume > 20")
    rows = cursor.fetchall()
    conn.close()
    return json.dumps({'numAverages': len(rows), 'averages': rows})

if __name__ == "__main__":
    main()
    print("Done!")