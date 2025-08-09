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
import traceback
from datetime import datetime

DECODE_ERROR_LOG = 'decode_errors.log'

def log_decode_error(context, exc):
    """Append a JSON line describing a decode failure for later analysis."""
    record = {
        'ts': datetime.utcnow().isoformat(timespec='seconds') + 'Z',
        'error': type(exc).__name__,
        'message': str(exc),
        'auction_context': context,
        'traceback': ''.join(traceback.format_exception(exc)).strip()
    }
    # Trim very large base64 data in log to avoid ballooning file size
    b64 = context.get('item_bytes')
    if b64 and isinstance(b64, str) and len(b64) > 120:
        record['auction_context']['item_bytes'] = b64[:120] + '...'
    try:
        with open(DECODE_ERROR_LOG, 'a') as f:
            f.write(json.dumps(record) + '\n')
    except Exception as log_exc:
        print(f"Logging failure (ignored): {log_exc}")

def decode_item_bytes(b, context=None):
    """Decode base64 NBT item bytes into a Python structure.

    Returns None on failure and logs detailed info for later debugging.
    """
    try:
        raw = base64.b64decode(b)
        nbt_file = nbt.nbt.NBTFile(fileobj=io.BytesIO(raw))
        def unpack_nbt(tag):
            if isinstance(tag, TAG_List):
                return [unpack_nbt(i) for i in tag.tags]
            elif isinstance(tag, TAG_Compound):
                return {i.name: unpack_nbt(i) for i in tag.tags}
            else:
                return tag.value
        unpacked_nbt = unpack_nbt(nbt_file)
        # Convert bytearray to string for top-level keys
        for key, value in list(unpacked_nbt.items()):
            if isinstance(value, (bytearray, bytes)):
                try:
                    unpacked_nbt[key] = value.decode('utf-8', errors='replace')
                except Exception:
                    pass
        return unpacked_nbt
    except Exception as e:
        ctx = context.copy() if isinstance(context, dict) else {}
        ctx['item_bytes'] = b
        log_decode_error(ctx, e)
        return None

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

    decoded = []
    failures = 0
    for x in auctions:
        ctx = {
            'auction_id': x.get('auction_id') or x.get('uuid') or x.get('id'),
            'price': x.get('price'),
            'timestamp': x.get('timestamp'),
        }
        detail = decode_item_bytes(x.get('item_bytes'), context=ctx)
        if detail is None:
            failures += 1
            continue
        decoded.append({**x, 'detail': detail})
    if failures:
        print(f"Warning: {failures} item(s) failed to decode (see {DECODE_ERROR_LOG}).")
    auctions = decoded
    try:
        with open('auctions.json', 'w') as f:
            json.dump(auctions, f, indent=4)
    except Exception as e:
        print("Error: Failed to write auctions.json: ", e)

    # Filter out any entries where expected structure is missing
    filtered = []
    missing_detail = 0
    for x in auctions:
        try:
            filtered.append({**x, 'detail': x['detail']['i'][0]})
        except Exception as e:
            missing_detail += 1
            log_decode_error({'stage': 'extract_i0', 'auction_id': x.get('auction_id') or x.get('uuid'), 'reason': 'detail.i[0] missing'}, e)
    if missing_detail:
        print(f"Warning: {missing_detail} decoded item(s) lacked expected structure (logged).")
    auctions = filtered

    auctions = [{
        'timestamp': x['timestamp'],
        'unitprice': x['price'] / x['detail']['Count'],
        'count': x['detail']['Count'],
        'ench1': x['detail']['tag'].get('ench'),
        'ench2': x['detail']['tag']['ExtraAttributes'].get('enchantments'),
        'recomb': x['detail']['tag']['ExtraAttributes'].get('rarity_upgrades'),
        'color': str(x['detail']['tag']['display'].get('color')) if x['detail']['tag']['display'].get('color') is not None else None,
        'attributes': x['detail']['tag']['ExtraAttributes'].get('attributes'),
        'gems': ({k: v['quality'] for k, v in x['detail']['tag']['ExtraAttributes'].get('gems', {}).items() if k != 'unlocked_slots' and isinstance(v, dict)}
                 if x['detail']['tag']['ExtraAttributes'].get('gems') and any(k != 'unlocked_slots' and isinstance(v, dict) and 'quality' in v for k, v in x['detail']['tag']['ExtraAttributes']['gems'].items()) else None),
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
                   if r in x['lore']
               ]) +
               '+' + ','.join([
                   r for r in options['reforges'] if r in x['name']
               ]) +
               ('+rarity_upgrade' if x['recomb'] else '') +
               (('+color=' + str(x['color'])) if x.get('color') is not None else '') +
               (('+' + ','.join([
                   f"{a}={x['attributes'][a]}" for a in x['attributes']
               ])) if x.get('attributes') else '')
    } for x in auctions]

    # print(auctions)-
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
    print("Done! (decode errors, if any, recorded in decode_errors.log)")