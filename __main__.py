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
    b64 = context.get('item_bytes') if isinstance(context, dict) else None
    if b64 and isinstance(b64, str) and len(b64) > 120:
        # truncate large base64 to keep log concise
        record['auction_context']['item_bytes'] = b64[:120] + '...'
    try:
        with open(DECODE_ERROR_LOG, 'a') as f:
            f.write(json.dumps(record) + '\n')
    except Exception as log_exc:
        print(f"Logging failure (ignored): {log_exc}")

def decode_item_bytes(b, context=None):
    """Decode base64 NBT item bytes into a Python structure; returns None if fails."""
    try:
        raw = base64.b64decode(b)
        nbt_file = nbt.nbt.NBTFile(fileobj=io.BytesIO(raw))
        def unpack(tag):
            if isinstance(tag, TAG_List):
                return [unpack(t) for t in tag.tags]
            if isinstance(tag, TAG_Compound):
                return {t.name: unpack(t) for t in tag.tags}
            return tag.value
        return unpack(nbt_file)
    except Exception as e:
        ctx = context.copy() if isinstance(context, dict) else {'note': 'no context'}
        ctx['item_bytes'] = b[:120] + '...' if isinstance(b, str) and len(b) > 120 else b
        log_decode_error(ctx, e)
        return None

def main():
    print("Starting...")
    # 1. Load config
    with open('options.json') as f:
        options = json.load(f)

    def json_default(o):
        if isinstance(o, (bytes, bytearray)):
            return base64.b64encode(o).decode('ascii')
        return str(o)

    # 2. Fetch auctions
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
    with open('raw_auctions.json', 'w') as f:
        json.dump(data0, f, indent=4)
    auctions = data0.get('auctions', [])
    auctions = [x for x in auctions if x.get('bin') and x.get('buyer')]

    # 3. Decode NBT
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
        decoded.append({**x, 'detail': detail, 'full_nbt': detail})
    if failures:
        print(f"Warning: {failures} item(s) failed to decode (see {DECODE_ERROR_LOG}).")
    auctions = decoded
    try:
        with open('auctions.json', 'w') as f:
            json.dump(auctions, f, indent=4, default=json_default)
    except Exception as e:
        print("Error: Failed to write auctions.json: ", e)

    # 4. Extract detail.i[0]
    filtered = []
    missing_detail = 0
    for x in auctions:
        try:
            filtered.append({**x, 'detail': x['detail']['i'][0], 'full_nbt': x.get('full_nbt')})
        except Exception as e:
            missing_detail += 1
            log_decode_error({'stage': 'extract_i0', 'auction_id': x.get('auction_id') or x.get('uuid'), 'reason': 'detail.i[0] missing'}, e)
    if missing_detail:
        print(f"Warning: {missing_detail} decoded item(s) lacked expected structure (logged).")
    auctions = filtered

    # 5. Build processed list
    processed = []
    for x in auctions:
        try:
            detail = x['detail']
            ea = detail['tag']['ExtraAttributes']
            display = detail['tag'].get('display', {})
            rec = {
                'timestamp': x['timestamp'],
                'price': x['price'],
                'unitprice': x['price'] / detail['Count'] if detail.get('Count') else None,
                'count': detail.get('Count'),
                'ench1': detail['tag'].get('ench'),
                'ench2': ea.get('enchantments'),
                'recomb': ea.get('rarity_upgrades'),
                'color': str(display.get('color')) if display.get('color') is not None else None,
                'attributes': ea.get('attributes'),
                'gems': ({k: v['quality'] for k, v in ea.get('gems', {}).items() if k != 'unlocked_slots' and isinstance(v, dict)}
                         if ea.get('gems') and any(k != 'unlocked_slots' and isinstance(v, dict) and 'quality' in v for k, v in ea.get('gems', {}).items()) else None),
                'lore': [l.replace('§.', '') for l in display.get('Lore', [])],
                'name': display.get('Name'),
                'id': ea.get('id'),
                'item_bytes': x.get('item_bytes'),
                'full_nbt': x.get('full_nbt')
            }
            processed.append(rec)
        except Exception as e:
            log_decode_error({'stage': 'process_record'}, e)
    auctions = processed

    # 6. Create composite + base keys
    for a in auctions:
        parts = []
        if a.get('ench2'):
            ench_part = ','.join([
                f"{e}={a['ench2'][e]}" for e in options.get('relevant_enchants', {})
                if e in a['ench2'] and a['ench2'][e] in options['relevant_enchants'][e]
            ])
            if ench_part:
                parts.append(ench_part)
        lore_rarities = [r for r in options.get('rarities', []) if r in a.get('lore', [])]
        if lore_rarities:
            parts.append(','.join(lore_rarities))
        reforges_present = [r for r in options.get('reforges', []) if a.get('name') and r in a['name']]
        if reforges_present:
            parts.append(','.join(reforges_present))
        if a.get('recomb'):
            parts.append('rarity_upgrade')
        if a.get('color') is not None:
            parts.append(f"color={a['color']}")
        if a.get('attributes'):
            attrs = ','.join([f"{k}={a['attributes'][k]}" for k in a['attributes']])
            if attrs:
                parts.append(attrs)
        a['key'] = a.get('id', 'UNKNOWN') + '.' + '+'.join(parts)
        a['base_key'] = a.get('id')

    # 7. Dump processed JSON snapshots
    try:
        with open('auctions2.json', 'w') as f:
            json.dump(auctions, f, indent=4, default=json_default)
    except Exception as e:
        print("Error: Failed to write auctions2.json", e)
    auctions3 = [{k: x[k] for k in ('timestamp','key','unitprice')} for x in auctions]
    with open('auctions3.json', 'w') as f:
        json.dump(auctions3, f, indent=4, default=json_default)

    # 8. Insert legacy DB
    sql = "INSERT INTO prices (timestamp, itemkey, price) VALUES (?, ?, ?)"
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS prices (timestamp INTEGER, itemkey TEXT, price REAL)")
    for auction in auctions3:
        cursor.execute(sql, (auction['timestamp'], auction['key'], auction['unitprice']))
    conn.commit(); cursor.close(); conn.close()

    # 9. Insert new detailed DB
    conn2 = sqlite3.connect('database2.db')
    c2 = conn2.cursor()
    c2.execute("""
        CREATE TABLE IF NOT EXISTS pricesV2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            itemkey TEXT,
            base_key TEXT,
            unitprice REAL,
            count INTEGER,
            recomb INTEGER,
            color TEXT,
            name TEXT,
            raw_item_bytes TEXT,
            full_nbt_json TEXT,
            ench TEXT -- JSON string of full enchantments dict (nullable)
        )
    """)
    # Ensure ench column exists (older DBs created before ench was added)
    info = c2.execute("PRAGMA table_info(pricesV2)").fetchall()
    existing_cols = {row[1] for row in info}
    if 'ench' not in existing_cols:
        try:
            c2.execute("ALTER TABLE pricesV2 ADD COLUMN ench TEXT")
            print("Migrated: added ench column to pricesV2")
        except Exception as e:
            log_decode_error({'stage': 'migrate_add_ench_column', 'note': 'ALTER TABLE failed'}, e)
    c2.execute("CREATE TABLE IF NOT EXISTS item_enchants (price_id INTEGER, enchant TEXT, level INTEGER)")
    c2.execute("CREATE TABLE IF NOT EXISTS item_attributes (price_id INTEGER, attribute TEXT, value INTEGER)")
    c2.execute("CREATE TABLE IF NOT EXISTS item_rarities (price_id INTEGER, rarity TEXT)")
    c2.execute("CREATE TABLE IF NOT EXISTS item_reforges (price_id INTEGER, reforge TEXT)")
    c2.execute("CREATE TABLE IF NOT EXISTS item_gems (price_id INTEGER, gem TEXT, quality INTEGER)")
    c2.execute("CREATE INDEX IF NOT EXISTS idx_pricesV2_itemkey ON pricesV2(itemkey)")
    c2.execute("CREATE INDEX IF NOT EXISTS idx_pricesV2_timestamp ON pricesV2(timestamp)")
    for a in auctions:
        full_nbt_json = json.dumps(a.get('full_nbt'), ensure_ascii=False)
        ench_json = json.dumps(a.get('ench2'), ensure_ascii=False) if a.get('ench2') else None
        c2.execute(
            "INSERT INTO pricesV2 (timestamp, itemkey, base_key, unitprice, count, recomb, color, name, raw_item_bytes, full_nbt_json, ench) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (a['timestamp'], a.get('key'), a.get('base_key'), a.get('unitprice'), a.get('count'), 1 if a.get('recomb') else 0, a.get('color'), a.get('name'), a.get('item_bytes'), full_nbt_json, ench_json)
        )
        price_id = c2.lastrowid
        if a.get('ench2'):
            for ench, lvl in a['ench2'].items():
                try:
                    c2.execute("INSERT INTO item_enchants (price_id, enchant, level) VALUES (?, ?, ?)", (price_id, ench, lvl))
                except Exception as e:
                    log_decode_error({'stage': 'insert_enchant', 'ench': ench, 'lvl': lvl, 'price_id': price_id}, e)
        if a.get('attributes'):
            for attr, val in a['attributes'].items():
                try:
                    c2.execute("INSERT INTO item_attributes (price_id, attribute, value) VALUES (?, ?, ?)", (price_id, attr, val))
                except Exception as e:
                    log_decode_error({'stage': 'insert_attribute', 'attr': attr, 'val': val, 'price_id': price_id}, e)
        if a.get('gems'):
            for gem, quality in a['gems'].items():
                try:
                    c2.execute("INSERT INTO item_gems (price_id, gem, quality) VALUES (?, ?, ?)", (price_id, gem, quality))
                except Exception as e:
                    log_decode_error({'stage': 'insert_gem', 'gem': gem, 'quality': quality, 'price_id': price_id}, e)
        if a.get('lore'):
            for r in (options.get('rarities') or []):
                if r in a['lore']:
                    try:
                        c2.execute("INSERT INTO item_rarities (price_id, rarity) VALUES (?, ?)", (price_id, r))
                    except Exception as e:
                        log_decode_error({'stage': 'insert_rarity', 'rarity': r, 'price_id': price_id}, e)
        if a.get('name') and options.get('reforges'):
            for reforge in options['reforges']:
                if reforge in a['name']:
                    try:
                        c2.execute("INSERT INTO item_reforges (price_id, reforge) VALUES (?, ?)", (price_id, reforge))
                    except Exception as e:
                        log_decode_error({'stage': 'insert_reforge', 'reforge': reforge, 'price_id': price_id}, e)
    conn2.commit(); c2.close(); conn2.close()

    print(f"Completed processing {len(auctions)} auctions (V2 records inserted).")

if __name__ == "__main__":
    try:
        main()
        print("Done! (decode errors, if any, recorded in decode_errors.log)")
    except Exception as e:
        print("Fatal error in main():", e)