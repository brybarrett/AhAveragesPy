import io
import base64
import json
import nbt
from nbt.nbt import TAG_List, TAG_Compound
import pyperclip

# WIP

def decode_item_bytes(b):
    nbt_file = nbt.nbt.NBTFile(fileobj=io.BytesIO(base64.b64decode(b)))
    def unpack_nbt(tag):
        if isinstance(tag, TAG_List):
            return [unpack_nbt(i) for i in tag.tags]
        elif isinstance(tag, TAG_Compound):
            return {i.name: unpack_nbt(i) for i in tag.tags}
        else:
            return tag.value
    return unpack_nbt(nbt_file)

def create_item_key(raw_item):
    item = raw_item['detail']
    key = {
        'name': item['tag']['display']['Name'],
        'id': item['tag']['ExtraAttributes']['id'],
        'count': item['Count'],
        'lore': item['tag']['display'].get('Lore', []),
        'ench1': item['tag'].get('ench', []),
        'ench2': item['tag']['ExtraAttributes'].get('enchantments', []),
        'recomb': item['tag']['ExtraAttributes'].get('rarity_upgrades', [])
    }
    return key

if __name__ == "__main__":
    item_bytes = input("Enter item bytes: ")
    decoded = decode_item_bytes(item_bytes)
    decoded_str = json.dumps(decoded, indent=4)
    print(decoded_str)
    try:
        pyperclip.copy(decoded_str)
        print("Copied decoded string to clipboard.")
    except ImportError:
        print("pyperclip is not installed. Skipping clipboard copy.")