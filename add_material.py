import json
import sys
import uuid

def load_catalog():
    try:
        with open('materials_catalog.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def save_catalog(catalog):
    with open('materials_catalog.json', 'w', encoding='utf-8') as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python add_material.py <name> <unit> <price> <labor_cost>")
        print("Example: python add_material.py \"Цемент М500\" кг 6.00 2.50")
        sys.exit(1)

    name = sys.argv[1]
    unit = sys.argv[2]
    try:
        price = float(sys.argv[3])
        labor_cost = float(sys.argv[4])
    except ValueError:
        print("Price and labor cost must be numbers")
        sys.exit(1)

    catalog = load_catalog()

    # Check if exists
    existing = [m for m in catalog if m['name'] == name and m['unit'] == unit]
    if existing:
        existing[0]['price'] = price
        existing[0]['labor_cost'] = labor_cost
        print(f"Updated existing material: {name} ({unit})")
    else:
        new_id = str(uuid.uuid4())
        catalog.append({
            'id': new_id,
            'name': name,
            'unit': unit,
            'price': price,
            'labor_cost': labor_cost
        })
        print(f"Added new material: {name} ({unit})")

    save_catalog(catalog)
