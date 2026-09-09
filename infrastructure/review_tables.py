from pathlib import Path

for file in [
    Path("data/olist_orders_dataset.csv"),
    Path("data/product_category_name_translation.csv"),
]:
    print(f"\nFILE: {file.name}")
    with open(file, "r", encoding="utf-8-sig") as f:
        for _ in range(3):
            print(repr(f.readline()))