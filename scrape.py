import requests
import csv
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_all_locations(session):
    url = "https://auction.bidfta.io/api/location/getAllLocations"
    response = session.get(url)
    response.raise_for_status()
    locations = response.json()
    location_dict = {loc["id"]: loc for loc in locations}
    return location_dict

def get_auctions(session, location_ids, page_id=1):
    location_str = ",".join(str(loc) for loc in location_ids)
    url = f"https://auction.bidfta.io/api/auction/getAuctions?pageId={page_id}&categories=Categories+-+All&pastAuction=false&selectedLocationIds={location_str}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()

def get_items_by_page(session, auction_id, page_id=1):
    url = f"https://auction.bidfta.io/api/item/getItemsByAuctionId/{auction_id}?&pageId={page_id}&auctionId={auction_id}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()

def get_all_items_for_auction(session, auction_id):
    """
    Fetch all items for a given auction by iterating page by page until no more items are returned.
    This approach ensures correctness regardless of itemCount or items per page.
    """
    all_items = []
    page = 1
    while True:
        items = get_items_by_page(session, auction_id, page_id=page)
        if not items:
            break
        all_items.extend(items)
        page += 1
    return all_items

def get_auction_pickup_dates(session, location_id):
    url = f"https://auction.bidfta.io/api/auction/getAuctionPickupDate?categories=Categories%20-%20All&locationIds={location_id}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()

def fetch_auction_data(session, auction, location_data, pickup_dates_cache):
    """
    Fetch all items for a single auction sequentially (page-by-page).
    Return a list of item rows for that auction.
    """
    auction_id = auction["id"]
    loc_id = auction.get("locationId")

    # Get pickup dates from cache or load if not present
    if loc_id not in pickup_dates_cache:
        pickup_dates_cache[loc_id] = get_auction_pickup_dates(session, loc_id)
    pickup_dates = pickup_dates_cache[loc_id]

    # Location details
    loc_info = location_data.get(loc_id, {})
    loc_nickname = loc_info.get("nickName", "")
    loc_address = loc_info.get("address", "")
    loc_city = loc_info.get("city", "")
    loc_state = loc_info.get("state", "")
    loc_zip = loc_info.get("zip", "")

    # Auction fields
    auction_number = auction.get("auctionNumber", "")
    auction_title = auction.get("title", "")
    auction_category = auction.get("category", "")
    auction_start = auction.get("utcStartDateTime", "")
    auction_end = auction.get("utcEndDateTime", "")

    # Fetch all items for this auction
    all_items = get_all_items_for_auction(session, auction_id)

    rows = []
    for item in all_items:
        current_bid = item.get("currentBid", 0.0)
        msrp = item.get("msrp", 0.0)
        ratio = current_bid / msrp if msrp > 0 else 0
        item_id = item.get("id")
        item_url = f"https://www.bidfta.com/{auction_id}/item-detail/{item_id}"

        row = {
            "auction_id": auction_id,
            "auction_number": auction_number,
            "auction_title": auction_title,
            "auction_category": auction_category,
            "auction_start_datetime": auction_start,
            "auction_end_datetime": auction_end,
            "auction_location_id": loc_id,
            "auction_location_nickname": loc_nickname,
            "auction_location_address": loc_address,
            "auction_location_city": loc_city,
            "auction_location_state": loc_state,
            "auction_location_zip": loc_zip,
            "pickup_dates": "; ".join(pickup_dates),
            "item_id": item_id,
            "lot_code": item.get("lotCode", ""),
            "current_bid": current_bid,
            "msrp": msrp,
            "condition": item.get("condition", ""),
            "brand": item.get("brand", ""),
            "item_title": item.get("title", ""),
            "item_category1": item.get("category1", ""),
            "item_category2": item.get("category2", ""),
            "bid_count": item.get("bidsCount", 0),
            "ratio_bid_to_msrp": ratio,
            "item_url": item_url
        }
        rows.append(row)

    return rows

def main():
    location_ids = [637,4,345,515,2,520,24,581,25,21,374]
    # location_ids = [637,4,345,515]
    now = datetime.now()
    csv_filename = f"./data/auction_data_{now.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    session = requests.Session()

    print("Fetching all locations information...")
    location_data = get_all_locations(session)
    print(f"Loaded {len(location_data)} locations.\n")

    fieldnames = [
        "auction_id",
        "auction_number",
        "auction_title",
        "auction_category",
        "auction_start_datetime",
        "auction_end_datetime",
        "auction_location_id",
        "auction_location_nickname",
        "auction_location_address",
        "auction_location_city",
        "auction_location_state",
        "auction_location_zip",
        "pickup_dates",
        "item_id",
        "lot_code",
        "current_bid",
        "msrp",
        "condition",
        "brand",
        "item_title",
        "item_category1",
        "item_category2",
        "bid_count",
        "ratio_bid_to_msrp",
        "item_url"
    ]

    write_header = False
    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as f:
            pass
    except FileNotFoundError:
        write_header = True

    # Collect all auctions first
    print("Collecting all auctions...")
    all_auctions = []
    page = 1
    while True:
        auctions = get_auctions(session, location_ids, page_id=page)
        if not auctions:
            break
        all_auctions.extend(auctions)
        page += 1
    print(f"Found {len(all_auctions)} total auctions.\n")

    # Process all auctions in parallel
    pickup_dates_cache = {}
    lock = threading.Lock()
    total_items = 0

    print("Fetching items from all auctions in parallel...\n")
    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        def process_and_write(auction):
            rows = fetch_auction_data(session, auction, location_data, pickup_dates_cache)
            # Write rows to CSV
            with lock:
                for r in rows:
                    writer.writerow(r)
                return len(rows)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_and_write, a) for a in all_auctions]
            for future in as_completed(futures):
                item_count = future.result()
                total_items += item_count

    print(f"Data collection completed. {total_items} items processed total.\n")
    print("Run 'python bidfta_analyze.py' to perform analytics on the collected data.")

if __name__ == "__main__":
    main()
