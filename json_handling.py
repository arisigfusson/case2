import os
import glob
import json
import pandas as pd


def load_json_objects_from_folder(folder_path):
    """
    Load JSON objects from all .json files in the given folder.
    Each file can have one or more JSON objects (e.g. one per line).
    """
    all_data = []
    # Look for all .json files in the folder
    for file_path in glob.glob(os.path.join(folder_path, "*.json")):
        with open(file_path, "r") as f:
            # Read the file content and split by lines
            lines = f.read().strip().splitlines()
            for line in lines:
                if line.strip():
                    try:
                        obj = json.loads(line)
                        all_data.append(obj)
                    except Exception as e:
                        print(f"Error parsing JSON in file {file_path}: {e}")
    return all_data


def extract_dataframes(data):
    """
    Given a list of JSON objects (with the structure shown),
    extract several DataFrames based on nested properties.
    This version removes from the orders DataFrame any columns that
    come from nested structures that are extracted separately.
    """
    # Extract orders from the '_source' key in each JSON object
    orders = [d["_source"] for d in data if "_source" in d]

    # Remove meta_click_info from search_data if present
    for order in orders:
        if "search_data" in order and isinstance(order["search_data"], dict):
            order["search_data"].pop("meta_click_info", None)

    # 1. Main orders DataFrame (flattened top-level order data)
    df_orders = pd.json_normalize(orders)

    # Define nested keys to remove. Any column that comes from one of these
    # nested dictionaries will have a column name starting with the key + '.'
    nested_keys = ["payment_details", "orderlines", "shown_addons", "search_data"]
    cols_to_drop = [
        col
        for col in df_orders.columns
        if any(col.startswith(f"{key}.") for key in nested_keys)
    ]

    # Drop the unwanted columns from the orders DataFrame
    df_orders = df_orders.drop(columns=cols_to_drop)

    # 2. Payment details DataFrame (nested 'payment_details' dict)
    payment_records = []
    for order in orders:
        record = {"order_id": order.get("order_id")}
        if isinstance(order.get("payment_details"), dict):
            record.update(order["payment_details"])
        payment_records.append(record)
    df_payment_details = pd.DataFrame(payment_records)

    # 3. Orderlines DataFrame: each key in 'orderlines' becomes a row.
    orderlines_records = []
    for order in orders:
        order_id = order.get("order_id")
        orderlines = order.get("orderlines", {})
        if isinstance(orderlines, dict):
            for key, val in orderlines.items():
                rec = {"order_id": order_id, "orderline_type": key}
                rec.update(val)
                orderlines_records.append(rec)
    df_orderlines = pd.DataFrame(orderlines_records)

    # 4. Shown addons DataFrame: capture boolean flags from 'shown_addons'
    shown_addons_records = []
    for order in orders:
        order_id = order.get("order_id")
        addons = order.get("shown_addons", {})
        if isinstance(addons, dict):
            rec = {"order_id": order_id}
            rec.update(addons)
            shown_addons_records.append(rec)
    df_shown_addons = pd.DataFrame(shown_addons_records)

    # 5. Search data DataFrame: extract non-nested parts from 'search_data'
    search_data_records = []
    for order in orders:
        search_data = order.get("search_data", {})
        if isinstance(search_data, dict):
            rec = search_data.copy()
            rec["parent_order_id"] = order.get("order_id")
            # Remove keys that contain nested dictionaries
            rec.pop("orders", None)
            rec.pop("search_parameters", None)
            search_data_records.append(rec)
    df_search_data = pd.DataFrame(search_data_records)

    # 6. Search orders: nested inside 'search_data.orders'
    search_orders_records = []
    for order in orders:
        search_data = order.get("search_data", {})
        search_orders = search_data.get("orders", {})
        if isinstance(search_orders, dict):
            for key, val in search_orders.items():
                rec = {
                    "parent_order_id": order.get("order_id"),
                    "search_order_key": key,
                }
                rec.update(val)
                search_orders_records.append(rec)
    df_search_orders = pd.DataFrame(search_orders_records)

    # 7. Search parameters: nested under 'search_data.search_parameters'
    search_parameters_records = []
    for order in orders:
        search_data = order.get("search_data", {})
        params = search_data.get("search_parameters", {})
        if params:
            rec = params.copy()
            rec["parent_order_id"] = order.get("order_id")
            search_parameters_records.append(rec)
    df_search_parameters = pd.DataFrame(search_parameters_records)

    return {
        "orders": df_orders,
        "payment_details": df_payment_details,
        "orderlines": df_orderlines,
        "shown_addons": df_shown_addons,
        "search_data": df_search_data,
        "search_orders": df_search_orders,
        "search_parameters": df_search_parameters,
    }
