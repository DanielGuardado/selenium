from selenium import webdriver
from os.path import abspath
from os import path
import os.path
import glob
from time import sleep
import pyodbc
import pandas as pd
import random
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import config

# I havent used this
# this file was used to look for that month of march that was outdated
# nonetheless theres still some stuff to use here from
# the pandas manipulation that was done.


def pyo():

    conn = pyodbc.connect(config.connection)
    cursor = conn.cursor()
    # cursor.execute("SELECT PO FROM PurchaseOrdersStatus WHERE downloaded IS NULL")
    cursor.execute(
        "SELECT * FROM PurchaseOrdersStatus WHERE downloaded IS NULL AND PurchaseOrderDate BETWEEN '2021-03-01T00:00:00Z' AND '2021-04-01T00:00:00Z' ORDER BY PurchaseOrderDate"
    )
    li = []
    for row in cursor.fetchall():
        li.append(row[0])
    return li


def pyo2(po):

    conn = pyodbc.connect(config.connection)
    cursor = conn.cursor()
    cursor.execute("update PurchaseOrdersStatus SET downloaded = 1 WHERE PO = ?", po)
    conn.commit()


def testing():
    po_stack = pyo()
    options = webdriver.ChromeOptions()

    options.add_argument(
        "user-data-dir=C:\\Users\\Dan\\AppData\\Local\\Google\\Chrome\\User Data"
    )
    web = webdriver.Chrome(
        executable_path="C:\\Windows\\chromedriver.exe", options=options
    )

    while po_stack:
        try:
            current_po = po_stack.pop()

            web.get(
                f"https://vendorcentral.amazon.com/hz/vendor/members/inv-mgmt/invoice-po-search?searchByNumberToken={current_po}"
            )

            export_all = web.find_element_by_link_text("Export All")
            export_all.click()
            sleep(3)
            parse_po_file(current_po)
            sleep(3)

            buttons = web.find_elements_by_css_selector(".a-button-small")
            for press in buttons:
                window = web.window_handles[0]
                web.switch_to_window(window)

                sleep(3)
                press.click()

                sleep(3)

                inv_link = web.find_element_by_link_text("View invoice details").click()

                sleep(3)

                window1 = web.window_handles[1]
                web.switch_to_window(window1)

                sleep(3)

                export_all = web.find_element_by_xpath(
                    '//button[contains(text(), "Export All")]'
                ).click()
                sleep(3)
                parse_item2_file()
                sleep(3)

                web.close()
                window = web.window_handles[0]
                web.switch_to_window(window)

            pyo2(current_po)
        except:
            web.quit()
            sleep(30)
            testing()
        # print(current_po)


def parse_po_file(po):
    folder_path = r"C:\Users\Dan\Downloads\automation"
    file_type = "\*csv"
    files = glob.glob(folder_path + file_type)
    max_file = max(files, key=os.path.getctime)

    try:
        df = pd.read_csv(max_file)
        df = df.rename(columns={"Unnamed: 11": "PO Number"})
        df["Invoice Amount"] = df["Invoice Amount"].replace(
            {"\$": "", ",": ""}, regex=True
        )
        df["Invoice Amount"] = df["Invoice Amount"].astype(float)
        df["PO Number"] = df["PO Number"].fillna(po)
        # print(df)

        conn = pyodbc.connect(config.connection)
        cursor = conn.cursor()
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO Invoices (marketplace, invoice_date, due_date, invoice_status, source, actual_paid_amount,payee, invoice_creation_date, invoice_number,invoice_amount,any_deductions, po_number) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                row["Marketplace"],
                row["Invoice Date"],
                row["Due Date"],
                row["Invoice Status"],
                row["Source"],
                row["Actual Paid Amount"],
                row["Payee"],
                row["Invoice Creation Date"],
                row["Invoice #"],
                row["Invoice Amount"],
                row["Any Deductions"],
                row["PO Number"],
            )
            conn.commit()
    except:
        df.to_csv(
            f"C:\\Users\\Dan\\Downloads\\automation\\invoice\\errorInv_{random.randint(0,100000)}.csv",
            index=False,
            header=True,
            quotechar='"',
        )


def parse_item_file():
    folder_path = r"C:\Users\Dan\Downloads\automation"
    file_type = "\*csv"
    files = glob.glob(folder_path + file_type)
    max_file = max(files, key=os.path.getctime)
    df = pd.read_csv(
        max_file,
        skiprows=3,
        names=[
            "filler",
            "po_number",
            "external_id",
            "title",
            "asin",
            "SKU",
            "freight_term",
            "qty",
            "unit_cost",
            "amount",
            "shortage_qty",
            "amount_shortage",
            "last_received_date",
            "asin_received",
            "quantity_received",
            "unit_cost_received",
            "amount_received",
        ],
    )
    try:
        df = df.shift(periods=1, axis=1)
        # print(df.columns)
        # print(df)
        cols = [
            "unit_cost",
            "amount",
            "amount_shortage",
            "unit_cost_received",
            "amount_received",
        ]
        cols2 = [
            "last_received_date",
            "asin_received",
            "quantity_received",
            "unit_cost_received",
            "amount_received",
        ]
        # df = df.rename(columns={"Unnamed: 11": "PO Number"})
        # df["unit_cost","amount","unit_cost_received","amount_received"] = df["unit_cost","amount","unit_cost_received","amount_received"].str.replace("$", "")
        # df["unit_cost","amount","unit_cost_received","amount_received"] = df["unit_cost","amount","unit_cost_received","amount_received"].astype(float)
        df[cols] = df[cols].replace({"\$": "", ",": ""}, regex=True)
        df[cols2] = df[cols2].fillna(0)
        df[cols] = df[cols].astype(float)
        # print(df)
        # df["Unit Cost"] = df["Unit Cost"].str.replace("$", "")
        # df[8] = df[8].str.replace("$", "")
        # df["Unit cost"] = df["Unit cost"].astype(float)
        # df["Unit Cost"] = df["Unit Cost"].astype(float)
        # df[8] = df[8].astype(float)
        # print(df)
        # df["PO Number"] = df["PO Number"].fillna(po)

        conn = pyodbc.connect(config.connection)
        cursor = conn.cursor()
        for index, row in df.iterrows():
            # last_received_date
            # asin_received
            # quantity_received
            # unit_cost_received
            cursor.execute(
                "INSERT INTO Invoice_Details (po_number, external_id, title, asin, SKU, freight_term,qty, unit_cost, amount,shortage_qty,amount_shortage, last_received_date,asin_received,quantity_received,unit_cost_received,amount_received, processed ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row["po_number"],
                row["external_id"],
                row["title"],
                row["asin"],
                row["SKU"],
                row["freight_term"],
                row["qty"],
                row["unit_cost"],
                row["amount"],
                row["shortage_qty"],
                row["amount_shortage"],
                row.get("last_received_date", 0),
                row.get("asin_received", "Not Availible"),
                row.get("quantity_received", 0),
                row.get("unit_cost_received", 0),
                row.get("amount_received", 0),
                1,
            )
            conn.commit()
    except:
        df.to_csv(
            f"C:\\Users\\Dan\\Downloads\\automation\\invoice\\error_{random.randint(0,100000)}.csv",
            index=False,
            header=True,
            quotechar='"',
        )


def parse_item2_file():
    folder_path = r"C:\Users\Dan\Downloads\automation"
    file_type = "\*csv"
    files = glob.glob(folder_path + file_type)
    max_file = max(files, key=os.path.getctime)
    try:
        df = pd.read_csv(
            max_file,
            skiprows=3,
            names=[
                "filler",
                "po_number",
                "external_id",
                "title",
                "asin",
                "SKU",
                "freight_term",
                "qty",
                "unit_cost",
                "amount",
                "shortage_qty",
                "amount_shortage",
                "last_received_date",
                "asin_received",
                "quantity_received",
                "unit_cost_received",
                "amount_received",
                "filler2",
            ],
        )
        df = df.shift(periods=1, axis=1)
        # print(df.columns)
        # print(df)
        cols = [
            "unit_cost",
            "amount",
            "amount_shortage",
            "unit_cost_received",
            "amount_received",
        ]
        cols2 = [
            "last_received_date",
            "asin_received",
            "quantity_received",
            "unit_cost_received",
            "amount_received",
        ]
        # df = df.rename(columns={"Unnamed: 11": "PO Number"})
        # df["unit_cost","amount","unit_cost_received","amount_received"] = df["unit_cost","amount","unit_cost_received","amount_received"].str.replace("$", "")
        # df["unit_cost","amount","unit_cost_received","amount_received"] = df["unit_cost","amount","unit_cost_received","amount_received"].astype(float)
        df[cols] = df[cols].replace({"\$": "", ",": ""}, regex=True)
        df[cols2] = df[cols2].fillna(0)
        df[cols] = df[cols].astype(float)

        # m = df[["asin"]].eq(' Black"').any(axis=1)
        # print(m)
        # df["title"] = df["asin"]
        # print(df)
        # df[m] = df[m].shift(-1, axis=1)
        # hij = ["asin"]
        x = df.asin == ' Black"'
        df.loc[x, ["asin"]] = df.loc[x, ["asin"]].apply(lambda x: x.shift(-3), axis=1)
        shifted_df = df.apply(lambda x: pd.Series(x.dropna().values), axis=1).fillna("")
        shifted_df.columns = [
            "po_number",
            "external_id",
            "title",
            "asin",
            "SKU",
            "freight_term",
            "qty",
            "unit_cost",
            "amount",
            "shortage_qty",
            "amount_shortage",
            "last_received_date",
            "asin_received",
            "quantity_received",
            "unit_cost_received",
            "amount_received",
        ]
        # print(df)
        shifted_df[cols] = shifted_df[cols].replace({"\$": "", ",": ""}, regex=True)
        shifted_df[cols2] = shifted_df[cols2].fillna(0)
        print(shifted_df)
        shifted_df[cols] = shifted_df[cols].astype(float)
        shifted_df[["quantity_received"]] = shifted_df[["quantity_received"]].astype(
            int
        )
        # print(shifted_df)
        # df.loc[x, ["filler2"]] = df.loc[x, ["filler2"]].shift(-1, axis=1)
        # df["amount_received"]["1"] = 10
        # print(df)
        # print(df)
        # df["Unit Cost"] = df["Unit Cost"].str.replace("$", "")
        # df[8] = df[8].str.replace("$", "")
        # df["Unit cost"] = df["Unit cost"].astype(float)
        # df["Unit Cost"] = df["Unit Cost"].astype(float)
        # df[8] = df[8].astype(float)
        # print(df)
        # df["PO Number"] = df["PO Number"].fillna(po)

        conn = pyodbc.connect(config.connection)
        cursor = conn.cursor()
        for index, row in df.iterrows():
            # last_received_date
            # asin_received
            # quantity_received
            # unit_cost_received
            cursor.execute(
                "INSERT INTO Invoice_Details (po_number, external_id, title, asin, SKU, freight_term,qty, unit_cost, amount,shortage_qty,amount_shortage, last_received_date,asin_received,quantity_received,unit_cost_received,amount_received, processed ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row["po_number"],
                row["external_id"],
                row["title"],
                row["asin"],
                row["SKU"],
                row["freight_term"],
                row["qty"],
                row["unit_cost"],
                row["amount"],
                row["shortage_qty"],
                row["amount_shortage"],
                row.get("last_received_date", 0),
                row.get("asin_received", "Not Availible"),
                row.get("quantity_received", 0),
                row.get("unit_cost_received", 0),
                row.get("amount_received", 0),
                1,
            )
            conn.commit()
    except:
        shifted_df.to_csv(
            f"C:\\Users\\Dan\\Downloads\\automation\\invoice\\error_{random.randint(0,1000000)}.csv",
            index=False,
            header=True,
            quotechar='"',
        )


# testing()
# parse_item_file()
try:
    testing()
except:
    sleep(30)
    testing()
# parse_item2_file()