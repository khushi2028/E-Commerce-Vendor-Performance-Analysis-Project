import pandas as pd
import logging
import sqlite3
import os
from ingestion_db import db_ingest

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Remove existing handlers to avoid duplicate logs
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configure logging
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    '''This function merges different tables to get overall vendor summary and adds new columns in the existing data.'''
    query = '''
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases AS p
        JOIN purchase_prices AS pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM Sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesPrice,
        ss.TotalSalesDollars,
        ss.TotalSalesQuantity,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary AS ps
    LEFT JOIN SalesSummary AS ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary AS fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    '''

    df = pd.read_sql_query(query, conn)
    return df

def clean_data(df):
    '''This function cleans the vendor summary data.'''

    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating new columns
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    return df

if __name__ == '__main__':
    try:
        logging.info("Connecting to database...")
        conn = sqlite3.connect('inventory.db')

        logging.info("Creating Vendor Summary DataFrame...")
        summary_df = create_vendor_summary(conn)
        logging.info(f"Summary DF shape: {summary_df.shape}")
        logging.debug(summary_df.head().to_string())

        logging.info("Cleaning data...")
        clean_df = clean_data(summary_df)
        logging.info(f"Cleaned DF shape: {clean_df.shape}")
        logging.debug(clean_df.head().to_string())

        logging.info("Ingesting cleaned data into database...")
        db_ingest(clean_df, 'vendor_sales_summary', conn)
        logging.info("Ingestion complete. Script finished successfully.")

    except Exception as e:
        logging.error(f"Error occurred: {e}")
