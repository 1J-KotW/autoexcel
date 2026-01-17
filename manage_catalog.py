#!/usr/bin/env python3
"""
Management script for the extended material catalog system.

Provides command-line interface for:
- Database initialization and migration
- Price imports from files
- Customer and vendor management
- Material management
"""

import argparse
import sys
from database_manager import db_manager
from price_importer import price_importer
from web_price_scraper import web_scraper
from datetime import date

def init_database():
    """Initialize database from schema"""
    print("Initializing database...")
    db_manager.initialize_database()
    print("Database initialized successfully")

def migrate_data():
    """Migrate data from JSON catalog to database"""
    print("Migrating data from JSON catalog...")
    db_manager.migrate_from_json()
    print("Migration completed")

def add_customer(name, preferred_source_type='invoice'):
    """Add a new customer"""
    customer_id = db_manager.add_customer(name, preferred_source_type)
    print(f"Customer '{name}' added with ID: {customer_id}")

def add_vendor(name, website_url=None):
    """Add a new vendor"""
    vendor_id = db_manager.add_vendor(name, website_url)
    print(f"Vendor '{name}' added with ID: {vendor_id}")

def add_material(name, unit, work_rate, category=None, vendor_id=None):
    """Add a new material"""
    material_id = db_manager.add_material(name, unit, work_rate, category, vendor_id)
    print(f"Material '{name}' added with ID: {material_id}")

def import_prices(file_path, customer_id=None, vendor_id=None, doc_date=None):
    """Import prices from Excel/CSV file"""
    print(f"Importing prices from {file_path}...")

    try:
        results = price_importer.import_from_file(
            file_path=file_path,
            customer_id=int(customer_id) if customer_id else None,
            vendor_id=int(vendor_id) if vendor_id else None,
            doc_date=doc_date
        )

        print("Import completed:")
        print(f"  Total rows: {results['total_rows']}")
        print(f"  Processed: {results['processed']}")
        print(f"  Errors: {results['errors']}")
        print(f"  Unmatched: {len(results['unmatched'])}")

        if results['unmatched']:
            print("\nUnmatched materials (need manual resolution):")
            for item in results['unmatched'][:5]:  # Show first 5
                print(f"  - {item['name']} ({item.get('unit', 'N/A')})")
            if len(results['unmatched']) > 5:
                print(f"  ... and {len(results['unmatched']) - 5} more")

    except Exception as e:
        print(f"Import failed: {e}")
        sys.exit(1)

def scrape_prices(vendor_id, material_ids=None):
    """Scrape prices from vendor website"""
    print(f"Scraping prices from vendor {vendor_id}...")

    try:
        results = web_scraper.scrape_vendor_prices(
            vendor_id=int(vendor_id),
            material_ids=material_ids.split(',') if material_ids else None
        )

        print("Scraping completed:")
        print(f"  Total materials: {results['total_materials']}")
        print(f"  Successful: {results['successful']}")
        print(f"  Failed: {results['failed']}")

        if results['errors']:
            print("\nErrors:")
            for error in results['errors'][:5]:
                print(f"  - {error}")

    except Exception as e:
        print(f"Scraping failed: {e}")
        sys.exit(1)

def list_customers():
    """List all customers"""
    customers = db_manager.get_customers()
    if not customers:
        print("No customers found")
        return

    print("Customers:")
    for customer in customers:
        print(f"  {customer['id']}: {customer['name']} (prefers {customer['preferred_price_source_type']})")

def list_vendors():
    """List all vendors"""
    vendors = db_manager.get_vendors()
    if not vendors:
        print("No vendors found")
        return

    print("Vendors:")
    for vendor in vendors:
        url = vendor.get('website_url', 'No website')
        print(f"  {vendor['id']}: {vendor['name']} ({url})")

def show_import_results(session_id):
    """Show results of an import session"""
    try:
        results = price_importer.get_import_results(int(session_id))
        print(f"Import session {session_id}:")
        print(f"  Unmatched items: {results['unmatched_count']}")

        if results['unmatched_items']:
            print("\nUnmatched materials:")
            for item in results['unmatched_items'][:10]:
                print(f"  - {item['raw_name']} ({item.get('raw_unit', 'N/A')}) - {item.get('raw_price', 'N/A')}")
    except Exception as e:
        print(f"Error getting results: {e}")

def main():
    parser = argparse.ArgumentParser(description="Material Catalog Management")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Database commands
    subparsers.add_parser('init-db', help='Initialize database')
    subparsers.add_parser('migrate', help='Migrate data from JSON to database')

    # Add entities
    add_customer_parser = subparsers.add_parser('add-customer', help='Add customer')
    add_customer_parser.add_argument('name', help='Customer name')
    add_customer_parser.add_argument('--source-type', default='invoice',
                                   choices=['invoice', 'website', 'manual'],
                                   help='Preferred price source type')

    add_vendor_parser = subparsers.add_parser('add-vendor', help='Add vendor')
    add_vendor_parser.add_argument('name', help='Vendor name')
    add_vendor_parser.add_argument('--website', help='Vendor website URL')

    add_material_parser = subparsers.add_parser('add-material', help='Add material')
    add_material_parser.add_argument('name', help='Material name')
    add_material_parser.add_argument('unit', help='Unit of measurement')
    add_material_parser.add_argument('work_rate', type=float, help='Work rate (labor cost)')
    add_material_parser.add_argument('--category', help='Material category')
    add_material_parser.add_argument('--vendor-id', type=int, help='Default vendor ID')

    # Import commands
    import_parser = subparsers.add_parser('import-prices', help='Import prices from file')
    import_parser.add_argument('file_path', help='Path to Excel/CSV file')
    import_parser.add_argument('--customer-id', type=int, help='Customer ID')
    import_parser.add_argument('--vendor-id', type=int, help='Vendor ID')
    import_parser.add_argument('--doc-date', help='Document date (YYYY-MM-DD)')

    scrape_parser = subparsers.add_parser('scrape-prices', help='Scrape prices from website')
    scrape_parser.add_argument('vendor_id', type=int, help='Vendor ID')
    scrape_parser.add_argument('--material-ids', help='Comma-separated material IDs')

    # List commands
    subparsers.add_parser('list-customers', help='List all customers')
    subparsers.add_parser('list-vendors', help='List all vendors')

    # Results
    results_parser = subparsers.add_parser('import-results', help='Show import results')
    results_parser.add_argument('session_id', type=int, help='Import session ID')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Execute commands
    try:
        if args.command == 'init-db':
            init_database()

        elif args.command == 'migrate':
            migrate_data()

        elif args.command == 'add-customer':
            add_customer(args.name, args.source_type)

        elif args.command == 'add-vendor':
            add_vendor(args.name, args.website)

        elif args.command == 'add-material':
            add_material(args.name, args.unit, args.work_rate, args.category, args.vendor_id)

        elif args.command == 'import-prices':
            import_prices(args.file_path, args.customer_id, args.vendor_id, args.doc_date)

        elif args.command == 'scrape-prices':
            scrape_prices(args.vendor_id, args.material_ids)

        elif args.command == 'list-customers':
            list_customers()

        elif args.command == 'list-vendors':
            list_vendors()

        elif args.command == 'import-results':
            show_import_results(args.session_id)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
