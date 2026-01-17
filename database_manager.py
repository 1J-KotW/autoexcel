import sqlite3
import json
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
import os

class DatabaseManager:
    def __init__(self, db_path: str = "materials.db"):
        self.db_path = db_path
        self.connection = None

    def connect(self):
        """Establish database connection"""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        return self.connection

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def initialize_database(self):
        """Create all tables from schema file"""
        with open('database_schema.sql', 'r', encoding='utf-8') as f:
            schema = f.read()

        with self.connect() as conn:
            conn.executescript(schema)
            conn.commit()

    def migrate_from_json(self):
        """Migrate existing JSON catalog to database"""
        if not os.path.exists('materials_catalog.json'):
            return

        with open('materials_catalog.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        with self.connect() as conn:
            for item in json_data:
                # Insert material
                conn.execute("""
                    INSERT OR REPLACE INTO materials
                    (id, name_canonical, unit, work_rate, category, active, default_vendor_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    item['id'],
                    item['name'],
                    item['unit'],
                    item.get('labor_cost', 0),
                    item.get('category', 'Строительные материалы'),
                    item.get('active', True),
                    item.get('default_vendor_id')
                ))

                # Insert price history if exists
                if 'price_history' in item:
                    for price_entry in item['price_history']:
                        # Create price source
                        source_id = self._get_or_create_price_source(conn, price_entry)

                        # Insert price
                        conn.execute("""
                            INSERT INTO material_prices
                            (material_id, price, price_date, source_id, is_active)
                            VALUES (?, ?, ?, ?, 1)
                        """, (
                            item['id'],
                            price_entry['price'],
                            price_entry['price_date'],
                            source_id
                        ))

                # Insert aliases if exist
                if 'aliases' in item:
                    for alias in item['aliases']:
                        conn.execute("""
                            INSERT OR IGNORE INTO material_aliases
                            (material_id, alias_name, source)
                            VALUES (?, ?, 'manual')
                        """, (item['id'], alias))

            conn.commit()

    def _get_or_create_price_source(self, conn, price_entry):
        """Get or create price source for price entry"""
        cursor = conn.execute("""
            SELECT id FROM price_sources
            WHERE type = ? AND name = ?
        """, (price_entry.get('source_type', 'manual'), price_entry.get('source_name', 'Migration')))

        result = cursor.fetchone()
        if result:
            return result[0]

        # Create new source
        cursor = conn.execute("""
            INSERT INTO price_sources (type, name, doc_date)
            VALUES (?, ?, ?)
        """, (
            price_entry.get('source_type', 'manual'),
            price_entry.get('source_name', 'Migration'),
            price_entry.get('price_date', date.today().isoformat())
        ))
        return cursor.lastrowid

    # Customer management
    def add_customer(self, name: str, preferred_price_source_type: str = 'invoice') -> int:
        with self.connect() as conn:
            cursor = conn.execute("""
                INSERT INTO customers (name, preferred_price_source_type)
                VALUES (?, ?)
            """, (name, preferred_price_source_type))
            return cursor.lastrowid

    def get_customers(self) -> List[Dict]:
        with self.connect() as conn:
            cursor = conn.execute("SELECT * FROM customers ORDER BY name")
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Vendor management
    def add_vendor(self, name: str, website_url: str = None) -> int:
        with self.connect() as conn:
            cursor = conn.execute("""
                INSERT INTO vendors (name, website_url)
                VALUES (?, ?)
            """, (name, website_url))
            return cursor.lastrowid

    def get_vendors(self) -> List[Dict]:
        with self.connect() as conn:
            cursor = conn.execute("SELECT * FROM vendors ORDER BY name")
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Material management
    def add_material(self, name: str, unit: str, work_rate: float,
                    category: str = None, default_vendor_id: int = None) -> str:
        import uuid
        material_id = str(uuid.uuid4())

        with self.connect() as conn:
            conn.execute("""
                INSERT INTO materials
                (id, name_canonical, unit, work_rate, category, default_vendor_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (material_id, name, unit, work_rate, category, default_vendor_id))

        return material_id

    def get_material_by_name(self, name: str, unit: str) -> Optional[Dict]:
        with self.connect() as conn:
            cursor = conn.execute("""
                SELECT * FROM materials
                WHERE name_canonical = ? AND unit = ? AND active = 1
            """, (name, unit))
            columns = [desc[0] for desc in cursor.description]
            result = cursor.fetchone()
            return dict(zip(columns, result)) if result else None

    def find_material_by_alias(self, alias: str, customer_id: int = None) -> List[Dict]:
        with self.connect() as conn:
            if customer_id:
                cursor = conn.execute("""
                    SELECT m.*, ma.alias_name
                    FROM materials m
                    JOIN material_aliases ma ON m.id = ma.material_id
                    WHERE ma.alias_name = ? AND (ma.customer_id = ? OR ma.customer_id IS NULL)
                    AND m.active = 1
                """, (alias, customer_id))
            else:
                cursor = conn.execute("""
                    SELECT m.*, ma.alias_name
                    FROM materials m
                    JOIN material_aliases ma ON m.id = ma.material_id
                    WHERE ma.alias_name = ? AND m.active = 1
                """, (alias,))

            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # Price management
    def add_price_source(self, type_: str, name: str, customer_id: int = None,
                        vendor_id: int = None, doc_date: str = None, meta: str = None) -> int:
        with self.connect() as conn:
            cursor = conn.execute("""
                INSERT INTO price_sources (type, name, customer_id, vendor_id, doc_date, meta)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (type_, name, customer_id, vendor_id, doc_date, meta))
            return cursor.lastrowid

    def add_material_price(self, material_id: str, price: float, price_date: str,
                          source_id: int, currency: str = 'RUB') -> int:
        with self.connect() as conn:
            cursor = conn.execute("""
                INSERT INTO material_prices
                (material_id, price, currency, price_date, source_id, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
            """, (material_id, price, currency, price_date, source_id))
            return cursor.lastrowid

    def get_current_price(self, material_id: str, calculation_date: str = None,
                         customer_id: int = None) -> Optional[Dict]:
        """Get the most relevant price for a material"""
        if not calculation_date:
            calculation_date = date.today().isoformat()

        with self.connect() as conn:
            # Get customer preferences
            preferred_type = 'invoice'
            if customer_id:
                cursor = conn.execute("SELECT preferred_price_source_type FROM customers WHERE id = ?",
                                    (customer_id,))
                result = cursor.fetchone()
                if result:
                    preferred_type = result[0]

            # Find best price
            cursor = conn.execute("""
                SELECT mp.*, ps.type, ps.customer_id as source_customer_id
                FROM material_prices mp
                JOIN price_sources ps ON mp.source_id = ps.id
                WHERE mp.material_id = ? AND mp.price_date <= ? AND mp.is_active = 1
                ORDER BY
                    CASE ps.type
                        WHEN ? THEN 1
                        WHEN 'invoice' THEN 2
                        WHEN 'website' THEN 3
                        WHEN 'manual' THEN 4
                        ELSE 5
                    END,
                    mp.price_date DESC
                LIMIT 1
            """, (material_id, calculation_date, preferred_type))

            columns = [desc[0] for desc in cursor.description]
            result = cursor.fetchone()
            return dict(zip(columns, result)) if result else None

    # Import session management
    def create_import_session(self, source_file: str, customer_id: int = None,
                            vendor_id: int = None) -> int:
        with self.connect() as conn:
            cursor = conn.execute("""
                INSERT INTO import_sessions (source_file, customer_id, vendor_id)
                VALUES (?, ?, ?)
            """, (source_file, customer_id, vendor_id))
            return cursor.lastrowid

    def update_import_session(self, session_id: int, status: str,
                            processed_rows: int = None, error_rows: int = None):
        with self.connect() as conn:
            updates = []
            params = []
            if processed_rows is not None:
                updates.append("processed_rows = ?")
                params.append(processed_rows)
            if error_rows is not None:
                updates.append("error_rows = ?")
                params.append(error_rows)
            updates.append("status = ?")
            params.append(status)
            params.append(session_id)

            conn.execute(f"""
                UPDATE import_sessions
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)

    def add_unmatched_import(self, session_id: int, raw_name: str, raw_price: float = None,
                           raw_unit: str = None, raw_article: str = None,
                           suggested_material_id: str = None) -> int:
        with self.connect() as conn:
            cursor = conn.execute("""
                INSERT INTO unmatched_imports
                (import_session_id, raw_name, raw_price, raw_unit, raw_article, suggested_material_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (session_id, raw_name, raw_price, raw_unit, raw_article, suggested_material_id))
            return cursor.lastrowid

    def get_unmatched_imports(self, session_id: int) -> List[Dict]:
        with self.connect() as conn:
            cursor = conn.execute("""
                SELECT * FROM unmatched_imports
                WHERE import_session_id = ? AND resolution_status = 'pending'
                ORDER BY created_at
            """, (session_id,))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

# Global instance
db_manager = DatabaseManager()
