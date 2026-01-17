import pandas as pd
import openpyxl
from typing import List, Dict, Optional, Tuple
import os
import re
from datetime import datetime, date
from database_manager import db_manager

class PriceImporter:
    def __init__(self):
        self.db = db_manager

    def import_from_file(self, file_path: str, customer_id: int = None,
                        vendor_id: int = None, doc_date: str = None) -> Dict:
        """
        Import prices from Excel or CSV file

        Returns:
            Dict with import results and statistics
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Create import session
        session_id = self.db.create_import_session(file_path, customer_id, vendor_id)

        try:
            # Read file
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8')
            else:
                df = pd.read_excel(file_path, engine='openpyxl')

            # Create price source
            source_name = f"Import from {os.path.basename(file_path)}"
            if doc_date:
                source_name += f" ({doc_date})"

            source_id = self.db.add_price_source(
                type_='invoice',
                name=source_name,
                customer_id=customer_id,
                vendor_id=vendor_id,
                doc_date=doc_date or date.today().isoformat()
            )

            # Process rows
            results = self._process_dataframe(df, source_id, session_id, customer_id)

            # Update session status
            self.db.update_import_session(
                session_id,
                'completed',
                processed_rows=results['processed'],
                error_rows=results['errors']
            )

            return {
                'session_id': session_id,
                'total_rows': len(df),
                'processed': results['processed'],
                'errors': results['errors'],
                'unmatched': results['unmatched'],
                'price_source_id': source_id
            }

        except Exception as e:
            self.db.update_import_session(session_id, 'failed')
            raise e

    def _process_dataframe(self, df: pd.DataFrame, source_id: int,
                          session_id: int, customer_id: int = None) -> Dict:
        """Process DataFrame rows and import prices"""
        processed = 0
        errors = 0
        unmatched = []

        # Detect columns
        column_mapping = self._detect_columns(df.columns.tolist())

        for idx, row in df.iterrows():
            try:
                # Extract data
                material_data = self._extract_material_data(row, column_mapping)

                if not material_data['name']:
                    errors += 1
                    continue

                # Try to match material
                matched_material = self._match_material(
                    material_data['name'],
                    material_data['unit'],
                    material_data['article'],
                    customer_id
                )

                if matched_material:
                    # Add price
                    self.db.add_material_price(
                        material_id=matched_material['id'],
                        price=material_data['price'],
                        price_date=date.today().isoformat(),
                        source_id=source_id
                    )
                    processed += 1

                    # Add alias if different from canonical name
                    if material_data['name'] != matched_material['name_canonical']:
                        self._add_alias_if_not_exists(
                            matched_material['id'],
                            material_data['name'],
                            customer_id,
                            'import'
                        )
                else:
                    # Add to unmatched
                    self.db.add_unmatched_import(
                        session_id=session_id,
                        raw_name=material_data['name'],
                        raw_price=material_data['price'],
                        raw_unit=material_data['unit'],
                        raw_article=material_data['article']
                    )
                    unmatched.append(material_data)
                    errors += 1

            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                errors += 1

        return {
            'processed': processed,
            'errors': errors,
            'unmatched': unmatched
        }

    def _detect_columns(self, columns: List[str]) -> Dict[str, str]:
        """Detect column mappings based on common patterns"""
        mapping = {}

        # Common patterns for material name
        name_patterns = [
            r'наименован', r'материал', r'товар', r'продукт',
            r'name', r'material', r'product', r'item'
        ]

        # Common patterns for price
        price_patterns = [
            r'цена', r'стоимость', r'price', r'cost'
        ]

        # Common patterns for unit
        unit_patterns = [
            r'ед\.?\s*изм', r'единица', r'unit'
        ]

        # Common patterns for article
        article_patterns = [
            r'артикул', r'код', r'article', r'code', r'sku'
        ]

        for col in columns:
            col_lower = col.lower().strip()

            if any(re.search(pattern, col_lower) for pattern in name_patterns):
                mapping['name'] = col
            elif any(re.search(pattern, col_lower) for pattern in price_patterns):
                mapping['price'] = col
            elif any(re.search(pattern, col_lower) for pattern in unit_patterns):
                mapping['unit'] = col
            elif any(re.search(pattern, col_lower) for pattern in article_patterns):
                mapping['article'] = col

        return mapping

    def _extract_material_data(self, row, column_mapping: Dict) -> Dict:
        """Extract material data from row"""
        def safe_get(column_name):
            if column_name in column_mapping:
                value = row[column_mapping[column_name]]
                return str(value).strip() if pd.notna(value) else None
            return None

        def safe_get_numeric(column_name):
            if column_name in column_mapping:
                value = row[column_mapping[column_name]]
                if pd.notna(value):
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None
            return None

        return {
            'name': safe_get('name'),
            'price': safe_get_numeric('price'),
            'unit': safe_get('unit'),
            'article': safe_get('article')
        }

    def _match_material(self, name: str, unit: str = None, article: str = None,
                       customer_id: int = None) -> Optional[Dict]:
        """Try to match material by name, unit, or article"""
        if not name:
            return None

        # Clean name
        clean_name = self._clean_material_name(name)

        # Try exact match by canonical name
        if unit:
            material = self.db.get_material_by_name(clean_name, unit)
            if material:
                return material

        # Try by aliases
        aliases = self.db.find_material_by_alias(clean_name, customer_id)
        if aliases:
            # Return first match (could be improved with scoring)
            return aliases[0]

        # Try fuzzy matching (placeholder for future implementation)
        # For now, return None
        return None

    def _clean_material_name(self, name: str) -> str:
        """Clean and normalize material name"""
        if not name:
            return ""

        # Remove extra spaces, lowercase
        cleaned = re.sub(r'\s+', ' ', name.strip().lower())

        # Remove common prefixes/suffixes
        cleaned = re.sub(r'^(товар|материал|продукт)\s*', '', cleaned)
        cleaned = re.sub(r'\s*(упаковка|штука|кг|м|м²|м³)$', '', cleaned)

        return cleaned

    def _add_alias_if_not_exists(self, material_id: str, alias: str,
                               customer_id: int = None, source: str = 'import'):
        """Add alias if it doesn't exist"""
        # Check if alias already exists
        existing = self.db.find_material_by_alias(alias, customer_id)
        if not existing:
            # Add new alias (would need to extend DB manager)
            pass  # Placeholder

    def get_import_results(self, session_id: int) -> Dict:
        """Get detailed results of an import session"""
        unmatched = self.db.get_unmatched_imports(session_id)

        return {
            'session_id': session_id,
            'unmatched_count': len(unmatched),
            'unmatched_items': unmatched
        }

    def resolve_unmatched(self, unmatched_id: int, material_id: str):
        """Manually resolve unmatched import item"""
        # Update unmatched record
        # This would need DB extension
        pass

# Global instance
price_importer = PriceImporter()
