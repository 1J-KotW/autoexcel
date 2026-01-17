-- Database schema for extended material catalog system
-- Supports multiple customers, price history, aliases, and price sources

-- Customers table
CREATE TABLE customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    preferred_price_source_type TEXT DEFAULT 'invoice',
    currency TEXT DEFAULT 'RUB',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Vendors/Suppliers table
CREATE TABLE vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    website_url TEXT,
    contact_info TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Materials table (extended)
CREATE TABLE materials (
    id TEXT PRIMARY KEY,
    name_canonical TEXT NOT NULL,
    unit TEXT NOT NULL,
    work_rate REAL NOT NULL, -- базовая стоимость работ
    category TEXT,
    active BOOLEAN DEFAULT 1,
    default_vendor_id INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (default_vendor_id) REFERENCES vendors(id)
);

-- Material aliases for different customers
CREATE TABLE material_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    material_id TEXT NOT NULL,
    customer_id INTEGER,
    alias_name TEXT NOT NULL,
    source TEXT DEFAULT 'manual', -- manual, import, ai, fuzzy
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (material_id) REFERENCES materials(id),
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    UNIQUE(material_id, customer_id, alias_name)
);

-- Price sources (files, websites, manual)
CREATE TABLE price_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL, -- invoice, website, manual
    name TEXT NOT NULL,
    customer_id INTEGER,
    vendor_id INTEGER,
    doc_date DATE,
    meta TEXT, -- JSON with additional metadata
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
);

-- Material price history
CREATE TABLE material_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    material_id TEXT NOT NULL,
    price REAL NOT NULL,
    currency TEXT DEFAULT 'RUB',
    price_date DATE NOT NULL,
    source_id INTEGER NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    valid_from DATE,
    valid_to DATE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (material_id) REFERENCES materials(id),
    FOREIGN KEY (source_id) REFERENCES price_sources(id)
);

-- Import sessions for tracking batch operations
CREATE TABLE import_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT,
    customer_id INTEGER,
    vendor_id INTEGER,
    import_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'processing', -- processing, completed, failed
    total_rows INTEGER DEFAULT 0,
    processed_rows INTEGER DEFAULT 0,
    error_rows INTEGER DEFAULT 0
);

-- Unmatched rows from imports (for manual resolution)
CREATE TABLE unmatched_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_session_id INTEGER NOT NULL,
    raw_name TEXT NOT NULL,
    raw_price REAL,
    raw_unit TEXT,
    raw_article TEXT,
    suggested_material_id TEXT,
    resolution_status TEXT DEFAULT 'pending', -- pending, resolved, rejected
    resolved_material_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (import_session_id) REFERENCES import_sessions(id),
    FOREIGN KEY (suggested_material_id) REFERENCES materials(id),
    FOREIGN KEY (resolved_material_id) REFERENCES materials(id)
);

-- Indexes for performance
CREATE INDEX idx_material_prices_material_date ON material_prices(material_id, price_date DESC);
CREATE INDEX idx_material_prices_active ON material_prices(is_active);
CREATE INDEX idx_material_aliases_lookup ON material_aliases(alias_name, customer_id);
CREATE INDEX idx_price_sources_type_date ON price_sources(type, doc_date DESC);

-- Views for convenience
CREATE VIEW active_material_prices AS
SELECT mp.*, m.name_canonical, m.unit, ps.name as source_name, ps.type as source_type
FROM material_prices mp
JOIN materials m ON mp.material_id = m.id
JOIN price_sources ps ON mp.source_id = ps.id
WHERE mp.is_active = 1 AND m.active = 1;

CREATE VIEW latest_prices AS
SELECT material_id, MAX(price_date) as latest_date
FROM material_prices
WHERE is_active = 1
GROUP BY material_id;
