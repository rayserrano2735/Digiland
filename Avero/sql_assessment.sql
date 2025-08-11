/*
Business Case: E-commerce Order Management System
Need to track: customers, products, orders, order items, inventory
*/

-- 1. SQL Schema Design (PostgreSQL)
--scenario1_schema = """

create schema if not exists avero;

-- set default schema name
set search_path = avero;


-- Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table  
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory tracking
CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    quantity_available INTEGER NOT NULL DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    last_restocked TIMESTAMP,
    CONSTRAINT positive_quantity CHECK (quantity_available >= 0)
);

-- Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    total_amount DECIMAL(10,2),
    shipping_address TEXT,
    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled'))
);

-- Order items (junction table for many-to-many)
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    subtotal DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    CONSTRAINT positive_quantity CHECK (quantity > 0)
);

-- Indexes for performance
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_inventory_product ON inventory(product_id);


/*
 *   3. Lawyers
 */

-- Option 1 Window function based
WITH actor_movie AS (
    SELECT DISTINCT part_id, party_role_id 
    FROM MOVIE_ROLE
),
actor_pairs AS (
    SELECT 
        am1.party_role_id as actor1_id,
        am2.party_role_id as actor2_id,
        am1.part_id,
        ROW_NUMBER() OVER (
            PARTITION BY 
                LEAST(am1.party_role_id, am2.party_role_id),
                GREATEST(am1.party_role_id, am2.party_role_id)
            ORDER BY am1.part_id
        ) as running_count
    FROM actor_movie am1
    JOIN actor_movie am2 
        ON am1.part_id = am2.part_id
        AND am1.party_role_id < am2.party_role_id
)
SELECT 
    actor1_id,
    actor2_id,
    MAX(running_count) as movies_together
FROM actor_pairs
GROUP BY actor1_id, actor2_id
ORDER BY movies_together DESC
LIMIT 20;

-- Option 2 Non-redundant window function
-- performance: 3m43s
WITH actor_movie AS (
    SELECT DISTINCT part_id, party_role_id 
    FROM MOVIE_ROLE
),
actor_pairs AS (
    SELECT DISTINCT  
        am1.party_role_id as actor1,
        am2.party_role_id as actor2,
        COUNT(*) OVER (
            PARTITION BY am1.party_role_id, am2.party_role_id
        ) as cnt
    FROM actor_movie am1
    JOIN actor_movie am2 
        ON am1.part_id = am2.part_id
        AND am1.party_role_id < am2.party_role_id
)
SELECT * FROM actor_pairs
ORDER BY cnt DESC
LIMIT 20;

-- option 3 Vintage approach
-- TODO: check if this works with large dataset
-- finding actor pairs who worked together most
WITH actor_movie AS (
    SELECT DISTINCT 
        part_id, 
        party_role_id 
    FROM MOVIE_ROLE  -- fixed: was using wrong table name earlier
)
-- self join to find pairs
SELECT 
    am1.party_role_id as actor1,  -- could use better naming here
    am2.party_role_id as actor2,
    COUNT(*) as cnt  -- abbreviated because why not
FROM actor_movie am1
JOIN actor_movie am2 
    ON am1.part_id = am2.part_id  
WHERE am1.party_role_id < am2.party_role_id  -- prevent duplicates
GROUP BY 1, 2  -- lazy GROUP BY
ORDER BY 3 DESC  -- even lazier ORDER BY
LIMIT 20;

-- NOTE: might need index on part_id for performance
-- UPDATE: tested on prod, takes ~45 seconds

-- option 4 Vintage 2
-- finding which actors worked together most
-- todo: test performance on full dataset
-- performance: 1m56s
WITH actor_movie AS (
    SELECT DISTINCT 
        part_id, 
        party_role_id 
    FROM movie_role  -- had to fix table name
)
-- self join to get pairs
SELECT 
    am1.party_role_id as actor1,  
    am2.party_role_id as actor2,
    COUNT(*) as cnt
FROM actor_movie am1
JOIN actor_movie am2 
    ON am1.part_id = am2.part_id  
WHERE am1.party_role_id < am2.party_role_id  -- avoid dupes
GROUP BY 1, 2  
ORDER BY 3 DESC  
LIMIT 20;

-- takes forever on prod
-- ~45 sec but whatever it works
