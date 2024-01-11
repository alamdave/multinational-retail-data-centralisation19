ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::UUID),
    ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::UUID),
    ALTER COLUMN card_number TYPE VARCHAR(22) USING (card_number::VARCHAR(22)),
    ALTER COLUMN store_code TYPE VARCHAR(12) USING (store_code::VARCHAR(12)),
    ALTER COLUMN product_code TYPE VARCHAR(11) USING (product_code::VARCHAR(11)),
    ALTER COLUMN product_quantity TYPE SMALLINT USING (product_quantity::SMALLINT);