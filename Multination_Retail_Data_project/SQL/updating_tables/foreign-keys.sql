-- Add foreign key constraint for dim_users
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

-- Add foreign key constraint for dim_store_details
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

-- Add foreign key constraint for dim_products
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);

-- Add foreign key constraint for dim_date_times
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

-- Add foreign key constraint for dim_card_details
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);