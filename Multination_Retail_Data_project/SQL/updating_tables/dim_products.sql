-- Clean and convert the product_price column
UPDATE dim_products
SET product_price = REPLACE(CAST(product_price AS TEXT), '£', '')::FLOAT;

-- Clean and convert the weight column
UPDATE dim_products
SET weight = REPLACE(CAST(weight AS TEXT), 'kg', '');



-- Add weight_class column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'dim_products' AND column_name = 'weight_class') THEN
        ALTER TABLE dim_products
        ADD COLUMN weight_class VARCHAR(50);
    END IF;
END $$;

ALTER TABLE dim_products
    ALTER COLUMN weight TYPE FLOAT USING (weight::FLOAT);

-- Populate the weight_class column based on weight ranges
UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE NULL
END;

-- Rename the removed column to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

UPDATE dim_products
SET still_available = CASE
    WHEN still_available = 'still available' THEN TRUE
    WHEN still_available = 'removed' THEN FALSE
    ELSE NULL
END;

-- Change the data types of columns
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN date_added TYPE DATE,
    ALTER COLUMN uuid TYPE UUID USING (uuid::UUID),
    ALTER COLUMN still_available TYPE BOOLEAN USING (still_available::BOOLEAN); 