-- CREATE TABLE accounts_stg (
-- 	created_at TIMESTAMP NOT NULL,
-- 	last_updated_at TIMESTAMP NOT NULL,
--   	user_id SERIAL PRIMARY KEY, 
--   	username VARCHAR (50) UNIQUE NOT NULL, 
--   	password VARCHAR (50) NOT NULL
-- );

CREATE TABLE country_location_stg (
  	created_at TIMESTAMP NOT NULL,
	last_updated_at TIMESTAMP NOT NULL,
  	country_id SERIAL PRIMARY KEY, 
  	country_name VARCHAR (100) NOT NULL
);

----
CREATE TABLE city_location_stg (
  	created_at TIMESTAMP NOT NULL,
	last_updated_at TIMESTAMP NOT NULL,
  	city_id SERIAL PRIMARY KEY, 
  	city_name VARCHAR(100) NOT NULL,
  	country_id int NOT NULL REFERENCES country_location_stg (country_id)
);

CREATE TABLE product_stg (
	created_at TIMESTAMP NOT NULL,
	last_updated_at TIMESTAMP NOT NULL,
  	product_id SERIAL PRIMARY KEY, 
  	product_name TEXT NOT NULL,
  	category_id int NOT NULL
)

---
CREATE TABLE customer_stg (
  	created_at TIMESTAMP NOT NULL,
	last_updated_at TIMESTAMP NOT NULL,
  	customer_id SERIAL PRIMARY KEY, 
-- 	user_id int not null REFERENCES accounts_stg (user_id),
  	customer_name VARCHAR(100) NOT NULL,
  	email VARCHAR(255) UNIQUE NOT NULL,
	phone VARCHAR(15), -- UNIQUE NOT NULL,
	city_id int not null REFERENCES city_location_stg (city_id),
-- 	country_id int NOT NULL REFERENCES country_location_stg (country_id),
	address_details VARCHAR(255)
);

---
CREATE SEQUENCE seq INCREMENT BY 1;

CREATE TABLE order_stg (
	created_at TIMESTAMP NOT NULL,
	last_updated_at TIMESTAMP NOT NULL,
  	order_id SERIAL PRIMARY KEY,
	order_code text DEFAULT 'ORDER'||nextval('seq')::text, 
	status varchar(30) not null,
  	customer_id int not null REFERENCES customer_stg (customer_id),
  	channnel VARCHAR(50) not null,
	currency_code TEXT CHECK (currency_code IN ('USD', 'EUR', 'GBP')),
	warehouse_code varchar(10) not null,
	city_shipping_id int NOT NULL REFERENCES city_location_stg (city_id),
	total_product_order_quantity int not null,
-- 	unit_price NUMERIC(7,2) not null,
	total_price NUMERIC(10,2) not null,
	total_cost NUMERIC(10,2) not null
);

---
CREATE TABLE order_product_stg (
	created_at TIMESTAMP NOT NULL,
	last_updated_at TIMESTAMP NOT NULL,
  	order_product_id serial PRIMARY KEY,
	order_id int NOT NULL REFERENCES order_stg (order_id),
	product_id int not null REFERENCES product_stg (product_id),
	currency_code TEXT CHECK (currency_code IN ('USD', 'EUR', 'GBP')),
	order_quantity int not null,
	unit_price NUMERIC(7,2) not null,
	total_unit_price NUMERIC(10,2) not null
);