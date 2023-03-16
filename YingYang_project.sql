--DATASET SOURCE : https://www.kaggle.com/olistbr/brazilian-ecommerce
/*********************************** IMPORT DATASETS ***********************************/
CREATE SCHEMA IF NOT EXISTS project;

DROP SERVER IF EXISTS svr_csv CASCADE; --this part is needed if you add more files or structure of files changed

CREATE SERVER svr_csv FOREIGN DATA WRAPPER ogr_fdw

OPTIONS (datasource '/ftp/yyang', format 'CSV');

IMPORT FOREIGN SCHEMA ogr_all FROM SERVER svr_csv INTO project;

-- CREATE SCHEMA project;

/*********************************** CREAT TABLES ***********************************/
--------------------------------------orders--------------------------------------
DROP TABLE IF EXISTS project.orders;

CREATE TABLE project.orders (
    order_id varchar(50) NOT NULL,
    customer_id varchar(50) NOT NULL,
	order_status varchar(15),
    order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp
);
SELECT * FROM project.orders;

--------------------------------------customers--------------------------------------
DROP TABLE IF EXISTS project.customers;

CREATE TABLE project.customers (
    customer_id varchar(50) ,
	customer_unique_id varchar(50) NOT NULL,
	customer_zip_code_prefix char(5),
	customer_city varchar(50),
	customer_state char(2)
);
SELECT * FROM project.customers;

--------------------------------------items--------------------------------------
DROP TABLE IF EXISTS project.items;
	
CREATE TABLE project.items (
	order_item_id serial,
	order_id varchar(50) NOT NULL,
    quantity smallint NOT NULL,
	product_id varchar(50) ,
    seller_id varchar(50) ,
	shipping_limit_date timestamp,
	price numeric(6,2),
	freight_value numeric(5,2)
);

SELECT * FROM project.items;

--------------------------------------payments--------------------------------------
DROP TABLE IF EXISTS project.payments;

CREATE TABLE project.payments (
	payment_id serial,
    order_id varchar(50) NOT NULL,
	payment_type varchar(20),
    payment_installments smallint,
	payment_value numeric(7,2),
	order_purchase_timestamp timestamp
);

ALTER TABLE project.payments ADD COLUMN due_date timestamp;

SELECT * FROM project.payments;
--------------------------------------reviews--------------------------------------
DROP TABLE IF EXISTS project.reviews;

CREATE TABLE project.reviews (
    review_id varchar(50) NOT NULL,
    order_id varchar(50) NOT NULL,
    review_score smallint,
	review_comment_title varchar(50),
	review_comment_message varchar(100),
	review_creation_date timestamp,
	review_answer_timestamp timestamp
);

SELECT * FROM project.reviews;
--------------------------------------products--------------------------------------
DROP TABLE IF EXISTS project.products;

CREATE TABLE project.products (
    product_id varchar(50) NOT NULL,
	product_category_name varchar(50),
	product_name_lenght smallint,
	product_description_lengh smallint,
	product_photos_qty smallint,
	product_weight_g int,
	product_length_cm smallint,
	product_height_cm smallint,
	product_width_cm smallint
);

SELECT * FROM project.products;

--------------------------------------sellers--------------------------------------
DROP TABLE IF EXISTS project.sellers;

CREATE TABLE project.sellers (
    seller_id varchar(50) NOT NULL,
    seller_zip_code_prefix char(5),
    seller_city varchar(20),
	seller_state char(2)
);

SELECT * FROM project.sellers;

--------------------------------------category--------------------------------------
DROP TABLE IF EXISTS project.category;

CREATE TABLE project.category (
    product_category_name varchar(50) ,
	product_category_name_english varchar(50)
);

SELECT * FROM project.category;

--------------------------------------payments_received--------------------------------------
DROP TABLE IF EXISTS project.payments_received;

CREATE TABLE project.payments_received (
    payments_sequential serial,
	order_id varchar(50) NOT NULL,
	payments_received_date date,
	payment_received_amount numeric(5,2)
);

SELECT * FROM project.payments_received;
/*********************************** ADD PRIMARY KEY ***********************************/
ALTER TABLE project.orders ADD CONSTRAINT pk_orders_orderid PRIMARY KEY(order_id);
SELECT * FROM project.orders;

ALTER TABLE project.customers ADD CONSTRAINT pk_customers_customerid PRIMARY KEY(customer_id);
SELECT * FROM project.customers;

ALTER TABLE project.items ADD CONSTRAINT pk_items_order_item_id PRIMARY KEY(order_item_id);
SELECT * FROM project.items;

ALTER TABLE project.payments DROP CONSTRAINT IF EXISTS pk_payments_paymentid;
ALTER TABLE project.payments ADD CONSTRAINT pk_payments_paymentid PRIMARY KEY(payment_id);
SELECT * FROM project.payments ; 


SELECT COUNT(DISTINCT review_id) FROM project.reviews;
SELECT COUNT(review_id) FROM project.reviews;
SELECT COUNT(*) FROM project.reviews;
ALTER TABLE project.reviews DROP CONSTRAINT IF EXISTS pk_reviews_reviewid_orderid;
ALTER TABLE project.reviews ADD CONSTRAINT pk_reviews_reviewid_orderid PRIMARY KEY(review_id, order_id);
SELECT * FROM project.reviews;

ALTER TABLE project.products ADD CONSTRAINT pk_products_productid PRIMARY KEY(product_id);
SELECT * FROM project.products;

ALTER TABLE project.sellers ADD CONSTRAINT pk_sellers_sellerid PRIMARY KEY(seller_id);
SELECT * FROM project.sellers;

ALTER TABLE project.category DROP CONSTRAINT IF EXISTS pk_category_name ;
ALTER TABLE project.category ADD CONSTRAINT pk_category_name PRIMARY KEY(product_category_name);
SELECT * FROM project.category;

ALTER TABLE project.payments_received ADD CONSTRAINT pk_payments_sequential PRIMARY KEY(payments_sequential);
SELECT * FROM project.payments_received;

/*********************************** ADD FOREIGN KEY ***********************************/
---------------------------- refer to customers in order table ----------------------------
ALTER TABLE project.orders
ADD CONSTRAINT fk_orders_customerid FOREIGN KEY (customer_id)
REFERENCES project.customers (customer_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

---------------------------- *create after import data :refer to product english name in product table ----------------------------
SELECT * FROM project.items WHERE product_id IN (SELECT product_id FROM project.products WHERE product_category_name IS NULL);

UPDATE project.products SET product_category_name = 'Not_specified' WHERE product_category_name IS NULL;
INSERT INTO project.category (product_category_name)
SELECT product_category_name FROM project.products
EXCEPT
SELECT product_category_name FROM project.category;

ALTER TABLE project.products
ADD CONSTRAINT fk_products_name FOREIGN KEY (product_category_name)
REFERENCES project.category (product_category_name)
ON UPDATE CASCADE 
ON DELETE CASCADE;

---------------------------- refer to product id in item table ----------------------------
ALTER TABLE project.items
ADD CONSTRAINT fk_items_productid FOREIGN KEY (product_id)
REFERENCES project.products (product_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

---------------------------- refer to order id in item table ----------------------------
ALTER TABLE project.items
ADD CONSTRAINT fk_items_orderid FOREIGN KEY (order_id)
REFERENCES project.orders (order_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

---------------------------- refer to seller id in item table ----------------------------
ALTER TABLE project.items
ADD CONSTRAINT fk_items_sellerid FOREIGN KEY (seller_id)
REFERENCES project.sellers (seller_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

---------------------------- refer to order id in payment table ----------------------------
ALTER TABLE project.payments
ADD CONSTRAINT fk_payments_orderid FOREIGN KEY (order_id)
REFERENCES project.orders (order_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

---------------------------- refer to order id in review table ----------------------------
ALTER TABLE project.reviews
ADD CONSTRAINT fk_reviews_orderid FOREIGN KEY (order_id)
REFERENCES project.orders (order_id)
ON UPDATE CASCADE 
ON DELETE CASCADE;

---------------------------- refer to order id in payment_received table ----------------------------
ALTER TABLE project.payments_received
ADD CONSTRAINT fk_payments_received_orderid FOREIGN KEY (order_id)
REFERENCES project.orders (order_id)
ON UPDATE RESTRICT
ON DELETE RESTRICT;

/*********************************** INSERT VALUES ***********************************/
--------------------------------------orders--------------------------------------
INSERT INTO project.orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
SELECT
    order_id,
    customer_id::varchar(50),
	order_status::varchar(15),
    order_purchase_timestamp::timestamp,
	order_approved_at::timestamp,
	order_delivered_carrier_date::timestamp,
	order_delivered_customer_date::timestamp,
	order_estimated_delivery_date::timestamp
FROM project.olist_orders_dataset;

SELECT * FROM project.orders LIMIT 10;

--------------------------------------customers--------------------------------------
INSERT INTO project.customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
SELECT
    customer_id::varchar(50) ,
	customer_unique_id::varchar(50),
	customer_zip_code_prefix::char(5),
	customer_city::varchar(50),
	customer_state::char(2)
FROM project.olist_customers_dataset;

SELECT * FROM project.customers LIMIT 10; 

--------------------------------------payments--------------------------------------
INSERT INTO project.payments (order_id, payment_type, payment_installments, payment_value)
SELECT
    order_id::varchar(50),
	payment_type::varchar(20),
    payment_installments::smallint,
	payment_value::numeric(7,2)
FROM project.olist_order_payments_dataset;


UPDATE project.payments p 
SET order_purchase_timestamp = (
	SELECT order_purchase_timestamp FROM project.orders o WHERE o.order_id = p.order_id);


SELECT * FROM project.payments LIMIT 10;

--------------------------------------reviews--------------------------------------
INSERT INTO project.reviews (review_id, order_id , review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
SELECT
    review_id::varchar(50),
    order_id::varchar(50),
    review_score::smallint,
	review_comment_title::varchar(50),
	review_comment_message::varchar(100),
	review_creation_date::timestamp,
	review_answer_timestamp::timestamp
FROM project.olist_order_reviews_dataset;

SELECT * FROM project.reviews LIMIT 10;

--------------------------------------products--------------------------------------
INSERT INTO project.products (product_id, product_category_name, product_name_lenght, product_description_lengh, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
SELECT
    product_id::varchar(50),
	product_category_name::varchar(50),
	product_name_lenght::smallint,
	product_description_lenght::smallint,
	product_photos_qty::smallint,
	product_weight_g::int,
	product_length_cm::smallint,
	product_height_cm::smallint,
	product_width_cm::smallint
FROM project.olist_products_dataset;

SELECT * FROM project.products LIMIT 10;

--------------------------------------sellers--------------------------------------
INSERT INTO project.sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
SELECT
    seller_id::varchar(50),
    seller_zip_code_prefix::char(5),
    seller_city::varchar(20),
	seller_state::char(2)
FROM project.olist_sellers_dataset;

SELECT * FROM project.sellers LIMIT 10;

--------------------------------------items--------------------------------------
INSERT INTO project.items (order_id, quantity, product_id, seller_id, shipping_limit_date, price, freight_value)
SELECT
	order_id::varchar(50),
    order_item_id::smallint,
	product_id::varchar(50) ,
    seller_id::varchar(50) ,
	shipping_limit_date::timestamp,
	price::numeric(6,2),
	freight_value::numeric(5,2)
FROM project.olist_order_items_dataset;

SELECT * FROM project.items LIMIT 10; 

--------------------------------------category--------------------------------------
INSERT INTO project.category (product_category_name, product_category_name_english)
SELECT
    product_category_name::varchar(50) ,
	product_category_name_english::varchar(50)
FROM project.product_category_name_translation;

SELECT * FROM project.category LIMIT 10;

/*********************************** ADD CHECK/UNIQUE CONSTRAINT ***********************************/
ALTER TABLE project.items ADD CONSTRAINT ck_price_more_than_zero CHECK (price > 0 AND price <= 9999); 
ALTER TABLE project.items ADD CONSTRAINT ck_freight_not_negative CHECK (freight_value >= 0 AND freight_value <= 9999); 


ALTER TABLE project.category ADD CONSTRAINT uq_category_name UNIQUE(product_category_name);

/*********************************** ADD INDEX ***********************************/
CREATE INDEX ix_orders_orderid ON project.orders USING btree (order_id);

/*********************************** BUILD QUERY TABLE ***********************************/
DROP TABLE IF EXISTS project.queries;
CREATE TABLE project.queries(
	explanation text,
    query  text
);

/*********************************** INSERT QUERIES ***********************************/
INSERT INTO project.queries (VALUES('',''));

INSERT INTO project.queries (VALUES(
	'create payment received table for tracking',
	'CREATE TABLE project.payments_received (
    payments_sequential serial,
	order_id varchar(50) NOT NULL,
	payments_expected_date date,
	payments_expected_amount numeric(5,2),
	payments_received_date date,
	payment_received_amount numeric(5,2))'));
	
INSERT INTO project.queries (VALUES(
	'add foreign key and restricts to real payments',
	'ALTER TABLE project.payments_received
	 ADD CONSTRAINT fk_payments_received_orderid FOREIGN KEY (order_id)
	 REFERENCES project.orders (order_id)
	 ON UPDATE RESTRICT
	 ON DELETE RESTRICT'));

INSERT INTO project.queries (VALUES(
	'deal with category names that are not shown in category table but appear in product table',
	'SELECT * FROM project.items WHERE product_id IN (SELECT product_id FROM project.products WHERE product_category_name IS NULL);
	UPDATE project.products SET product_category_name = "Not_specified" WHERE product_category_name IS NULL;
	INSERT INTO project.category (product_category_name)
	SELECT product_category_name FROM project.products
	EXCEPT
	SELECT product_category_name FROM project.category'));

INSERT INTO project.queries (VALUES(
	'investigate why the review id could not be the primary key. 
	the reason is that some review id could correspond to multiple order ids.
	change the primary key into a combination of review id and order id',
	'SELECT COUNT(DISTINCT review_id) FROM project.reviews;
	 SELECT COUNT(review_id) FROM project.reviews;
	 SELECT COUNT(*) FROM project.reviews;
     ALTER TABLE project.reviews DROP CONSTRAINT IF EXISTS pk_reviews_reviewid_orderid;
	 ALTER TABLE project.reviews ADD CONSTRAINT pk_reviews_reviewid_orderid PRIMARY KEY(review_id, order_id);'));

INSERT INTO project.queries (VALUES(
	'find out after year 2018, the total payment for each customer',
	'SELECT o.customer_id, SUM(i.quantity*i.price+i.freight_value) OVER (PARTITION BY o.customer_id) AS moneraty
	FROM project.orders o
	INNER JOIN project.items i
	ON o.order_id = i.order_id
	WHERE o.order_purchase_timestamp > "2018-01-01 00:00:00"'));

INSERT INTO project.queries (VALUES(
	'pivot table for different payment method',
	'SELECT payment_type, SUM(total_payment) AS debit_card_sum FROM X GROUP BY payment_type HAVING payment_type = ''debit_card''
	UNION ALL
	SELECT payment_type, SUM(total_payment) AS credit_card_sum FROM X GROUP BY payment_type HAVING payment_type = ''credit_card''
	UNION ALL
	SELECT payment_type, SUM(total_payment) AS debit_card_sum FROM X GROUP BY payment_type HAVING payment_type = ''debit_card''
'));

INSERT INTO project.queries (VALUES(
	'find last purchase time for each regular customer',
	'WITH 
		X AS (SELECT customer_id, LEAD(order_purchase_timestamp)OVER(PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS last_purchase_time FROM project.orders)
	SELECT * FROM X WHERE last_purchase_time IS NOT NULL'));

INSERT INTO project.queries (VALUES(
	'find top 10 best selling products',
	'WITH 
		X AS   (SELECT c.product_category_name_english, SUM(i.quantity*price) AS monetary
                FROM project.orders o
                LEFT JOIN project.items i
                ON o.order_id = i.order_id
                LEFT JOIN project.products p
                ON i.product_id = p.product_id
                INNER JOIN project.category c
                ON p.product_category_name = c.product_category_name
                GROUP BY c.product_category_name_english) 
	SELECT * FROM 
	(SELECT product_category_name_english, RANK()OVER(ORDER BY monetary DESC) AS rank FROM X) t1
	WHERE rank <=10'));


UPDATE project.payments p 
SET due_date = (
	SELECT order_purchase_timestamp + payment_installments * interval '1 month' AS due_date FROM project.payments p2 WHERE p.payment_id = p2.payment_id)

SELECT * FROM queries;
/*********************************** CREATE VIEW ***********************************/
------------------------------ view the total payment value per order ------------------------------
-- take an example to see how does the price work
SELECT * FROM project.items WHERE LIMIT 100;
SELECT * FROM project.items WHERE order_item_id = 2 LIMIT 10;
SELECT * FROM project.items WHERE order_id = '0008288aa423d2a3f00fcb17cd7d8719' ;
-- order_item_id could indicate the number of items purchased
-- price is the average price of all items

CREATE VIEW project.vw_total_payment_value_per_order AS
SELECT order_id, SUM(quantity * price + freight_value) AS total_payment_value 
FROM project.items
GROUP BY order_id 
ORDER BY total_payment_value DESC;

SELECT * FROM project.vw_total_payment_value_per_order LIMIT 10;


-- check payment situation
CREATE VIEW project.vw_payment_progress AS
WITH 
	X AS (SELECT order_id, SUM(payment_received_amount) AS payment_paid FROM project.payments_received GROUP BY order_id ),
	Y AS (SELECT order_id, SUM(payment_value) AS total_payments FROM project.payments GROUP BY order_id )
SELECT X.order_id, X.payment_paid, Y.total_payments-X.payment_paid AS payment_unpaid
FROM X
INNER JOIN Y
ON X.order_id = Y.order_id;



--create function
DROP FUNCTION project.insert_payment(character varying,smallint,numeric,timestamp without time zone)
CREATE OR REPLACE FUNCTION project.insert_payment (param_order_id varchar(50), param_installment smallint, param_value numeric(7,2), param_order_purchase_timestamp timestamp)
RETURNS VOID AS 
$$
    DELETE FROM project.payments WHERE order_id = param_order_id; --clear
	
	INSERT INTO project.payments (order_id, payment_installments, payment_value, order_purchase_timestamp, due_date) 
	VALUES(
			param_order_id, param_installment, param_value, param_order_purchase_timestamp, param_order_purchase_timestamp + param_installment * interval '1 month' )

$$
LANGUAGE sql VOLATILE;

INSERT INTO project.orders (order_id,customer_id) VALUES('test_order_id','test_customer_id')
SELECT project.insert_payment('test_order_id'::varchar, 3::smallint, 1000.00, '2018-08-20 00:00:00'::timestamp)

SELECT * FROM project.payments WHERE order_id = 'test_order_id';
