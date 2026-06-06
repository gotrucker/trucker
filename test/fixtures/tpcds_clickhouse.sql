TRUNCATE DATABASE trucker;

CREATE TABLE store_sales_flat (
  id                    String,
  quantity              AggregateFunction(argMax, Int32,   DateTime64),
  net_paid              AggregateFunction(argMax, Float64, DateTime64),
  net_profit            AggregateFunction(argMax, Float64, DateTime64),
  net_paid_inc_tax      AggregateFunction(argMax, Float64, DateTime64),
  ext_discount_amt      AggregateFunction(argMax, Float64, DateTime64),
  sale_year             AggregateFunction(argMax, Int32,   DateTime64),
  sale_month            AggregateFunction(argMax, Int32,   DateTime64),
  store_name            AggregateFunction(argMax, String,  DateTime64),
  store_state           AggregateFunction(argMax, String,  DateTime64),
  customer_last_name    AggregateFunction(argMax, String,  DateTime64),
  item_category         AggregateFunction(argMax, String,  DateTime64),
  item_brand            AggregateFunction(argMax, String,  DateTime64),
  deleted               AggregateFunction(argMax, Boolean, DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_store_sales_flat AS
SELECT * FROM (
  SELECT id,
         argMaxMerge(quantity)           AS quantity,
         argMaxMerge(net_paid)           AS net_paid,
         argMaxMerge(net_profit)         AS net_profit,
         argMaxMerge(net_paid_inc_tax)   AS net_paid_inc_tax,
         argMaxMerge(ext_discount_amt)   AS ext_discount_amt,
         argMaxMerge(sale_year)          AS sale_year,
         argMaxMerge(sale_month)         AS sale_month,
         argMaxMerge(store_name)         AS store_name,
         argMaxMerge(store_state)        AS store_state,
         argMaxMerge(customer_last_name) AS customer_last_name,
         argMaxMerge(item_category)      AS item_category,
         argMaxMerge(item_brand)         AS item_brand,
         argMaxMerge(deleted)            AS deleted
  FROM store_sales_flat
  GROUP BY id
) WHERE NOT deleted;

CREATE TABLE store_ch (
  id                    String,
  store_id              AggregateFunction(argMax, String,  DateTime64),
  rec_start_date        AggregateFunction(argMax, Date32,  DateTime64),
  rec_end_date          AggregateFunction(argMax, Date32,  DateTime64),
  closed_date_sk        AggregateFunction(argMax, Int32,   DateTime64),
  store_name            AggregateFunction(argMax, String,  DateTime64),
  number_employees      AggregateFunction(argMax, Int32,   DateTime64),
  floor_space           AggregateFunction(argMax, Int32,   DateTime64),
  hours                 AggregateFunction(argMax, String,  DateTime64),
  manager               AggregateFunction(argMax, String,  DateTime64),
  market_id             AggregateFunction(argMax, Int32,   DateTime64),
  geography_class       AggregateFunction(argMax, String,  DateTime64),
  market_desc           AggregateFunction(argMax, String,  DateTime64),
  market_manager        AggregateFunction(argMax, String,  DateTime64),
  division_id           AggregateFunction(argMax, Int32,   DateTime64),
  division_name         AggregateFunction(argMax, String,  DateTime64),
  company_id            AggregateFunction(argMax, Int32,   DateTime64),
  company_name          AggregateFunction(argMax, String,  DateTime64),
  street_number         AggregateFunction(argMax, String,  DateTime64),
  street_name           AggregateFunction(argMax, String,  DateTime64),
  street_type           AggregateFunction(argMax, String,  DateTime64),
  suite_number          AggregateFunction(argMax, String,  DateTime64),
  city                  AggregateFunction(argMax, String,  DateTime64),
  county                AggregateFunction(argMax, String,  DateTime64),
  state                 AggregateFunction(argMax, String,  DateTime64),
  zip                   AggregateFunction(argMax, String,  DateTime64),
  country               AggregateFunction(argMax, String,  DateTime64),
  gmt_offset            AggregateFunction(argMax, Float64, DateTime64),
  tax_precentage        AggregateFunction(argMax, Float64, DateTime64),
  deleted               AggregateFunction(argMax, Boolean, DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_store_ch AS
SELECT * FROM (
  SELECT id,
         argMaxMerge(store_id)         AS store_id,
         argMaxMerge(rec_start_date)   AS rec_start_date,
         argMaxMerge(rec_end_date)     AS rec_end_date,
         argMaxMerge(closed_date_sk)   AS closed_date_sk,
         argMaxMerge(store_name)       AS store_name,
         argMaxMerge(number_employees) AS number_employees,
         argMaxMerge(floor_space)      AS floor_space,
         argMaxMerge(hours)            AS hours,
         argMaxMerge(manager)          AS manager,
         argMaxMerge(market_id)        AS market_id,
         argMaxMerge(geography_class)  AS geography_class,
         argMaxMerge(market_desc)      AS market_desc,
         argMaxMerge(market_manager)   AS market_manager,
         argMaxMerge(division_id)      AS division_id,
         argMaxMerge(division_name)    AS division_name,
         argMaxMerge(company_id)       AS company_id,
         argMaxMerge(company_name)     AS company_name,
         argMaxMerge(street_number)    AS street_number,
         argMaxMerge(street_name)      AS street_name,
         argMaxMerge(street_type)      AS street_type,
         argMaxMerge(suite_number)     AS suite_number,
         argMaxMerge(city)             AS city,
         argMaxMerge(county)           AS county,
         argMaxMerge(state)            AS state,
         argMaxMerge(zip)              AS zip,
         argMaxMerge(country)          AS country,
         argMaxMerge(gmt_offset)       AS gmt_offset,
         argMaxMerge(tax_precentage)   AS tax_precentage,
         argMaxMerge(deleted)          AS deleted
  FROM store_ch
  GROUP BY id
) WHERE NOT deleted;

CREATE TABLE date_dim_ch (
  id                    String,
  date_id               AggregateFunction(argMax, String,  DateTime64),
  d_date                AggregateFunction(argMax, Date32,  DateTime64),
  month_seq             AggregateFunction(argMax, Int32,   DateTime64),
  week_seq              AggregateFunction(argMax, Int32,   DateTime64),
  quarter_seq           AggregateFunction(argMax, Int32,   DateTime64),
  d_year                AggregateFunction(argMax, Int32,   DateTime64),
  dow                   AggregateFunction(argMax, Int32,   DateTime64),
  moy                   AggregateFunction(argMax, Int32,   DateTime64),
  dom                   AggregateFunction(argMax, Int32,   DateTime64),
  qoy                   AggregateFunction(argMax, Int32,   DateTime64),
  fy_year               AggregateFunction(argMax, Int32,   DateTime64),
  fy_quarter_seq        AggregateFunction(argMax, Int32,   DateTime64),
  fy_week_seq           AggregateFunction(argMax, Int32,   DateTime64),
  day_name              AggregateFunction(argMax, String,  DateTime64),
  quarter_name          AggregateFunction(argMax, String,  DateTime64),
  holiday               AggregateFunction(argMax, String,  DateTime64),
  weekend               AggregateFunction(argMax, String,  DateTime64),
  following_holiday     AggregateFunction(argMax, String,  DateTime64),
  first_dom             AggregateFunction(argMax, Int32,   DateTime64),
  last_dom              AggregateFunction(argMax, Int32,   DateTime64),
  same_day_ly           AggregateFunction(argMax, Int32,   DateTime64),
  same_day_lq           AggregateFunction(argMax, Int32,   DateTime64),
  current_day           AggregateFunction(argMax, String,  DateTime64),
  current_week          AggregateFunction(argMax, String,  DateTime64),
  current_month         AggregateFunction(argMax, String,  DateTime64),
  current_quarter       AggregateFunction(argMax, String,  DateTime64),
  current_year          AggregateFunction(argMax, String,  DateTime64),
  deleted               AggregateFunction(argMax, Boolean, DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_date_dim_ch AS
SELECT * FROM (
  SELECT id,
         argMaxMerge(date_id)           AS date_id,
         argMaxMerge(d_date)            AS d_date,
         argMaxMerge(month_seq)         AS month_seq,
         argMaxMerge(week_seq)          AS week_seq,
         argMaxMerge(quarter_seq)       AS quarter_seq,
         argMaxMerge(d_year)            AS d_year,
         argMaxMerge(dow)               AS dow,
         argMaxMerge(moy)               AS moy,
         argMaxMerge(dom)               AS dom,
         argMaxMerge(qoy)               AS qoy,
         argMaxMerge(fy_year)           AS fy_year,
         argMaxMerge(fy_quarter_seq)    AS fy_quarter_seq,
         argMaxMerge(fy_week_seq)       AS fy_week_seq,
         argMaxMerge(day_name)          AS day_name,
         argMaxMerge(quarter_name)      AS quarter_name,
         argMaxMerge(holiday)           AS holiday,
         argMaxMerge(weekend)           AS weekend,
         argMaxMerge(following_holiday) AS following_holiday,
         argMaxMerge(first_dom)         AS first_dom,
         argMaxMerge(last_dom)          AS last_dom,
         argMaxMerge(same_day_ly)       AS same_day_ly,
         argMaxMerge(same_day_lq)       AS same_day_lq,
         argMaxMerge(current_day)       AS current_day,
         argMaxMerge(current_week)      AS current_week,
         argMaxMerge(current_month)     AS current_month,
         argMaxMerge(current_quarter)   AS current_quarter,
         argMaxMerge(current_year)      AS current_year,
         argMaxMerge(deleted)           AS deleted
  FROM date_dim_ch
  GROUP BY id
) WHERE NOT deleted;

CREATE TABLE customer_ch (
  id                    String,
  customer_id           AggregateFunction(argMax, String,  DateTime64),
  current_cdemo_sk      AggregateFunction(argMax, Int32,   DateTime64),
  current_hdemo_sk      AggregateFunction(argMax, Int32,   DateTime64),
  current_addr_sk       AggregateFunction(argMax, Int32,   DateTime64),
  first_shipto_date_sk  AggregateFunction(argMax, Int32,   DateTime64),
  first_sales_date_sk   AggregateFunction(argMax, Int32,   DateTime64),
  salutation            AggregateFunction(argMax, String,  DateTime64),
  first_name            AggregateFunction(argMax, String,  DateTime64),
  last_name             AggregateFunction(argMax, String,  DateTime64),
  preferred_cust_flag   AggregateFunction(argMax, String,  DateTime64),
  birth_day             AggregateFunction(argMax, Int32,   DateTime64),
  birth_month           AggregateFunction(argMax, Int32,   DateTime64),
  birth_year            AggregateFunction(argMax, Int32,   DateTime64),
  birth_country         AggregateFunction(argMax, String,  DateTime64),
  login                 AggregateFunction(argMax, String,  DateTime64),
  email_address         AggregateFunction(argMax, String,  DateTime64),
  last_review_date_sk   AggregateFunction(argMax, Int32,   DateTime64),
  deleted               AggregateFunction(argMax, Boolean, DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_customer_ch AS
SELECT * FROM (
  SELECT id,
         argMaxMerge(customer_id)          AS customer_id,
         argMaxMerge(current_cdemo_sk)     AS current_cdemo_sk,
         argMaxMerge(current_hdemo_sk)     AS current_hdemo_sk,
         argMaxMerge(current_addr_sk)      AS current_addr_sk,
         argMaxMerge(first_shipto_date_sk) AS first_shipto_date_sk,
         argMaxMerge(first_sales_date_sk)  AS first_sales_date_sk,
         argMaxMerge(salutation)           AS salutation,
         argMaxMerge(first_name)           AS first_name,
         argMaxMerge(last_name)            AS last_name,
         argMaxMerge(preferred_cust_flag)  AS preferred_cust_flag,
         argMaxMerge(birth_day)            AS birth_day,
         argMaxMerge(birth_month)          AS birth_month,
         argMaxMerge(birth_year)           AS birth_year,
         argMaxMerge(birth_country)        AS birth_country,
         argMaxMerge(login)                AS login,
         argMaxMerge(email_address)        AS email_address,
         argMaxMerge(last_review_date_sk)  AS last_review_date_sk,
         argMaxMerge(deleted)              AS deleted
  FROM customer_ch
  GROUP BY id
) WHERE NOT deleted;

CREATE TABLE item_ch (
  id                    String,
  item_id               AggregateFunction(argMax, String,  DateTime64),
  rec_start_date        AggregateFunction(argMax, Date32,  DateTime64),
  rec_end_date          AggregateFunction(argMax, Date32,  DateTime64),
  item_desc             AggregateFunction(argMax, String,  DateTime64),
  current_price         AggregateFunction(argMax, Float64, DateTime64),
  wholesale_cost        AggregateFunction(argMax, Float64, DateTime64),
  brand_id              AggregateFunction(argMax, Int32,   DateTime64),
  brand                 AggregateFunction(argMax, String,  DateTime64),
  class_id              AggregateFunction(argMax, Int32,   DateTime64),
  i_class               AggregateFunction(argMax, String,  DateTime64),
  category_id           AggregateFunction(argMax, Int32,   DateTime64),
  category              AggregateFunction(argMax, String,  DateTime64),
  manufact_id           AggregateFunction(argMax, Int32,   DateTime64),
  manufact              AggregateFunction(argMax, String,  DateTime64),
  i_size                AggregateFunction(argMax, String,  DateTime64),
  formulation           AggregateFunction(argMax, String,  DateTime64),
  color                 AggregateFunction(argMax, String,  DateTime64),
  units                 AggregateFunction(argMax, String,  DateTime64),
  container             AggregateFunction(argMax, String,  DateTime64),
  manager_id            AggregateFunction(argMax, Int32,   DateTime64),
  product_name          AggregateFunction(argMax, String,  DateTime64),
  deleted               AggregateFunction(argMax, Boolean, DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_item_ch AS
SELECT * FROM (
  SELECT id,
         argMaxMerge(item_id)        AS item_id,
         argMaxMerge(rec_start_date) AS rec_start_date,
         argMaxMerge(rec_end_date)   AS rec_end_date,
         argMaxMerge(item_desc)      AS item_desc,
         argMaxMerge(current_price)  AS current_price,
         argMaxMerge(wholesale_cost) AS wholesale_cost,
         argMaxMerge(brand_id)       AS brand_id,
         argMaxMerge(brand)          AS brand,
         argMaxMerge(class_id)       AS class_id,
         argMaxMerge(i_class)        AS i_class,
         argMaxMerge(category_id)    AS category_id,
         argMaxMerge(category)       AS category,
         argMaxMerge(manufact_id)    AS manufact_id,
         argMaxMerge(manufact)       AS manufact,
         argMaxMerge(i_size)         AS i_size,
         argMaxMerge(formulation)    AS formulation,
         argMaxMerge(color)          AS color,
         argMaxMerge(units)          AS units,
         argMaxMerge(container)      AS container,
         argMaxMerge(manager_id)     AS manager_id,
         argMaxMerge(product_name)   AS product_name,
         argMaxMerge(deleted)        AS deleted
  FROM item_ch
  GROUP BY id
) WHERE NOT deleted;
