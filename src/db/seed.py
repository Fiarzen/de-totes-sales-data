from src.db.connection import connect_to_test_db


def seed_db():
    db = connect_to_test_db()
    db.run("DROP TABLE if exists fact_sales_order")
    db.run("DROP TABLE if exists dim_date")
    db.run("DROP TABLE if exists dim_staff")
    db.run("DROP TABLE if exists dim_location")
    db.run("DROP TABLE if exists dim_currency")
    db.run("DROP TABLE if exists dim_design")
    db.run("DROP TABLE if exists dim_counterparty")

    db.run(
        'CREATE TABLE fact_sales_order (\
        sales_record_id SERIAL PRIMARY KEY,\
        sales_order_id INT, \
        created_date DATE, \
        created_time TIME, \
        last_updated_date DATE, \
        last_updated_time TIME, \
        sales_staff_id INT, \
        counterparty_id INT, \
        units_sold INT, \
        unit_price NUMERIC(10, 2),\
        currency_id INT, \
        design_id INT, \
        agreed_payment_date DATE, \
        agreed_delivery_date DATE, \
        agreed_delivery_location_id INT\
        )'
    )

    db.run(
        'CREATE TABLE dim_date (\
        date_id DATE PRIMARY KEY,\
        year INT,\
        month INT,\
        day INT,\
        day_of_week INT,\
        day_name VARCHAR,\
        month_name VARCHAR,\
        quarter INT\
        )'
    )

    db.run(
        'CREATE TABLE dim_staff (\
        staff_id INT PRIMARY KEY,\
        first_name VARCHAR,\
        last_name VARCHAR,\
        department_name VARCHAR,\
        location VARCHAR,\
        email_address VARCHAR\
        )'
    )

    db.run(
        'CREATE TABLE dim_location (\
        location_id INT PRIMARY KEY,\
        address_line_1 VARCHAR,\
        address_line_2 VARCHAR,\
        district VARCHAR,\
        city VARCHAR,\
        postal_code VARCHAR,\
        country VARCHAR,\
        phone VARCHAR\
        )'
    )

    db.run(
        'CREATE TABLE dim_currency (\
        currency_id INT PRIMARY KEY,\
        currency_code VARCHAR,\
        currency_name VARCHAR\
        )'
    )

    db.run(
        'CREATE TABLE dim_design (\
        design_id INT PRIMARY KEY,\
        design_name VARCHAR,\
        file_location VARCHAR,\
        file_name VARCHAR\
        )'
    )

    db.run(
        'CREATE TABLE dim_counterparty (\
        counterparty_id INT PRIMARY KEY,\
        counterparty_legal_name VARCHAR,\
        counterparty_legal_address_line_1 VARCHAR,\
        counterparty_legal_address_line_2 VARCHAR,\
        counterparty_legal_district VARCHAR,\
        counterparty_legal_city VARCHAR,\
        counterparty_legal_postal_code VARCHAR,\
        counterparty_legal_country VARCHAR,\
        counterparty_legal_phone_number VARCHAR\
        )'
    )
    db.close()
