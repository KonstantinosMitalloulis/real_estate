--CREATE DIMENSION TABLES
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_german_geography') THEN
        CREATE TABLE real_estate.dim_german_geography (
            postal_code varchar(40) ,
            lat real,
            lng real ,
            german_state varchar(1000),
            city varchar(1000) ,
            primary key (postal_code,lat,lng,german_state,city)
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_publisher_name') THEN
        CREATE TABLE real_estate.dim_publisher_name (
            publisher_name_id SERIAL PRIMARY KEY,
            publisher_name varchar(140) UNIQUE
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_floor') THEN
        CREATE TABLE real_estate.dim_floor (
            floor_id SERIAL PRIMARY KEY,
            no_floor varchar(140) UNIQUE
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_energy_class') THEN
        CREATE TABLE real_estate.dim_energy_class (
            energy_class_id SERIAL PRIMARY KEY,
            energy_class varchar(140) UNIQUE
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_property_type') THEN
        CREATE TABLE real_estate.dim_property_type (
            property_type_id SERIAL PRIMARY KEY,
            property_type varchar(140) UNIQUE
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_apartment_house_type') THEN
        CREATE TABLE real_estate.dim_apartment_house_type (
            apartment_house_type_id SERIAL PRIMARY KEY,
            apartment_house_type varchar(140) UNIQUE
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_publisher_type') THEN
        CREATE TABLE real_estate.dim_publisher_type (
            publisher_type_id SERIAL PRIMARY KEY,
            publisher_type varchar(140) UNIQUE
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_city') THEN
        CREATE TABLE real_estate.dim_city (
            city_id SERIAL PRIMARY KEY,
            city varchar(140) UNIQUE
        );
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_german_state') THEN
        CREATE TABLE real_estate.dim_german_state (
            german_state_id SERIAL PRIMARY KEY,
            german_state varchar(140) UNIQUE
        );
    END IF;
END $$;

--staging_table

DROP TABLE IF EXISTS real_estate.staging_table;
create table real_estate.staging_table(
online_id varchar(40) primary key ,
webpage varchar(40),
property_type varchar(40),
delivery_time timestamp,
energy_consumption real,
energy_class varchar(40),
sqm_plot real,
sqm_property real,
postal_code int ,
price real,
no_rooms varchar(40),
no_floor varchar(40),
publisher_type varchar(40),
apartment_house_type varchar(40),
construction_year int,
publisher_name varchar(140),
german_state varchar(40),
city varchar(80));

--CREATE FACT TABLE
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'fact_table') THEN
        CREATE TABLE real_estate.fact_table (
            online_id varchar(40) PRIMARY KEY,
            delivery_time timestamp,
            webpage varchar(40),
            energy_consumption real,
            sqm_property REAL,
            sqm_plot REAL,
            price real,
            postal_code VARCHAR(40),
            energy_class_id int REFERENCES real_estate.dim_energy_class(energy_class_id),
            no_rooms varchar(40),
            floor_id int REFERENCES real_estate.dim_floor(floor_id),
            publisher_type_id int REFERENCES real_estate.dim_publisher_type(publisher_type_id),
            apartment_house_type_id int REFERENCES real_estate.dim_apartment_house_type(apartment_house_type_id),
            property_type_id int REFERENCES real_estate.dim_property_type(property_type_id),
            construction_year int,
            publisher_name_id int REFERENCES real_estate.dim_publisher_name(publisher_name_id),
            german_state_id int REFERENCES real_estate.dim_german_state(german_state_id),
            city_id int REFERENCES real_estate.dim_city(city_id)
        );
    END IF;
END $$;
