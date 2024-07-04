DELETE FROM real_estate.staging_table
WHERE online_id IN (SELECT online_id FROM real_estate.fact_table);

----ALL INSERTS INTO DIMENSION TABLES -------
-------dim_publisher_name------
INSERT INTO real_estate.dim_publisher_name (publisher_name)
SELECT DISTINCT publisher_name
FROM real_estate.staging_table
ON CONFLICT (publisher_name) DO NOTHING;
--------dim_floor------
INSERT INTO real_estate.dim_floor (no_floor)
SELECT DISTINCT no_floor
FROM real_estate.staging_table
ON CONFLICT (no_floor) DO NOTHING;
-----------dim_energy_class--------------

INSERT INTO real_estate.dim_energy_class (energy_class)
SELECT DISTINCT energy_class
FROM real_estate.staging_table
ON CONFLICT (energy_class) DO NOTHING;
--------dim_property_type---------
INSERT INTO real_estate.dim_property_type (property_type)
SELECT DISTINCT property_type
FROM real_estate.staging_table
ON CONFLICT (property_type) DO NOTHING;
-----dim_apartment_house_type----
INSERT INTO real_estate.dim_apartment_house_type (apartment_house_type)
SELECT DISTINCT apartment_house_type
FROM real_estate.staging_table
ON CONFLICT (apartment_house_type) DO NOTHING;
------dim_publisher_type----
INSERT INTO real_estate.dim_publisher_type (publisher_type)
SELECT DISTINCT publisher_type
FROM real_estate.staging_table
ON CONFLICT (publisher_type) DO NOTHING;

------dim_german_state----
INSERT INTO real_estate.dim_german_state (german_state)
SELECT DISTINCT german_state
FROM real_estate.staging_table
ON CONFLICT (german_state) DO NOTHING;

------dim_city----
INSERT INTO real_estate.dim_city (city)
SELECT DISTINCT city
FROM real_estate.staging_table
ON CONFLICT (city) DO NOTHING;


INSERT INTO real_estate.fact_table (
    online_id,
    delivery_time,
    webpage,
    energy_consumption,
    sqm_property,
    sqm_plot,
    price,
    postal_code,
    energy_class_id,
    no_rooms,
    floor_id,
    publisher_type_id,
    apartment_house_type_id,
    property_type_id,
    construction_year,
    publisher_name_id,
    german_state_id,
    city_id
)
SELECT 
    st.online_id,
    st.delivery_time,
    st.webpage,
    st.energy_consumption,
    st.sqm_property,
    st.sqm_plot,
    st.price,
    st.postal_code,
    dec.energy_class_id,
    st.no_rooms,
    df.floor_id,
    dpt.publisher_type_id,
    dah.apartment_house_type_id,
    dptp.property_type_id,
    st.construction_year,
    dpn.publisher_name_id,
    gs.german_state_id,
    ct.city_id
FROM 
    real_estate.staging_table st
JOIN 
    real_estate.dim_energy_class dec ON st.energy_class = dec.energy_class
JOIN 
    real_estate.dim_floor df ON st.no_floor = df.no_floor
JOIN 
    real_estate.dim_publisher_type dpt ON st.publisher_type = dpt.publisher_type
JOIN 
    real_estate.dim_apartment_house_type dah ON st.apartment_house_type = dah.apartment_house_type
JOIN 
    real_estate.dim_property_type dptp ON st.property_type = dptp.property_type
JOIN 
    real_estate.dim_publisher_name dpn ON st.publisher_name = dpn.publisher_name
JOIN real_estate.dim_german_state gs ON st.german_state = gs.german_state
JOIN real_estate.dim_city ct ON st.city = ct.city ;





