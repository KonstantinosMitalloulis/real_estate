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
publisher_name varchar(140));