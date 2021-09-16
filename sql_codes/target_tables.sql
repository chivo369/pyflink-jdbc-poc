/*
postgresql target table ddl
*/

create table country_city_agg (
		country_id integer,
		city_id integer,
		total_count integer,
		primary key(country_id, city_id)
)
