from pyflink.table.catalog import JdbcCatalog


class HelperMethods:
    """
    helper class
    """
    def __init__(self, table_env):
        self.table_env = table_env


    def set_catalog(self, catlog_name, jdbc_obj):
        """
        accepts jdbc catalog params and set a catalog
        """
        catalog = JdbcCatalog(catlog_name, jdbc_obj.database, jdbc_obj.username,
                              jdbc_obj.password, jdbc_obj.base_url)
        self.table_env.register_catalog(catlog_name, catalog)


    def read_customer(self):
        """
        read customers from postgres catalog,
        filter only active customers and drop store id
        """
        customer = self.table_env.from_path('customer')
        active_customer = customer \
                          .filter(customer.activebool == True) \
                          .rename_columns(customer.address_id.alias('address_id_ref')) \
                          .drop_columns('store_id')
        return active_customer


    def read_address(self):
        """
        read paddress
        """
        address = self.table_env.from_path('address')
        address = address \
                  .rename_columns(address.city_id.alias('city_id_ref')) \
                  .drop_columns('last_update')
        return address


    def read_city(self):
        """
        read city details
        """
        city = self.table_env.from_path('city')
        city = city \
               .rename_columns(city.country_id.alias('country_id_ref')) \
               .drop_columns('last_update')
        return city


    def read_country(self):
        """
        read country
        """
        country = self.table_env.from_path('country')
        country = country.drop_columns('last_update')
        return country


    def denorm_customers(self):
        """
        join dataframes together
        """
        active_customer = self.read_customer()
        address = self.read_address()
        city = self.read_city()
        country = self.read_country()

        customer_denorm = active_customer \
                          .join(address) \
                          .where(active_customer.address_id_ref == address.address_id) \
                          .join(city) \
                          .where(address.city_id_ref == city.city_id) \
                          .join(country) \
                          .where(city.country_id_ref == country.country_id)

        return customer_denorm


    def agg_customers(self):
        """
        aggregate customers
        """
        customer_denorm = self.denorm_customers()
        result = customer_denorm \
                 .group_by(customer_denorm.country_id, customer_denorm.city_id) \
                 .select(customer_denorm.country_id, customer_denorm.city_id,
                         customer_denorm.customer_id.count)

        return result


    def create_sink(self, stage_table_name, jdbc_obj):
        """
        define the postgres sink
        """
        sink = f"""
                CREATE TABLE {stage_table_name} (
                  country_id INT,
                  city_id INT,
                  total_count BIGINT,
                  PRIMARY KEY (country_id, city_id) NOT ENFORCED
                ) WITH (
                   'connector' = 'jdbc',
                   'url' = '{jdbc_obj.base_url}',
                   'table-name' = 'country_city_agg',
                   'username' = '{jdbc_obj.username}',
                   'password' = '{jdbc_obj.password}'
                )
                """
        self.table_env.execute_sql(sink)
