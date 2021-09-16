from helper_methods import HelperMethods
from database import SourceDatabase, TargetDatabase


class CustomerDenorm:
    def __init__(self, table_env):
        self.table_env = table_env
        self.help_obj = HelperMethods(self.table_env)
        self.jdbc_src_obj = SourceDatabase()
        self.jdbc_tar_obj = TargetDatabase()
        self.catalog_name = 'dvdrental'
        self.stage_table_name = 'country_city_agg_stage'


    def process_job(self):
        """
        process code
        """
        # set the catalog
        self.help_obj.set_catalog(self.catalog_name, self.jdbc_src_obj)
        self.table_env.use_catalog(self.catalog_name)

        # read the target
        agg_customers = self.help_obj.agg_customers()

        # set to def catalog
        self.table_env.use_catalog('default_catalog')

        # create target table
        self.help_obj.create_sink(self.stage_table_name, self.jdbc_tar_obj)

        # write to the target table
        agg_customers.execute_insert(self.stage_table_name).wait()
