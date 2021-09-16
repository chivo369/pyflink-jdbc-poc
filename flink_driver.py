from pyflink.table import EnvironmentSettings, BatchTableEnvironment

from customer_denorm import CustomerDenorm


# create a flink table env in batch mode
# ------------------------------------------------------------
env_settings = EnvironmentSettings \
               .new_instance() \
               .in_batch_mode() \
               .use_blink_planner() \
               .build()

table_env = BatchTableEnvironment \
            .create(environment_settings=env_settings)

# create CustomerDenorm object
# ------------------------------------------------------------
cust_denorm = CustomerDenorm(table_env)

# run the etl code
# ------------------------------------------------------------
try:
    cust_denorm.process_job()
except Exception as e:
    print(e)
