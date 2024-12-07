from src.utils.snowUtils import SnowflakeConnector

account = "AQWHMHL-CF83888"
user = "AMANMOTGHARE"
password = "Roomvi@12345"
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCH_SF1"
role = "ACCOUNTADMIN"
warehouse = "COMPUTE_WH"

sf_connector = SnowflakeConnector(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
    database=database,
    schema=schema,
    role=role
)

# Establishing a connection
sf_connector.connect()

# Sending a query
query = """
SELECT
    *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER
WHERE S_SUPPKEY <= 20
"""

result = sf_connector.execute_query(query)

for record in result:
    print(record)
