# Install the latest version of snowflake-connector-python 3.12.3
# pip install snowflake-connector-python

import snowflake.connector as sc


class SnowflakeConnector:

    def __init__(self, account, user, password, warehouse, database, schema, role):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role

    def connect(self):
        """
        Creating a Connection to the snowFlake.
        :return:
        """
        self.connection = sc.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema
        )
        print("Connection Established")

        # Defining a cursor
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        """
        Executing a Snowflake Query
        :param query: A snowflake query.
        :return: result of the query.
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close_connection(self):
        """
        Closing Cursor and Connection.
        :return:
        """
        self.cursor.close()
        self.connection.close()
