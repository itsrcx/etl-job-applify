class DBConfig:
    """Abstract base class for database configuration."""
    def get_connection_string(self):
        raise NotImplementedError("This method should be overridden in the subclass")
    
class MySQLConfig(DBConfig):
    """MySQL configuration class."""
    def __init__(self, server, database, user, password, driver="{MySQL ODBC 9.1 Unicode Driver}"):
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver

    def get_connection_string(self):
        return f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password};"

class PostgreSQLConfig(DBConfig):
    """PostgreSQL configuration class."""
    def __init__(self, server, database, user, password, driver="{PostgreSQL Unicode}"):
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.driver = driver

    def get_connection_string(self):
        return f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password};"
