class SourceDatabase:

    def __init__(self):
        self.database = 'dvdrental'
        self.username = 'postgres'
        self.password = 'postgres'
        self.host = 'localhost'
        self.port = 5432
        self.base_url = f"jdbc:postgresql://{self.host}:{self.port}/"


class TargetDatabase:

    def __init__(self):
        self.database = 'postgres'
        self.username = 'postgres'
        self.password = 'postgres'
        self.host = 'localhost'
        self.port = 5432
        self.base_url = f"jdbc:postgresql://{self.host}:{self.port}/"
