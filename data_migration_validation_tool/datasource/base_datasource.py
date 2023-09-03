class BaseDatasource:
    """
    A class to represent data source.

    Attributes:
        name (str): The name of the data source.
        type (str): The type of data source.

    Methods:
        load_data(self): Loads the data from the data source.
    """

    def __init__(self, name, type):
        self.name = name
        self.type = type

    def load_data(self):
        """Loads the data from the data source."""
        raise NotImplementedError("This method must be implemented by subclasses.")
