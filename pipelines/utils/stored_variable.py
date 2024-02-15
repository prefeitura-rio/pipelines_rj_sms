# -*- coding: utf-8 -*-
import os
import pickle
import uuid


class StoredVariableReference:
    """
    Represents a reference to a local stored variable.

    Attributes:
        uuid (uuid.UUID): The unique identifier for the stored variable reference.
        file_path (str): The file path where the variable is stored.
    """

    def __init__(self, value, directory=""):
        """
        Initializes a new instance of the StoredVariableReference class.

        Args:
            value: The value to be stored.
            directory (str, optional): The directory where the variable should be stored.
        """
        self.uuid = uuid.uuid4()
        self.file_path = os.path.join(directory, f"{self.uuid}.pkl")
        self.save(value)

    def save(self, value):
        """
        Saves the value to the file.

        Args:
            value: The value to be saved.
        """
        with open(self.file_path, "wb") as file:
            pickle.dump(value, file)

    def load(self):
        """
        Loads the value from the file.

        Returns:
            The loaded value.
        """
        with open(self.file_path, "rb") as file:
            return pickle.load(file)

    def set(self, value):
        """
        Sets the value and saves it to the file.

        Args:
            value: The value to be set.
        """
        self.save(value)

    def get(self):
        """
        Gets the value from the file.

        Returns:
            The stored value.
        """
        return self.load()

    def __del__(self):
        """
        Deletes the stored variable file.
        """
        try:
            os.remove(self.file_path)
        except FileNotFoundError:
            pass  # File has already been removed
