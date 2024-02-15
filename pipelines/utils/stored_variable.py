# -*- coding: utf-8 -*-
import os
import pickle
from typing import Literal
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
        self.file_path = os.path.join(directory, f"{self.uuid}.storedvar")
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
            pass



def stored_variable_converter(
    output_mode: Literal['transform', 'auto', 'original'] = 'auto'
):
    """
    A decorator that wraps a function and allows the use of stored variables. StoredVariables are
        lightweight references to files. They are useful for storing large objects in the disk and
        not in memory during Prefect flow executions.

    Args:
        transform_output (bool, optional): Indicates whether the output of the function should
            be transformed into a stored variable.
        allowed_transformations (list[str], optional): A list of allowed transformations.
        autodetect (bool, optional): If true, the previous parameters are automatically set based
            on the following criteria: 
            - Transformation: all parameters of type StoredVariableReference will be transformed. 
            - Output: if at least one parameter is StoredVariableReference, the output will be in
            form of a StoredVariableReference.

    Example usage:
        @stored_variable_wrapper(transform_output=True, allowed_transformations=['var1'])
        def my_function(var1, var2):
            # Function implementation
            return result
    """
    def decorator(func):
        def wrapper(**kwargs):
            is_using_stored_variable = False

            # Translate
            for key, value in kwargs.items():
                if isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, StoredVariableReference):
                            kwargs[key][i] = item.get()
                            is_using_stored_variable = True
                elif isinstance(value, StoredVariableReference):
                    kwargs[key] = value.get()
                    is_using_stored_variable = True

            output = func(**kwargs)

            if (output_mode == 'transform') or \
                (output_mode == 'auto' and is_using_stored_variable):
                if isinstance(output, list):
                    for i, item in enumerate(output):
                        output[i] = StoredVariableReference(item)
                else:
                    output = StoredVariableReference(output)
            return output
        return wrapper
    return decorator
