"""
This module is for defining custom exceptions.
These custom exceptions can be based business use case. 
"""
class FileNotFoundException(Exception):
    def __init__(self, message):
        super().__init__(message)