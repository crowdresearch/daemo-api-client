"""Daemo Exception Classes
Includes two main exceptions: ``ServerException`` for when something goes wrong
on the server side, and ``ClientException`` when something goes wrong on the
client side. Both of these classes extend ``DaemoException``.
"""


class DaemoException(Exception):
    """The base exception that all other exception classes extend"""


class ServerException(DaemoException):
    """Indicate exception that involve responses from Daemo API"""

    def __init__(self, context, code, message):
        """Construct an APIException.
        :param error_type: the type of error on daemo server end
        :param message: the error message
        :param field: field if any associated with error
        """
        error_str = '[{}] {}'.format(context, message)
        super(ServerException, self).__init__(error_str)
        self.context = context
        self.code = code
        self.message = message


class ClientException(DaemoException):
    """Indicate exceptions that don't involve interaction with Daemo API"""
