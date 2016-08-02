class Error:
    def __init__(self):
        pass

    @staticmethod
    def func_def_undefined(error):
        return "No definition for %s function found." % error

    @staticmethod
    def unauthenticated():
        return "Permission denied. Please authenticate first!"

    @staticmethod
    def missing_connection():
        return "No connection exists. Please try again."

    @staticmethod
    def required(param):
        return 'Field %s is missing!' % param
