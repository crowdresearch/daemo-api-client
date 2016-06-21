class Error:
    def __init__(self):
        pass

    def func_def_undefined(self, error):
        return "No definition for %s function found." % error

    def unauthenticated(self):
        return "Permission denied. Please authenticate first!"

    def missing_connection(self):
        return "No connection exists. Please try again."

    def required(self, param):
        return 'Field %s is missing!' % param
