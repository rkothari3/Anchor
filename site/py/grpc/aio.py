class Metadata(dict):
    pass


class AioRpcError(Exception):
    def __init__(self, code, initial_metadata=None, trailing_metadata=None):
        super().__init__(str(code))
        self._code = code

    def code(self):
        return self._code
