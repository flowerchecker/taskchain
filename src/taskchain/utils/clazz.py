def persistent(method):
    def persist(self):
        attr = f'_{method.__name__}'
        if not hasattr(self, attr) or getattr(self, attr) is None:
            setattr(self, attr, method(self))
        return getattr(self, attr)
    return persist
