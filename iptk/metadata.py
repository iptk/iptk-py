import collections, json, os

class KeyValueMetadata(collections.abc.MutableMapping):
    """
    The KeyValueMetadata class handles acts as a wrapper to a datasets metadata
    of a given specification to create a simple key-value store. It can be used
    like a dict object (i.e. kvm["key"] = value) but only strings are accepted
    as keys and values can only be strings, floats, integers, boolean values,
    or one-dimensional arrays of one of this types. Current values will be read
    from the dataset store on instance creation time and can be read again
    using the reload() method. You must explicitly call save() to write any 
    changes back to the store.
    """
    def __init__(self, dataset, spec):
        super().__init__()
        self.dataset = dataset
        self.spec = spec
        self.dictionary = None
        self.reload()

    def reload(self):
        """
        Discard all changes and load the current values stored on disk inside
        the dataset. Automatically called during initialization.
        """
        self.dictionary = self.dataset.get_metadata(self.spec.identifier)

    def save(self):
        """
        Write any changes you made back to the underlaying dataset.
        """
        self.dataset.set_metadata(self.spec.identifier, self.dictionary)
        self.reload()
        
    def __getitem__(self, key):
        return self.dictionary[key]
        
    def __setitem__(self, key, value):
        allowed_types = (str, bool, float, int)
        if not isinstance(key, str):
            raise TypeError('Keys must be strings')
        if isinstance(value, list):
            last_type = None
            for x in value:
                x_type = type(x)
                if not isinstance(x, allowed_types):
                    raise TypeError(f"Values must be {allowed_types}, not {x_type}")
                if last_type and last_type != x_type:
                    raise TypeError(f"All list elements must be of the same type")
                last_type = x_type
        elif not isinstance(value, allowed_types):
            raise TypeError(f"Value must be {allowed_types} or one-dimensional list thereof")
        return self.dictionary.__setitem__(key, value)
        
    def __delitem__(self, key):
        return self.dictionary.__delitem__(key)

    def __iter__(self):
        return self.dictionary.__iter__()
    
    def __len__(self):
        return self.dictionary.__len__()
        
    def __repr__(self):
        return f"<{self.__class__.__name__} {self.dictionary}>"