import collections, json, os

class Set(collections.abc.MutableMapping):
    """
    The KeyValueMetadata class handles acts as a wrapper to a datasets metadata
    of a given specification to create a simple key-value store. It can be used
    like a dict object (i.e. kvm["key"] = value) but only strings are accepted
    as keys. Current values will be read from the dataset store on instance 
    creation time and can be read again using the reload() method. 
    """
    def __init__(self, dataset, spec_id):
        super().__init__()
        self._dataset = dataset
        self._spec_id = spec_id
        self._dictionary = None
        self.reload()

    @property
    def spec_id(self):
        return self._spec_id

    @property
    def path(self):
        """
        Returns the path to the JSON file containing the metadata set compliant
        with the given metadata specification identifier for this dataset. This
        method will always return a path, even if no file exists at that 
        location.
        """
        meta_dir = os.path.join(self._dataset._path, "meta")
        os.makedirs(meta_dir, exist_ok=True)
        json_path = os.path.join(meta_dir, f"{self._spec_id}.json")
        return json_path

    def reload(self):
        """
        Discard all changes and load the current values stored on disk inside
        the dataset. Automatically called during initialization.
        """
        if os.path.exists(self.path):
            f = open(self.path, "r")
            self._dictionary = json.load(f)
            f.close
        else:
            self._dictionary = {}
        return self._dictionary

    def save(self):
        """
        Write any changes you made back to the underlaying dataset. Called
        automatically on each update. Call this method if you directly 
        manipulate the _dictionary property for any reason.
        """
        if self._dictionary:
            with open(self.path, "w+") as f:
                json.dump(self._dictionary, f)
        else:
            # Delete the file instead of storing an empty dictionary
            os.remove(self.path)
        return None
                
    def __getitem__(self, key):
        return self._dictionary[key]
        
    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise TypeError('Keys must be strings')
        self._dictionary.__setitem__(key, value)
        self.save()
        return None
        
    def __delitem__(self, key):
        self.save()
        self._dictionary.__delitem__(key)

    def __iter__(self):
        return self._dictionary.__iter__()
    
    def __len__(self):
        return self._dictionary.__len__()
        
    def __repr__(self):
        return f"<{self.__class__.__name__} {self._dictionary}>"