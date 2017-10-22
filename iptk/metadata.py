import collections, json, os

class Metadata(object):
    """
    Each instance of the Metadata class represents the concrete Metadata of a
    given metadata specification attached to a single dataset.
    """
    def __init__(self, dataset, spec):
        super(Metadata, self).__init__()
        self.dataset = dataset
        self.spec = spec
        self.initialize()
        
    @property
    def path(self):
        return os.path.join(self.dataset.path, "meta", self.spec.identifier)

    def initialize(self):
        os.makedirs(self.path, exist_ok=True)
    
class KeyValueMetadata(Metadata, collections.abc.MutableMapping):
    """
    The KeyValueMetadata class handles acts as a wrapper to a datasets metadata
    of a given specification to create a simple key-value store. It can be used
    like a dict object (i.e. kvm["key"] = value) but only strings are accepted
    as keys and values can only be strings, floats, integers, boolean values,
    or one-dimensional arrays of one of this types. These values are stored in
    a file called iptk-kv.json within the metadata directory. Current values
    will be read from the file on instance creation time and can be read again
    using the reload() method. You must explicitly call save() to write any 
    changes back to disk.
    """
    def __init__(self, *args, **kwargs):
        super(KeyValueMetadata, self).__init__(*args, **kwargs)
        self.dictionary = None
        self.reload()
        
    @property
    def file_path(self):
        return os.path.join(self.path, "iptk-kv.json")
    
    def reload(self):
        self.dictionary = {}
        if not os.path.exists(self.file_path):
            return
        with open(self.file_path, "r") as f:
            self.dictionary = json.load(f)
    
    def save(self):
        with open(self.file_path, "w") as f:
            json.dump(self.dictionary, f, sort_keys=True, indent=4, separators=(',', ': '))
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