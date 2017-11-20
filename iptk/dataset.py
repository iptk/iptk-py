import os, re, json, shutil

class Dataset(object):
    """
    The Dataset class is used to represent an IPTK dataset on disk. It provides
    a thin abstraction layer for many commonly used functions. Please note that
    according to the IPTK specification, the name of the dataset folder must be
    a valid IPTK identifier. This is enforced by this class.
    """
    def __init__(self, path, create_ok=False):
        super().__init__()
        identifier = os.path.basename(path)
        if not re.match("^[0-9a-z]{40}$", identifier):
            raise ValueError('Invalid dataset identifier')
        if create_ok:
            os.makedirs(path, exist_ok=True)
        if not os.path.exists(path):
            raise ValueError(f'Path {path} does not exist')
        self.identifier = identifier
        self.path = path

    @property
    def data_dir(self):
        """
        Return the path to the data/ subfolder of this dataset. The folder will
        be created if it does not exist.
        """
        path = os.path.join(self.path, 'data')
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def lock_dir(self):
        return os.path.join(self.path, 'lock')

    @property
    def is_locked(self):
        """
        Returns whether this dataset is locked. Locked datasets cannot be
        manipulated.
        """
        return os.path.exists(self.lock_dir)

    def lock(self):
        """
        Locks the dataset. Locked datasets can be used as job inputs but the
        content of their data/ directory must remain unchanged. A locked 
        dataset is indicated by the existence of a lock/ subdirectory. 
        Unlocking a dataset by deleting lock/ is not allowed and may lead to 
        unpleasant side effects. Locking a locked dataset is a no-op.
        """
        if not self.is_locked:
            os.makedirs(self.lock_dir, exist_ok=True)

    def metadata_path(self, spec_id):
        """
        Returns the path to the JSON file containing the metadata set compliant
        with the given metadata specification identifier for this dataset. This
        method will always return a path, even if no file exists at that 
        location.
        """
        meta_path = os.path.join(self.path, "meta")
        if not os.path.exists(meta_path):
            os.makedirs(meta_path, exist_ok=True)
        json_path = os.path.join(meta_path, f"{metadata_id}.json")
        return json_path

    def get_metadata(self, spec_id):
        """
        Read the metadata of this dataset for the given metadata specification
        identifier. Returns an empty dictionary if no metadata has been set for
        the identifier.
        """
        path = self.metadata_path(spec_id)
        if not os.path.exists(path):
            return {}
        with open(path, "r") as f:
            dictionary = json.load(f)
        return dictionary

    def set_metadata(self, spec_id, data):
        """
        Set the metadata to store for a specified metadata specification. This
        method will create a new metadata set if none existed before.
        
        Args:
            spec_id (str): Metadata specification identifier. Must be a valid
        IPTK identifier.
            data (dict): The data to store in the metadata set. Must be a 
        JSON-serializable dictionary.
        """
        path = self.metadata_path(spec_id)
        with open(path, "w") as f:
            json.dump(data, f)
        return data

    def archive(self):
        """
        Creates an archive of this dataset, including metadata. The returned
        object is a generator that can be iterated over to create the complete
        archive.
        """
        return None

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.identifier}>"
        