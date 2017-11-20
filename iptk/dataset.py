import os, re, shutil

class Dataset(object):
    """
    The Dataset class is used to represent an IPTK dataset on disk. It provides
    a thin abstraction layer for many commonly used functions. Please note that
    according to the IPTK specification, the name of the dataset folder must be
    a valid IPTK identifier. This is enforced by this class.
    """
    def __init__(self, path, create_ok=False):
        super().__init__()
        identifier = os.path.dirname(path)
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
        return os.path.join(self.path, 'data')

    @property
    def meta_dir(self):
        return os.path.join(self.path, 'meta')

    @property
    def lock_dir(self):
        return os.path.join(self.path, 'lock')

    @property
    def is_locked(self, dataset):
        """
        Returns whether this dataset is locked. Locked datasets cannot be
        manipulated.
        """
        return os.path.exists(self.lock_dir)

    def lock(self, dataset):
        """
        Locks the dataset. Locked datasets can be used as job inputs but the
        content of their data/ directory must remain unchanged. A locked 
        dataset is indicated by the existence of a lock/ subdirectory. 
        Unlocking a dataset by deleting lock/ is not allowed and may lead to 
        unpleasant side effects. Locking a locked dataset is a no-op.
        """
        if not self.is_locked():
            os.makedirs(self.lock_dir, exist_ok=True)

    def archive(self):
        """
        Creates an archive of this dataset, including metadata. The returned
        object is a generator that can be iterated over to create the complete
        archive.
        """

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.identifier}>"
        