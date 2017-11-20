import json, os, re, shutil
from .dataset import Dataset

class DatasetStore(object):
    """Manages the storage and creation of datasets on disk"""
    def __init__(self, root_path):
        super(DatasetStore, self).__init__()
        self.root_path = root_path
    
    def get_path(self, dataset):
        """
        Returns the path of the requested dataset on disk.
        """
        subdir = "/".join(list(dataset.identifier[:4]))
        path = os.path.join(self.root_path, subdir, dataset.identifier)
        return path

    def get_data_path(self, dataset, mutable=None):
        """
        Returns the full path to the given dataset's data/ directory. If the
        mutable argument is set, an exception is raised for locked datasets (if
        mutable == True) or unlocked datasets (if mutable == False).
        """
        if (mutable is not None) and (self.is_locked(dataset) == mutable):
            raise ValueError("Dataset lock state does not match request")
        dataset_path = self.get_path(dataset)
        return os.path.join(dataset_path, "data")
        
    def get_meta_path(self, dataset, spec):
        dataset_path = self.get_path(dataset)
        return os.path.join(dataset_path, "meta", f"{spec.identifier}.json")
        
    def get_metadata(self, dataset, spec):
        """
        Returns the metadata saved within this store for the given combination
        of dataset and metadata specification.
        """
        path = self.get_meta_path(dataset, spec)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return json.load(f)

    def set_metadata(self, dataset, spec, data):
        """
        Sets the metadata for the given dataset and metadata specification. The
        data object must be JSON-serializable. Note that your changes may be
        overwritten if the metadata specification is associated with a metadata
        generator.
        """
        path = self.get_meta_path(dataset, spec)
        with open(path, "w+") as f:
            return json.dump(data, f, sort_keys=True, indent=4, separators=(',', ': '))

    def get_dataset(self, dataset_id, create_ok=False):
        """
        Fetch a Dataset object backed by this DatasetStore. Raises a value
        error for invalid values of dataset_id. The dataset identifier must be
        a hex string of 40 characters (i.e. must match /^[0-9a-z]{40}$/). This
        method can optionally create an empty dataset if no dataset with the
        given identifier exists. 
        """
        dataset = Dataset(dataset_id, self)
        path = self.get_path(dataset)
        if not os.path.exists(path):
            if create_ok:
                subdirs = ["temp", "data", "meta"]
                for s in subdirs:
                    os.makedirs(os.path.join(path, s), exist_ok=True)
            else:
                raise ValueError("Dataset not found in this store")
        return dataset
    


    def __repr__(self):
        return f"<{self.__class__.__name__} {self.root_path}>"