import os, re, json, shutil, zipstream
from glob import glob

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

    def list_data(self):
        files = []
        for data_file in glob(os.path.join(self.data_dir, '**'), recursive=True):
            relative_path = os.path.relpath(os.path.normpath(data_file), self.data_dir)
            if os.path.isdir(data_file):
                relative_path += "/"
            files.append(relative_path)
        return files

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
        json_path = os.path.join(meta_path, f"{spec_id}.json")
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
        
        :param spec_id: Metadata specification identifier. Must be a valid IPTK identifier.
        :param data: The data to store in the metadata set. Must be a JSON-serializable dictionary.
        """
        path = self.metadata_path(spec_id)
        with open(path, "w") as f:
            json.dump(data, f)
        return data

    def delete_metadata(self, spec_id):
        """
        Delete the JSON file containing the metadata specified by the given identifier.
        :param spec_id: Metadata specification identifier. Must be a valid IPTK identifier.
        """
        path = self.metadata_path(spec_id)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        return True
        
    @property
    def metadata_sets(self):
        meta_path = os.path.join(self.path, "meta", "*.json")
        available_specs = []
        for path in glob(meta_path):
            basename = os.path.basename(path)
            spec = os.path.splitext(basename)[0]
            available_specs.append(spec)
        return available_specs

    def archive(self):
        """
        Creates an archive of this dataset, including metadata. The returned
        object is a generator that can be iterated over to create the complete
        archive.
        """
        z = zipstream.ZipFile(mode='w', compression=zipstream.ZIP_DEFLATED)
        data_path = self.data_dir
        for root, dirs, files in os.walk(data_path):
            for f in files:
                full_path = os.path.join(root, f)
                if not os.path.islink(full_path):
                    z.write(full_path, os.path.relpath(full_path, data_path))
        return z

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.identifier}>"
        