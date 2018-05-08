import os, re, json, shutil, zipstream
from .metadata import Set as MetadataSet
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
        identifier = os.path.basename(path.rstrip('/'))
        if not re.match("^[0-9a-z]{40}$", identifier):
            raise ValueError('Invalid dataset identifier')
        if create_ok:
            os.makedirs(path, exist_ok=True)
        if not os.path.exists(path):
            raise ValueError(f'Path {path} does not exist')
        self._identifier = identifier
        self._path = path

    @property
    def identifier(self):
        return self._identifier

    @property
    def data_dir(self):
        """
        Return the path to the data/ subfolder of this dataset. The folder will
        be created if it does not exist.
        """
        path = os.path.join(self._path, 'data')
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

    def metadata_set(self, spec_id):
        """
        Returns an iptk.metadata.set for this dataset and the given metadata 
        specification identifier. The returned set can be used to read and 
        write metadata.
        """
        return MetadataSet(self, spec_id)
        
    def metadata_specs(self):
        """
        Lists the available metadata specification identifiers for this 
        dataset. Use the metadata_set method to retrieve the corresponding 
        metadata set.
        """
        meta_path = os.path.join(self._path, "meta", "*.json")
        available_specs = []
        for path in glob(meta_path):
            basename = os.path.basename(path)
            spec = os.path.splitext(basename)[0]
            available_specs.append(spec)
        return available_specs

    def __repr__(self):
        return f"<{self.__class__.__name__} {self._identifier}>"
        