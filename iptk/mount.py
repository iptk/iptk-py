from .dataset import Dataset

class Mount(object):
    """
    Mount objects are used to connect datasets to containers in jobs. All 
    user-defined mounts are read-only to facilitate IPTK's caching mechanism.
    """
    def __init__(self, dataset, location="/input"):
        super(Mount, self).__init__()
        self.dataset = dataset
        self.location = location
    
    @classmethod
    def from_dict(cls, specification):
        dataset = Dataset(specification["name"])
        location = specification["path"]
        return cls(dataset, location)
    
    @property
    def spec(self):
        spec = {
            "type": "dataset",
            "name": self.dataset.identifier,
            "path": self.location
        }
        return spec