from .dataset import Dataset

class Mount(object):
    """
    Mount objects are used to connect datasets to containers in jobs.
    """
    def __init__(self, dataset_id, location="/input"):
        super().__init__()
        self.dataset_id = dataset_id
        self.location = location
    
    @classmethod
    def from_dict(cls, specification):
        dataset_id = specification["name"]
        location = specification["path"]
        return cls(dataset_id, location)
    
    @property
    def spec(self):
        spec = {
            "type": "dataset",
            "name": self.dataset_id,
            "path": self.location
        }
        return spec