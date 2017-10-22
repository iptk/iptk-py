import os, re, shutil

class Dataset(object):
    """
    Instances of the Dataset class represent IPTK datasets in an abstract 
    fashion. A concrete dataset is specified by the combination of a pair of
    a Dataset and a DatasetStore instance. 
    """
    def __init__(self, identifier, store=None):
        super(Dataset, self).__init__()
        if not re.match("^[0-9a-z]{40}$", identifier):
            raise ValueError('Invalid dataset identifier')
        self.identifier = identifier
        self.store = store

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.identifier}>"