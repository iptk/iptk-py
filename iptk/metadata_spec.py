import os
from pathlib import Path
from .json_utils import json_hash

class MetadataGenerator(object):
    """
    A metadata generator automatically launches a specified Docker container on
    matching IPTK datasets. The spawned containers will have the dataset's data
    mounted on /input and are expected to write their generated metadata to
    /output/meta.json. All other output files will be disregarded. Execution
    time for the container may be limited on some systems. The optional 
    conditions array may specify a range of Unix shell patterns, the Docker
    container will only be created if all of them match at least one file.
    Patterns are evaluated relative to the data/ directory. Non-relative 
    patterns are unsupported.
    """
    def __init__(self, image, conditions=[]):
        super(MetadataGenerator, self).__init__()
        self.image = image
        self.conditions = conditions
    
    @property
    def spec(self):
        spec = {
            "conditions": self.conditions,
            "image": self.image.spec
        }
        return spec
    
    def should_fire(self, dataset, store):
        path = os.path.join(store.get_path(dataset), "data")
        for condition in self.conditions:
            generator = Path(path).glob(condition)
            if not next(generator, None):
                return False
        return True
    
    def __repr__(self):
        return f"<{self.__class__.__name__} {self.image}>"
        
class MetadataSpec(object):
    """
    IPTK supports the storage of arbitrary metadata within a dataset's meta/
    directory. The MetadataSpec class provides unique identifiers for each
    metadata collection. Identifiers are generated from the organization, name,
    and version fields of a MetadataSpec.
    """
    def __init__(self, organization: str, name: str, version: int, generator: MetadataGenerator=None):
        super(MetadataSpec, self).__init__()
        self.organization = organization
        self.name = name
        self.version = version
        self.generator = generator

    @property
    def minimal_spec(self):
        """
        The minimal specification which is also used to create the unique
        identifier.
        """
        spec = {
            "name": self.name,
            "organization": self.organization,
            "version": self.version
        }
        return spec
    
    @property
    def spec(self):
        spec = self.minimal_spec
        if self.generator:
            spec["generator"] = self.generator.spec
        return spec
        
    @property
    def identifier(self):
        return json_hash(self.minimal_spec)
        
    def __repr__(self):
        return f"<{self.__class__.__name__} {self.identifier}>"