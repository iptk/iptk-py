#!/usr/local/bin/python3
import requests
from .docker_image import DockerImage
from .json import json_hash, json_pretty

class Job(object):
    """
    An IPTK job is stored like any other IPTK metadata. This is a convenience
    class to create an empty IPTK dataset and store the job definition inside.
    This class will make sure that the same dataset identifier is used for 
    equivalent jobs, thus implementing a simple cache algorithm. Docker image
    references will be resolved and converted to a digest-based format upon
    job creation.
    """
    def __init__(self, image_reference, command=[]):
        super(Job, self).__init__()
        self.image = DockerImage(image_reference)
        self.command = command
        self.inputs = []
        self.resource_requests = {}

    def add_input_dataset(self, dataset_id, path="/input"):
        input = {
            "type": "dataset",
            "id": dataset_id,
            "path": path
        }
        self.inputs.append(input)

    def to_json(self):
        return json_pretty(self.spec)
        
    def request_resource(self, resource_type, quantity):
        """
        Request a specified quantity of the given type. The meaning of both the
        type and the quantity fields is up to the job scheduler. Resource 
        requests are not part of the job's minimal specification and the job's
        identifier remains unchanged if requests are added. Calling this method
        again with the same type argument will replace the original value.
        Returns the updated list of resource requests.
        """
        self.resource_requests[resource_type] = quantity
        return self.resource_requests
    
    def enqueue(self, dataset_store):
        """
        Enqueues this job by saving it into the given DatasetStore.
        """
        dataset = dataset_store.dataset(self.identifier)
        metadata_set = dataset.metadata_set("f28b5c411584cd69e29b760305dff098ca286865")
        metadata_set.update(self.minimal_spec)

    @property
    def spec(self):
        """
        Returns the full specification for this job. This can be send to the 
        IPTK web API to enqueue a job remotely.
        """
        spec = {
            "version": 3,
            "image": self.image.spec,
            "command": self.command,
            "inputs": self.inputs,
            "resource_requests": self.resource_requests
        }
        return spec
        
    @property
    def minimal_spec(self):
        """
        Returns the minimal specification that uniquely defines this job. Does
        not include optional fields that are only relevant to the job scheduler
        (e.g. resource requests)
        """
        keys = ["version", "image", "command", "inputs"]
        spec = {x: self.spec[x] for x in keys}
        return spec
        
    @property
    def identifier(self):
        return json_hash(self.minimal_spec)
    
    def __repr__(self):
        return f"<{self.__class__.__name__} {self.identifier}>"
