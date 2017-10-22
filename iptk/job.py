from datetime import datetime
from .json_utils import json_hash
from .metadata_spec import MetadataSpec
from .mount import Mount
from .docker_utils import DockerImage

class Job(object):
    """
    IPTK jobs take a variable amount of input datasets and a Docker image
    specification. The IPTK runner will create a container from the given image
    and mount the input datasets as specified. An output dataset will 
    automatically be created and it's data/ directory will be mounted in the
    container at /output. The output dataset will automatically be locked after
    the container has finished executing.
    """
    def __init__(self, image, mounts, command=None):
        super(Job, self).__init__()
        self.image = image
        self.mounts = mounts
        self.command = command

    @classmethod
    def from_dict(cls, specification):
        image = DockerImage.from_dict(specification["image"])
        mounts = []
        for m in specification["mounts"]:
            mounts.append(Mount.from_dict(m))
        command = specification.get("command", None)
        return cls(image, mounts, command)

    @property
    def minimal_spec(self):
        spec = {
            "command": self.command,
            "image": self.image.spec,
            "mounts": [m.spec for m in self.mounts]
        }
        return spec
        
    @property
    def spec(self):
        spec = self.minimal_spec
        return spec
    
    @property
    def identifier(self):
        """
        A job's identifier is also the identifier of the resulting dataset if
        the jobs is executed by the IPTK runner.
        """
        return json_hash(self.minimal_spec)
        
    def save(self, store):
        """
        Save this job to a dataset store. This creates a new dataset with this
        job's specification stored in it's metadata. An IPTK runner can then be
        used to run the job.
        """
        meta_spec = MetadataSpec("University of Münster", "IPTK Job", 3)
        dataset = store.get_dataset(self.identifier, create_ok=True)
        current_job_spec = store.get_metadata(dataset, meta_spec)
        if current_job_spec:
            current_job = Job.from_dict(current_job_spec)
            if self.minimal_spec != current_job.minimal_spec:
                raise Exception("Different job exists with equal identifier")
        store.set_metadata(dataset, meta_spec, self.spec)

            