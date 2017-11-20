import docker, redis, uuid
from .dataset import Dataset
from .metadata_spec import MetadataSpec
from .job import Job

class Runner(object):
    """
    Runner objects implement a simple way to execute jobs in IPTK. A runner can
    either be used manually, trough the process_dataset() method, or 
    automatically. Use the connect() method to connect the runner to a Redis
    instance for an automated runner. It will then fetch dataset identifiers
    from a Redis queue which can be shared across multiple runners.
    """
    def __init__(self):
        super().__init__()
        self.uuid = str(uuid.uuid4())
            
    def claim_dataset(self, dataset):
        """
        This marks the dataset as being processed by this runner instance.
        Returns True if the dataset was claimed successfully, False otherwise.
        Claiming fails if the dataset is locked or already claimed by another
        runner. If you develop your own runner you are encouraged to copy this
        method, including the metadata specification.
        """
        if dataset.is_locked:
            return False
        spec = MetadataSpec("University of Münster", "IPTK Runner", 1)
        claim = self.store.get_metadata(dataset, spec)
        if not claim or "runner" not in claim:
            claim = {"runner": self.uuid}
            self.store.set_metadata(dataset, spec, claim)
        claim = self.store.get_metadata(dataset, spec)
        return claim["runner"] == self.uuid
    
    def process_dataset(self, dataset: Dataset):
        """
        Fetches the job description for the given dataset in this runner's
        store and executes the job if all input datasets are locked and the
        output dataset is unlocked and not being used by another runner.
        """
        if not self.claim_dataset(dataset):
            # Unsuitable dataset
            return 
        spec = MetadataSpec("University of Münster", "IPTK Job", 3)
        job_spec = self.store.get_metadata(dataset, spec)
        if not job_spec:
            # Dataset did not contain a job specification
            return
        job = Job.from_dict(job_spec)
        mounts = {
            self.store.get_data_path(dataset, mutable=True): {
                "bind": "/output",
                "mode": "rw"
            }
        }
        for mount in job.mounts:
            path = self.store.get_data_path(mount.dataset, mutable=False)
            mounts[path] = {"bind": mount.location, "mode": "ro"}
        name = f"iptk-job-{dataset.identifier}"
        client = docker.from_env(version="auto")
        client.images.pull(job.image.reference)
        container = client.containers.create(job.image.reference, 
            volumes=mounts, command=job.command, detach=True)
        container.start()
        container.wait()
        container.remove()
        self.store.lock_dataset(dataset)

