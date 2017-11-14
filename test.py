#!/usr/local/bin/python3
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import os, shutil
from glob import glob
from iptk.docker_utils import DockerImage
from iptk.runner import Runner
from iptk.dataset_store import DatasetStore
from iptk.metadata_spec import MetadataSpec, MetadataGenerator
from iptk.metadata import KeyValueMetadata
from iptk.mount import Mount
from iptk.job import Job
from iptk.runner import Runner

# Create a dataset store. A store automatically keeps the datasets arranged in
# compliance with the IPTK specification. It also manages metadata.
store = DatasetStore("/Users/janten/Desktop/datasets")

# Create an empty dataset with a given id. Freshly created datasets are 
# unlocked, so the contents of their data/ subdirectory can be manipulated.
# Identifiers must match the [0-9-z]{40} regular expression but it is otherwise
# left to the user to generate them for root datasets, i.e. datasets that are
# not autmatically created as part of a pipeline. A proven method to group 
# DICOM images into datasets is to use the SHA1 sum of their SeriesInstanceUID
# for the identifier. For random identifiers, SHA1 hashes of UUIDs minimize the
# collision probability (purely random data also works, of course).
id = "9953b9152702d1094aadea9c686584b10f385b84"
dataset = store.get_dataset(id, create_ok=True)

# Request the path to the actual data of the dataset and create some files.
# Note that get_data_path() will fail on the second run of this script because
# the dataset will be in a locked state and we request a mutable dataset.
# Locked datasets are considered immutable, which is central to IPTK's caching
# mechanism. Never manipulate a locked dataset's contents. Manipulating its 
# metadata is fine, though. The sample dataset is Lionheart, W. R. B. (2015):
# An MRI DICOM data set of the head of a normal male human aged 52. Zenodo.
# http://doi.org/10.5281/zenodo.16956
if not store.is_locked(dataset):
    data_path = store.get_data_path(dataset, mutable=True)
    for filename in glob("test_data/*.dcm"):
        shutil.copy(filename, data_path)

# Locking the dataset tells compatible IPTK applications that this dataset will
# not be manipulated hereafter. Only locked datasets can be used as inputs in
# IPTK jobs. Locking is done by removing the temp/ directory of a dataset.
store.lock_dataset(dataset)

# Create a job using dataset above as an input. This simple job only uses a
# single input but IPTK supports an arbitrary amount.
image = DockerImage("blakedewey/dcm2niix")
mounts = [Mount(dataset, "/input")]
job = Job(image, mounts, "dcm2niix -o /output -z i -f nifti /input")

# A job is executed in two steps. First, a new dataset for the job's output is
# created in the dataset store and the job specification is stored within its
# metadata. Then an IPTK runner accesses the dataset, reads the specification,
# executes the job, and locks the dataset afterwards. Saving the same job 
# multiple times is a no-op.
job.save(store)

# Create a runner using our dataset store. Runners will normally be executed on
# different machines, this is just for testing.
runner = Runner(store)

# We can fetch the identifier of the job's output dataset from the Job
# instance to execute the job. This part would normally be executed on a remote
# computer and the dataset identifiers to process will be passed through a 
# Redis queue or web API. Note that process_dataset() may not do anything if an
# equivalent job already ran before. This is the caching mechanism mentioned
# above.
output_dataset = store.get_dataset(job.identifier)
runner.process_dataset(output_dataset)

# Since process_dataset() is a synchronous method, the job is now finished.
output_path = store.get_data_path(output_dataset)
print(os.listdir(output_path))

# Create another job, this time using the previous output as an input.
image = DockerImage("vistalab/fsl-v5.0")
mounts = [Mount(output_dataset, "/input")]
job = Job(image, mounts, "bet2 /input/nifti.nii.gz /output/betted")
job.save(store)
final_dataset = store.get_dataset(job.identifier)
runner.process_dataset(final_dataset)

# Create a metadata specification without a generator and store some data 
# inside. The Metadata class represents a pair of a dataset and a MetadataSpec.
# The KeyValueMetadata subclass provides additional methods to manipulate a 
# dataset's metadata. Simply create a new metadata specification whenever you
# need a new namespace for a set of related metadata. You can also edit 
# metadata in the file system. It is stored in <id>.json files inside the meta/
# subdirectory of the dataset, where <id> is the identifier of the 
# corresponding metadata specification.
metadata_spec = MetadataSpec("University of Münster", "Group Management", 1)
kv_store = KeyValueMetadata(dataset, store, metadata_spec)
kv_store["tags"] = ["Sample", "HCP"]
kv_store["external_data"] = True
kv_store.save()

# You can also access the metadata directly. While this allows you to store
# arbitrary JSON-serializable content, it does not ensure the indexability of
# the data.
raw_metadata = store.get_metadata(dataset, metadata_spec)
raw_metadata["nested"] = {"I": {"will": {"break": "ElasticSearch", "Right?": True}}}
store.set_metadata(dataset, metadata_spec, raw_metadata)

# Since the true IPTK API is always the data as stored on disk, you can also
# directly manipulate that. Everything is fine as long as the file does not
# exist or is a valid JSON file.
meta_path = store.get_meta_path(dataset, metadata_spec)

# Create a metadata specification with a generator and glob filter patterns.
# Metadata generators are docker images that are automatically instantiated on
# datasets if all of their filters match the dataset's contents. The contents
# will be mounted into the container at /input and the container has to write
# its metadata to /output/meta.json.
image = DockerImage("janten/nps-dicom-to-json")
filters = ["*.dcm"]
generator = MetadataGenerator(image, filters)
metadata_spec = MetadataSpec("University of Münster", "DICOM Headers", 1, generator)
print(metadata_spec.spec)

# Should be True
metadata_spec.generator.should_fire(dataset, store)

# The end.

# Current issues:
#
# - Metadata generators are not automatically triggered because there is no
#   central installation location in the IPTK system. Maybe an ApplicationStore
#   should be defined alongside the existing DatasetStore.
# - The Python interface is upside down. There are many calls to DatasetStore
#   that pass Dataset objects, which have been created by the store itself. It
#   would probably be easier to pass identifiers to the store instances and see
#   the Dataset objects as convenience wrappers with initializers that require
#   a DatasetStore to be passed. This could look this:
#   
#       store = DatasetStore("<path>")
#       dataset = Dataset("<id>", store)
#       kv = KeyValueMetadata(dataset, spec)
#       kv["cool"] = True
#
#   Which would internally call kv.dataset.store to actually set the value.
#   However, this would make it impossible to create datasets for which the
#   backing store is not available, e.g. to create a job that is to be 
#   submitted to a remote runner from a computer that does not mount the shared
#   dataset storage.
# - There is no command-line interface yet. The previous NPS CLI made it very
#   easy to create jobs and edits tags on a remote system.
# - Runners need full write access to the backing store. GitLab uses a system
#   where runners receive and send data over a https connection. IPTK could 
#   send dataset contents as tar archives and runners could temporarily 
#   replicate the store locally before sending back the results. 