#!/usr/local/bin/python3
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import os, shutil
from glob import glob
from iptk import Dataset
from iptk.metadata_spec import MetadataSpec, MetadataGenerator
from iptk.metadata import KeyValueMetadata

# Create an empty dataset with a given id. Freshly created datasets are 
# unlocked, so the contents of their data/ subdirectory can be manipulated.
# Identifiers must match the [0-9-z]{40} regular expression but it is otherwise
# left to the user to generate them for root datasets, i.e. datasets that are
# not autmatically created as part of a pipeline. A proven method to group 
# DICOM images into datasets is to use the SHA1 sum of their SeriesInstanceUID
# for the identifier. For random identifiers, SHA1 hashes of UUIDs minimize the
# collision probability (purely random data also works, of course).
path = os.path.expanduser("~/Desktop/9953b9152702d1094aadea9c686584b10f385b84")
dataset = Dataset(path, create_ok=True)

# Request the path to the actual data of the dataset and create some files.
# Note that get_data_path() will fail on the second run of this script because
# the dataset will be in a locked state and we request a mutable dataset.
# Locked datasets are considered immutable, which is central to IPTK's caching
# mechanism. Never manipulate a locked dataset's contents. Manipulating its 
# metadata is fine, though. The sample dataset is Lionheart, W. R. B. (2015):
# An MRI DICOM data set of the head of a normal male human aged 52. Zenodo.
# http://doi.org/10.5281/zenodo.16956
if not dataset.is_locked:
    for filename in glob("test_data/*.dcm"):
        shutil.copy(filename, dataset.data_dir)

# Locking the dataset tells compatible IPTK applications that this dataset will
# not be manipulated hereafter. Only locked datasets can be used as inputs in
# IPTK jobs. Locking is done by removing the temp/ directory of a dataset.
dataset.lock()

# Create a metadata specification without a generator and store some data 
# inside. The Metadata class represents a pair of a dataset and a MetadataSpec.
# The KeyValueMetadata subclass provides additional methods to manipulate a 
# dataset's metadata. Simply create a new metadata specification whenever you
# need a new namespace for a set of related metadata. You can also edit 
# metadata in the file system. It is stored in <id>.json files inside the meta/
# subdirectory of the dataset, where <id> is the identifier of the 
# corresponding metadata specification.
metadata_spec = MetadataSpec("University of Münster", "Group Management", 1)
kv_store = KeyValueMetadata(dataset, metadata_spec)
kv_store["tags"] = ["Sample", "HCP"]
kv_store["external_data"] = True
kv_store.save()

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