# A DatasetStore can be used to create and retrieve IPTK Datasets
from iptk import DatasetStore
ds = DatasetStore("test_store/")

# Each IPTK dataset needs a unique identifier. Using the SHA1 hash of a unique
# descriptor of the files in the dataset is strongly recommended. For DICOM
# data, the SeriesInstanceUID field is the recommended descriptor to group 
# files by acquisition.
import hashlib, pydicom
dicom_info = pydicom.read_file("test_data/IM-0001-0001.dcm")
hash = hashlib.sha1(dicom_info.SeriesInstanceUID.encode('utf-8')).hexdigest().lower()
dataset = ds.dataset(hash)

# The data_dir property contains the path to the dataset's content. To populate 
# the dataset, copy files to that location.
from glob import glob
from shutil import copy2
target_path = dataset.data_dir
for source_path in glob("test_data/*.dcm"):
    copy2(source_path, target_path)
# print(dataset.list_data())

# To add metadata to the dataset, create a metadata set. A metadata set 
# contains all the metadata of a specific type (the metadata specification) for
# a single dataset.
spec_id = "2bc88bb1cbe97e9fa747ea54635888983de942d6"
metadata_set = dataset.metadata_set(spec_id)
metadata_set["tags"] = ["IPTK", "awesome"]
metadata_set["count"] = 0 if not "count" in metadata_set else metadata_set["count"] + 1
# Metadata has now been persisted to the underlaying storage.
print(metadata_set)

# The iptk.utils.job class can be used to build jobs that are saved like any 
# other metadata and can be executed by compatible schedulers. This example
# converts our dicom dataset to the nifti file format. The /output location
# will always point to the job's target dataset.

from iptk.utils import Job
job = Job("janten/dicom-to-nifti", ["dcm2niix", "-o", "/output", "/input"])
job.add_input_dataset(dataset.identifier, "/input")
job.request_resource("memory", "256MB")
print(job.to_json())

# To schedule the job, save it into the DatasetStore.
job.enqueue(ds)