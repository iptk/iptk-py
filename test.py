#!/usr/local/bin/python3
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from iptk.docker_utils import get_parts, get_reference
from iptk.runner import Runner
from iptk.dataset_store import DatasetStore
from iptk.metadata_spec import MetadataSpec, MetadataGenerator
from iptk.metadata import KeyValueMetadata

# Create a dataset store
s = DatasetStore("/Users/janten/Desktop/datasets")

# Create an empty dataset with a given id
id = "9953b9152702d1094aadea9c686584b10f385b84"
d = s.get_dataset("9953b9152702d1094aadea9c686584b10f385b84", create_ok=True)

# Create a metadata specification with a generator and glob patterns
globs = ["*.dcm"]
registry, repository, tag, digest = get_parts("neurology/jupyter:latest")
g = MetadataGenerator(registry, repository, digest, globs)
m = MetadataSpec("University of Münster", "DICOM Headers", 1, g)

# Should be False as there are no *.dcm files in an empty dataset.
print(m.generator.should_fire(d)) 

# Create a metadata specification without a generator and store some data 
# inside. The Metadata class represents a pair of a dataset and a MetadataSpec.
# The KeyValueMetadata subclass provides additional methods manipulate a 
# dataset's metadata.
m = MetadataSpec("University of Münster", "Tags", 1)
k = KeyValueMetadata(d, m)
k["tags"] = ["New", "Cool!", "\U0001F4A9"]
k["cool"] = True
k.save()
print(k)

valid_descriptors = [
    "neurology/jupyter:latest",
    "hello-world"
]

job = {
    "container": {
        "command": "exit",
        "digest": "sha256:494769f95148e697a5c321b72767052b233d58f7a0e240cd6c1175aa7cae5c0e",
        "registry": "registry-1.docker.io",
        "repository": "neurology/jupyter"
    },
    "inputs": [
        {
            "name": "",
            "path": "/input",
            "type": "dataset"
        }
    ],
    "name": "Normalise to MNI Space",
    "version": 3
}

#for desc in valid_descriptors:
#    registry, repository, tag, digest = get_parts(desc)
#    print(get_reference(registry, repository, digest))

