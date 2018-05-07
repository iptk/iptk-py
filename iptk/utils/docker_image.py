#!/usr/local/bin/python3
import requests

class DockerImage(object):
    """
    Represents a specific Docker image in IPTK. While this can be constructed
    from a human-readable image reference, the reference will be resolved to a
    digest value on instance creation.
    """
    def __init__(self, reference):
        super(DockerImage, self).__init__()
        self.resolve(reference)
    
    @classmethod
    def from_dict(cls, specification):
        registry = specification["registry"]
        repository = specification["repository"]
        digest = specification["digest"]
        reference = f"{registry}/{repository}@{digest}"
        return cls(reference)
    
    @property
    def spec(self):
        spec = {
            "registry": self.registry,
            "repository": self.repository,
            "digest": self.digest
        }
        return spec
        
    
    def get_digest(self, registry, repository, tag):
        manifest_url = f"https://{registry}/v2/{repository}/manifests/{tag}"
        headers = {"Accept": "application/vnd.docker.distribution.manifest.v2+json"}
        r = requests.head(manifest_url, headers=headers)
        if r.status_code == 401:
            token_url = f"https://auth.docker.io/token"
            token_params = {
                "service": "registry.docker.io",
                "scope": f"repository:{repository}:pull"
            }
            token = requests.get(token_url, params=token_params, json=True).json()["token"]
            headers["Authorization"] = f"Bearer {token}"
            r = requests.head(manifest_url, headers=headers)
        if r.status_code != 200:
            raise ValueError(f"Could not get manifest from {manifest_url}. Check image reference.")
        return r.headers['Docker-Content-Digest']

    def resolve(self, reference):
        # Docker default values
        registry = "registry-1.docker.io"
        repository = reference
        tag = "latest"
        digest = None
        # Parse domain part, if any
        if "/" in repository:
            domain, remainder = repository.split("/", 1)
            if domain == "localhost" or "." in domain or ":" in domain:
                registry = domain
                repository = remainder
        # Separate image reference and digest
        if "@" in repository:
            repository, digest = repository.split("@", 1)
        # See if image contains a tag
        if ":" in repository:
            repository, tag = repository.split(":", 1)
        # Handle "familiar" Docker references
        if registry == "registry-1.docker.io" and "/" not in repository:
            repository = "library/" + repository
        if not digest:
            digest = self.get_digest(registry, repository, tag)
        self.registry = registry
        self.repository = repository
        self.tag = tag
        self.digest = digest
        self.reference = f"{registry}/{repository}@{digest}"
    
    def __repr__(self):
        return f"<{self.__class__.__name__} {self.reference}>"
