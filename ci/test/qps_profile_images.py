"""Publish prebuilt immutable runtime artifacts for this diagnostic comparison."""
import json
import os
from types import SimpleNamespace

from materialize import mzbuild


def main():
    inputs = json.loads(os.environ["CI_QPS_RUNTIME_IMAGES"])
    for tag, images in inputs.items():
        assert tag.endswith(".release")
        assert set(images) == {"environmentd", "clusterd"}
        deps = [[], []]
        for name, sources in images.items():
            assert len(sources) == 2
            image = SimpleNamespace(name=name, docker_name=lambda tag, name=name: f"materialize/{name}:{tag}")
            for index, source in enumerate(sources):
                assert source.startswith(f"materialize/{name}:mzbuild-")
                deps[index].append(SimpleNamespace(image=image, spec=lambda source=source: source))
        mzbuild.publish_multiarch_images(tag, deps, verify=True)


if __name__ == "__main__":
    main()
