import os
import setuptools
import versioneer

here = os.path.abspath(os.path.dirname(__file__))

DESCRIPTION = ("Mlflow plugin to use ElasticSearch as backend for MLflow tracking service")

try:
    LONG_DESCRIPTION = open(os.path.join(here, "README.md"), encoding="utf-8").read()
except Exception:
    LONG_DESCRIPTION = ""


def _read_reqs(relpath):
    fullpath = os.path.join(os.path.dirname(__file__), relpath)
    with open(fullpath) as f:
        return [s.strip() for s in f.readlines()
                if (s.strip() and not s.startswith("#"))]


REQUIREMENTS = _read_reqs("requirements.txt")
TESTS_REQUIREMENTS = _read_reqs("tests-requirements.txt")

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Operating System :: POSIX :: Linux",
    "Environment :: Console",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Topic :: Software Development :: Libraries"
]


setuptools.setup(
    name="mlflow-elasticsearchstore",
    packages=setuptools.find_packages(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    install_requires=REQUIREMENTS,
    tests_require=["pytest"],
    python_requires=">=3.6",
    maintainer="Criteo",
    maintainer_email="github@criteo.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    classifiers=CLASSIFIERS,
    keywords="mlflow",
    url="https://github.com/criteo/mlflow-elasticsearchstore",
    # Require MLflow as a dependency of the plugin, so that plugin users can simply install
    # the plugin and then immediately use it with MLflow
    entry_points={
        # Define a Tracking Store plugin for tracking URIs with scheme 'file-plugin'
        "mlflow.tracking_store": "elasticsearch=mlflow_elasticsearchstore."
        "elasticsearch_store:ElasticsearchStore",
    }
)
