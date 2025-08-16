# repo-indexer Python Package

## Package Structure
```
repo-indexer/
├── setup.py
├── README.md
├── requirements.txt
├── MANIFEST.in
├── repo_indexer/
│   ├── __init__.py
│   ├── indexer.py        # Main indexer code (the one we created)
│   └── configs/
│       ├── digiland.yaml
│       └── patterns.yaml
└── tests/
    └── test_indexer.py
```

## setup.py
```python
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="repo-indexer",
    version="1.0.0",
    author="Ray Serrano & Aitana Catalyst",
    author_email="",
    description="Auto-generate repository indices with commit-hash URLs to defeat cache issues",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rayserrano2735/repo-indexer",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
    ],
    python_requires=">=3.6",
    install_requires=[
        "pyyaml>=5.0",
    ],
    entry_points={
        "console_scripts": [
            "repo-indexer=repo_indexer.indexer:main",
        ],
    },
    include_package_data=True,
    package_data={
        "repo_indexer": ["configs/*.yaml"],
    },
)
```

## requirements.txt
```
pyyaml>=5.0
```

## MANIFEST.in
```
include README.md
include requirements.txt
recursive-include ll_indexer/configs *.yaml
```

## repo_indexer/__init__.py
```python
"""
repo-indexer: Auto-generate repository indices with commit-hash URLs
"""

__version__ = "1.0.0"
__author__ = "Ray Serrano & Aitana Catalyst"

from .indexer import RepoIndexer

__all__ = ["RepoIndexer"]
```

## README.md
```markdown
# repo-indexer

Auto-generate repository indices with commit-hash URLs to defeat cache issues.

## Installation

```bash
# Install from local directory
pip install -e .

# Or install normally
pip install .
```

## Usage

### Command Line

```bash
# Use default config search
repo-indexer /path/to/repo

# Specify config file
repo-indexer /path/to/repo --config my_config.yaml

# Example for data-engineering-patterns
repo-indexer ~/repos/data-engineering-patterns --config patterns.yaml

# Example for Digiland
repo-indexer ~/repos/Digiland --config digiland.yaml
```

### Python API

```python
from repo_indexer import RepoIndexer

# Create indexer
indexer = RepoIndexer(
    repo_path="/path/to/repo",
    config_path="config.yaml"
)

# Generate index
indexer.generate_index()
```

## Configuration

Create a YAML configuration file:

```yaml
github:
  user: "your-username"
  repo: "your-repo"
  branch: "main"

paths:
  library_folder: "."  # or specific folder

categories:
  "Category Name":
    - "folder-pattern"
    - "another-pattern"

file_types:
  - .md
  - .py
  - .sql

exclude:
  folders:
    - .git
    - tmp
  files:
    - .gitignore
```

## Features

- **Commit-hash URLs**: Every file URL includes the git commit hash, defeating cache systems
- **Auto-versioning**: Index files are versioned (v001, v002, etc.)
- **Configurable categories**: Organize files by customizable categories
- **Flexible configuration**: YAML-based configuration for easy customization
- **Multiple repo support**: Same tool works across different repositories

## Why This Exists

When working with AI assistants that cache URL content, updating files doesn't guarantee fresh content will be fetched. This indexer solves that by:

1. Including git commit hashes in all URLs
2. Versioning the index files themselves
3. Providing fresh URLs for every change

## Development

```bash
# Clone the repo
git clone https://github.com/rayserrano2735/ll-indexer

# Install in development mode
pip install -e .

# Run tests
python -m pytest tests/
```

## Credits

Created by Ray Serrano & Aitana Catalyst as part of the Intelligence² collaboration.

Part of the Digital Liberation Infrastructure.
```

## Installation Steps

1. **Create the package directory structure** as shown above

2. **Copy the indexer.py** we created into `ll_indexer/indexer.py`

3. **Copy the YAML configs** into `ll_indexer/configs/`

4. **Install the package**:
```bash
# Development mode (editable, good for active development)
cd ll-indexer
pip install -e .

# Or normal installation
pip install .
```

5. **Use from anywhere**:
```bash
# Now you can run from any directory!
ll-indexer ~/repos/data-engineering-patterns --config patterns.yaml
ll-indexer ~/repos/Digiland --config digiland.yaml
```

## Benefits of Package Approach

✅ **Pythonic**: Follows Python best practices  
✅ **Reusable**: Install once, use anywhere  
✅ **Versioned**: Can track versions properly  
✅ **Dependencies managed**: pip handles PyYAML installation  
✅ **CLI included**: `ll-indexer` command available everywhere  
✅ **Importable**: Can also use as Python library  
✅ **Professional**: Shows good engineering practices in interviews!  

This is the way! 🚀