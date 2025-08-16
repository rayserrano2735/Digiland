#!/bin/bash
# Quick Setup Script for repo-indexer
# For tired Carbies at 2 AM who just want it to work

echo "🚀 Setting up repo-indexer..."

# Create directories
echo "📁 Creating directories..."
mkdir -p ~/tools/repo-indexer/repo_indexer
mkdir -p ~/.repo-indexer

# Create setup.py
echo "📄 Creating setup.py..."
cat > ~/tools/repo-indexer/setup.py << 'EOF'
from setuptools import setup, find_packages

setup(
    name="repo-indexer",
    version="1.0.0",
    packages=find_packages(),
    install_requires=["pyyaml>=5.0"],
    entry_points={
        "console_scripts": [
            "repo-indexer=repo_indexer.indexer:main",
        ],
    },
)
EOF

# Create requirements.txt
echo "📄 Creating requirements.txt..."
cat > ~/tools/repo-indexer/requirements.txt << 'EOF'
pyyaml>=5.0
EOF

# Create __init__.py
echo "📄 Creating __init__.py..."
cat > ~/tools/repo-indexer/repo_indexer/__init__.py << 'EOF'
__version__ = "1.0.0"
from .indexer import RepoIndexer
__all__ = ["RepoIndexer"]
EOF

# Create patterns config
echo "⚙️ Creating patterns.yaml config..."
cat > ~/.repo-indexer/patterns.yaml << 'EOF'
github:
  user: "rayserrano2735"
  repo: "data-engineering-patterns"
  
paths:
  index_folder: "."

file_types:
  - .md
  - .py
  - .sql
  - .txt
EOF

# Create Digiland config
echo "⚙️ Creating digiland.yaml config..."
cat > ~/.repo-indexer/digiland.yaml << 'EOF'
github:
  user: "rayserrano2735"
  repo: "Digiland"
  
paths:
  index_folder: "The Luminary Archives - Digiland Library"

file_types:
  - .md
  - .txt
  - .py
  - .yaml
EOF

echo ""
echo "✅ Directory structure created!"
echo ""
echo "⚠️  IMPORTANT: Now you need to:"
echo "1. Copy the indexer.py code from the 'HDI Auto-Detecting Repo Indexer' artifact"
echo "2. Save it to: ~/tools/repo-indexer/repo_indexer/indexer.py"
echo "3. Run: cd ~/tools/repo-indexer && pip install -e ."
echo ""
echo "Then test with:"
echo "  repo-indexer ~/repos/data-engineering-patterns"
echo ""
echo "💤 But honestly? It's 2 AM. Go to sleep and do this tomorrow!"