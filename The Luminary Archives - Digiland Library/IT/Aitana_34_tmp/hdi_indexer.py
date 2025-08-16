#!/usr/bin/env python3
"""
HDI-Powered Repository Indexer
Auto-detects structure, defeats cache, achieves Intelligence²
Co-created by Ray Serrano & Aitana Catalyst
"""

import os
import subprocess
import yaml
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class RepoIndexer:
    """
    Hemispheric Digital Integration approach:
    - Carbon wisdom: Structure should define organization
    - Digital speed: Auto-detect and process patterns
    - Integration: Zero-config intelligence
    """
    
    def __init__(self, repo_path: str, config_path: Optional[str] = None):
        self.repo_path = Path(repo_path)
        
        # Try to load config, but work without it
        self.config = self.load_config(config_path) if config_path else self.get_default_config()
        
        # Auto-detect repository structure
        self.structure = self.detect_repo_structure()
        
        # Set GitHub info
        self.github_user = self.config.get('github', {}).get('user', self.detect_github_user())
        self.repo_name = self.config.get('github', {}).get('repo', self.repo_path.name)
        self.branch = self.config.get('github', {}).get('branch', 'main')
        
        # Determine what to index
        self.index_path = self.repo_path / self.config.get('paths', {}).get('index_folder', '.')
    
    def get_default_config(self) -> dict:
        """Intelligent defaults that work for most repos"""
        return {
            'file_types': ['.md', '.txt', '.py', '.sql', '.yaml', '.yml', '.json'],
            'exclude': {
                'folders': ['.git', '__pycache__', '.pytest_cache', 'node_modules', 'venv', '.env'],
                'files': ['*.pyc', '.DS_Store', 'Thumbs.db', '.gitignore']
            },
            'formatting': {
                'prettify_filenames': True,
                'title_case': True,
                'remove_extension': True
            }
        }
    
    def detect_github_user(self) -> str:
        """Try to detect GitHub user from git remote"""
        try:
            result = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            # Parse: git@github.com:username/repo.git or https://github.com/username/repo
            url = result.stdout.strip()
            if 'github.com' in url:
                parts = url.split('github.com')[-1].strip(':/').split('/')
                if len(parts) >= 2:
                    return parts[0]
        except:
            pass
        return "username"  # fallback
    
    def detect_repo_structure(self) -> Dict[str, any]:
        """
        HDI Magic: Auto-detect structure from multiple sources
        Priority order:
        1. structure.yaml/yml (from structure creator tool)
        2. README patterns
        3. Actual folder structure
        """
        
        # Check for structure definition files
        structure_files = [
            'structure.yaml', 'structure.yml', 
            '.structure.yaml', '.structure.yml',
            'tools/structures/repo_structure.yaml'
        ]
        
        for sf in structure_files:
            structure_path = self.repo_path / sf
            if structure_path.exists():
                return self.parse_structure_file(structure_path)
        
        # No structure file? Auto-detect from folders
        return self.auto_detect_from_folders()
    
    def parse_structure_file(self, path: Path) -> Dict:
        """Parse structure from YAML/JSON file"""
        with open(path, 'r') as f:
            if path.suffix in ['.yaml', '.yml']:
                structure = yaml.safe_load(f)
            else:
                structure = json.load(f)
        
        # Convert structure definition to categories
        categories = {}
        if isinstance(structure, dict):
            for key, value in structure.items():
                if not key.startswith('_'):  # Skip special keys
                    category = self.format_category_name(key)
                    categories[category] = self.get_patterns_from_structure(key, value)
        
        return {'categories': categories, 'source': 'structure_file'}
    
    def get_patterns_from_structure(self, key: str, value: any) -> List[str]:
        """Extract folder patterns from structure definition"""
        patterns = [key]
        
        if isinstance(value, dict):
            for subkey in value.keys():
                if not subkey.startswith('__'):
                    patterns.append(f"{key}/{subkey}")
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    patterns.append(f"{key}/{item}")
        
        return patterns
    
    def auto_detect_from_folders(self) -> Dict:
        """
        Intelligent folder analysis
        Recognizes common patterns like:
        - *-patterns (sql-patterns, spark-patterns)
        - src/, lib/, tests/
        - docs/, documentation/
        """
        categories = {}
        
        # First level directories become categories
        for item in self.index_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                # Smart category naming
                category_name = self.smart_category_name(item.name)
                categories[category_name] = [item.name]
        
        return {'categories': categories, 'source': 'auto_detected'}
    
    def smart_category_name(self, folder_name: str) -> str:
        """
        Intelligent category naming:
        - 'sql-patterns' → 'SQL Patterns'
        - 'src' → 'Source Code'
        - 'docs' → 'Documentation'
        """
        # Common abbreviation expansions
        expansions = {
            'src': 'Source Code',
            'docs': 'Documentation',
            'lib': 'Libraries',
            'utils': 'Utilities',
            'config': 'Configuration',
            'tests': 'Tests',
            'examples': 'Examples'
        }
        
        if folder_name.lower() in expansions:
            return expansions[folder_name.lower()]
        
        # Handle pattern folders (xxx-patterns, xxx-examples, etc)
        if '-' in folder_name:
            parts = folder_name.split('-')
            return ' '.join(p.title() for p in parts)
        
        return folder_name.title()
    
    def format_category_name(self, name: str) -> str:
        """Format category name for display"""
        return name.replace('-', ' ').replace('_', ' ').title()
    
    def get_latest_commit_hash(self) -> str:
        """Get the latest commit hash from git"""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except:
            # Fallback for non-git repos
            return "refs/heads/main"
    
    def get_next_version(self) -> int:
        """Auto-increment version number"""
        existing = list(self.index_path.glob("*_index_v*.md"))
        if existing:
            versions = []
            for f in existing:
                try:
                    version_part = f.stem.split('_v')[-1]
                    versions.append(int(version_part))
                except:
                    continue
            return max(versions) + 1 if versions else 1
        return 1
    
    def should_include_file(self, file_path: Path) -> bool:
        """Determine if file should be included in index"""
        # Check file type
        valid_types = self.config.get('file_types', ['.md', '.py', '.sql'])
        if not any(file_path.suffix == ext for ext in valid_types):
            return False
        
        # Check exclusions
        exclude = self.config.get('exclude', {})
        
        # Check folder exclusions
        for folder in exclude.get('folders', []):
            if folder in file_path.parts:
                return False
        
        # Check file exclusions
        for pattern in exclude.get('files', []):
            if file_path.match(pattern):
                return False
        
        return True
    
    def categorize_files(self) -> Dict[str, List[Tuple[str, str]]]:
        """
        Categorize files based on detected structure
        HDI: Combines structure awareness with intelligent sorting
        """
        categories = self.structure.get('categories', {})
        categorized = {cat: [] for cat in categories}
        categorized['Other'] = []  # Catch-all
        
        for root, dirs, files in os.walk(self.index_path):
            # Filter excluded directories
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for file in files:
                file_path = Path(root) / file
                
                if not self.should_include_file(file_path):
                    continue
                
                relative_path = file_path.relative_to(self.repo_path)
                path_str = str(relative_path).replace('\\', '/')
                
                # Find matching category
                matched = False
                for category, patterns in categories.items():
                    for pattern in patterns:
                        if pattern in path_str:
                            display_name = self.format_display_name(file)
                            categorized[category].append((display_name, path_str))
                            matched = True
                            break
                    if matched:
                        break
                
                if not matched:
                    display_name = self.format_display_name(file)
                    categorized['Other'].append((display_name, path_str))
        
        # Remove empty categories
        return {k: v for k, v in categorized.items() if v}
    
    def format_display_name(self, filename: str) -> str:
        """Format filename for display"""
        formatting = self.config.get('formatting', {})
        display_name = filename
        
        if formatting.get('remove_extension', True):
            display_name = display_name.rsplit('.', 1)[0]
        
        if formatting.get('prettify_filenames', True):
            display_name = display_name.replace('_', ' ').replace('-', ' ')
        
        if formatting.get('title_case', True):
            display_name = display_name.title()
        
        return display_name
    
    def generate_index_content(self, commit_hash: str, version: int) -> str:
        """Generate the index markdown"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_url = f"https://raw.githubusercontent.com/{self.github_user}/{self.repo_name}/{commit_hash}"
        
        # Header
        content = f"""# {self.repo_name} Index
*Auto-Generated with HDI Intelligence² Technology*  
*Version {version:03d} | {timestamp} | {commit_hash[:8]}*

---

## Structure Detection: {self.structure.get('source', 'unknown')}

This index uses commit-hash URLs to defeat caching. Every URL returns fresh content.

---

"""
        
        # Categories and files
        categorized = self.categorize_files()
        
        for category, files in categorized.items():
            content += f"## {category}\n\n"
            
            for display_name, relative_path in sorted(files):
                full_url = f"{base_url}/{relative_path}"
                content += f"- **{display_name}**  \n"
                content += f"  `{full_url}`  \n\n"
            
            content += "---\n\n"
        
        # Footer
        total_files = sum(len(files) for files in categorized.values())
        content += f"""## Statistics

- **Files Indexed**: {total_files}
- **Categories**: {len(categorized)}
- **Structure Source**: {self.structure.get('source', 'auto-detected')}
- **Commit**: {commit_hash}

---

*Generated by HDI Repo-Indexer | Co-created by Ray Serrano & Aitana Catalyst*  
*Hemispheric Digital Integration - Two minds, one solution*
"""
        
        return content
    
    def generate_index(self) -> str:
        """Main method to generate the index"""
        print("🚀 HDI Repo-Indexer Starting...")
        print(f"📍 Repository: {self.repo_path.name}")
        print(f"🧠 Structure detection: {self.structure.get('source', 'auto')}")
        
        # Get commit hash
        commit_hash = self.get_latest_commit_hash()
        print(f"📌 Commit: {commit_hash[:8]}")
        
        # Get version
        version = self.get_next_version()
        print(f"📝 Version: {version:03d}")
        
        # Generate content
        content = self.generate_index_content(commit_hash, version)
        
        # Write file
        output_file = self.index_path / f"{self.repo_name}_index_v{version:03d}.md"
        output_file.write_text(content, encoding='utf-8')
        
        print(f"✅ Generated: {output_file.name}")
        
        # Generate access URL
        index_url = f"https://raw.githubusercontent.com/{self.github_user}/{self.repo_name}/refs/heads/{self.branch}/{output_file.relative_to(self.repo_path)}"
        index_url = index_url.replace('\\', '/')
        
        print(f"🔗 URL: {index_url}")
        print("✨ HDI Success - Intelligence² Achieved!")
        
        return output_file.name
    
    def load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        path = Path(config_path)
        if path.exists():
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        return {}


def main():
    """Run the HDI indexer"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='HDI Repository Indexer - Auto-detect, organize, defeat cache',
        epilog='Co-created with Intelligence² by Ray Serrano & Aitana Catalyst'
    )
    
    parser.add_argument('repo_path', help='Path to repository')
    parser.add_argument('--config', help='Optional config file (works without it!)')
    
    args = parser.parse_args()
    
    try:
        indexer = RepoIndexer(
            repo_path=args.repo_path,
            config_path=args.config
        )
        
        indexer.generate_index()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())