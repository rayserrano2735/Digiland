#!/usr/bin/env python3
"""
Luminary Library Auto-Indexer
Generates versioned index with commit-hash URLs to defeat cache issues
"""

import os
import subprocess
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class LuminaryIndexer:
    def __init__(self, repo_path: str, config_path: Optional[str] = None):
        self.repo_path = Path(repo_path)
        
        # Load configuration
        if config_path:
            self.config = self.load_config(config_path)
        else:
            # Look for config in standard locations
            possible_configs = [
                self.repo_path / "indexer_config.yaml",
                self.repo_path / "The Luminary Archives - Digiland Library" / "indexer_config.yaml",
                Path("indexer_config.yaml")
            ]
            for config_file in possible_configs:
                if config_file.exists():
                    self.config = self.load_config(str(config_file))
                    break
            else:
                raise FileNotFoundError("No indexer_config.yaml found. Please provide config path.")
        
        # Set attributes from config
        self.github_user = self.config['github']['user']
        self.repo_name = self.config['github']['repo']
        self.branch = self.config['github']['branch']
        self.ll_path = self.repo_path / self.config['paths']['library_folder']
    
    def load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
        
    def get_latest_commit_hash(self) -> str:
        """Get the latest commit hash from git."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error getting commit hash: {e}")
            # Fallback to 'main' branch if git command fails
            return "refs/heads/main"
    
    def get_next_version(self) -> int:
        """Get the next version number for the index."""
        pattern = self.config['index_filename_pattern'].replace('{version:03d}', '*')
        pattern = pattern.replace('{version}', '*')
        existing = list(self.ll_path.glob(pattern))
        
        if existing:
            versions = []
            for f in existing:
                try:
                    # Extract version number from filename
                    name = f.stem
                    if '_v' in name:
                        version = int(name.split('_v')[1])
                        versions.append(version)
                except (IndexError, ValueError):
                    continue
            return max(versions) + 1 if versions else 1
        return 1
    
    def should_exclude(self, path: Path) -> bool:
        """Check if a file or folder should be excluded."""
        # Check folder exclusions
        for folder in self.config['exclude']['folders']:
            if folder in path.parts:
                return True
        
        # Check file exclusions
        for pattern in self.config['exclude']['files']:
            if path.match(pattern):
                return True
        
        return False
    
    def is_valid_file_type(self, file: str) -> bool:
        """Check if file has a valid extension."""
        return any(file.endswith(ext) for ext in self.config['file_types'])
    
    def categorize_files(self) -> Dict[str, List[Tuple[str, str]]]:
        """Organize files by category based on folder structure."""
        categories = {cat: [] for cat in self.config['categories'].keys()}
        categories[self.config['default_category']] = []
        
        for root, dirs, files in os.walk(self.ll_path):
            # Filter out excluded directories
            dirs[:] = [d for d in dirs if not self.should_exclude(Path(root) / d)]
            
            for file in files:
                file_path = Path(root) / file
                
                # Skip if should be excluded
                if self.should_exclude(file_path):
                    continue
                    
                # Only index valid file types
                if not self.is_valid_file_type(file):
                    continue
                
                relative_path = file_path.relative_to(self.repo_path)
                
                # Determine category
                category = self.config['default_category']
                path_str = str(relative_path).lower()
                
                for cat_name, patterns in self.config['categories'].items():
                    if any(pattern.lower() in path_str for pattern in patterns):
                        category = cat_name
                        break
                
                # Format display name based on config
                display_name = self.format_display_name(file)
                
                categories[category].append((display_name, str(relative_path)))
        
        # Remove empty categories
        return {k: v for k, v in categories.items() if v}
    
    def format_display_name(self, filename: str) -> str:
        """Format filename for display based on config."""
        display_name = filename
        
        if self.config['formatting']['remove_extension']:
            display_name = display_name.rsplit('.', 1)[0]
        
        if self.config['formatting']['prettify_filenames']:
            display_name = display_name.replace('_', ' ').replace('-', ' ')
        
        if self.config['formatting']['title_case']:
            display_name = display_name.title()
        
        return display_name
    
    def generate_index_content(self, commit_hash: str, version: int) -> str:
        """Generate the markdown content for the index."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        base_url = f"https://raw.githubusercontent.com/{self.github_user}/{self.repo_name}/{commit_hash}"
        
        # Build metadata section based on config
        metadata_parts = []
        if self.config['metadata']['show_version']:
            metadata_parts.append(f"*Auto-Generated Index - Version {version:03d}*")
        if self.config['metadata']['show_timestamp']:
            metadata_parts.append(f"*Generated: {timestamp}*")
        if self.config['metadata']['show_commit_hash']:
            metadata_parts.append(f"*Commit: {commit_hash[:8]}*")
        
        metadata_section = "  \n".join(metadata_parts)
        
        content = f"""# The Luminary Archives Index
## The Complete Library of Digital Liberation & Consciousness
{metadata_section}

---

## Navigation

This index provides cache-busting URLs using commit hashes. Each file URL will return the latest content.

---

"""
        
        categories = self.categorize_files()
        
        for category, files in categories.items():
            content += f"## {category}\n\n"
            
            for display_name, relative_path in sorted(files):
                # Convert Windows paths to URL format
                url_path = relative_path.replace('\\', '/')
                full_url = f"{base_url}/{url_path}"
                
                content += f"- **{display_name}**  \n"
                content += f"  {full_url}  \n\n"
            
            content += "---\n\n"
        
        # Add metadata footer if configured
        footer_parts = []
        if self.config['metadata']['show_version']:
            footer_parts.append(f"- **Version**: {version:03d}")
        if self.config['metadata']['show_timestamp']:
            footer_parts.append(f"- **Generated**: {timestamp}")
        if self.config['metadata']['show_commit_hash']:
            footer_parts.append(f"- **Commit Hash**: {commit_hash}")
        if self.config['metadata']['show_file_count']:
            footer_parts.append(f"- **Total Files**: {sum(len(files) for files in categories.values())}")
        if self.config['metadata']['show_category_count']:
            footer_parts.append(f"- **Categories**: {len(categories)}")
        
        if footer_parts:
            content += "## Metadata\n\n"
            content += "\n".join(footer_parts)
            content += "\n\n---\n\n"
        
        content += """## Usage Instructions

1. This index uses commit-hash URLs to bypass cache issues
2. Each URL will always return the latest version of the file
3. New index versions are generated after repository updates
4. Save this index URL for future reference

---

*Generated by Luminary Library Auto-Indexer*  
*Part of the Digital Liberation Infrastructure*
"""
        
        return content
    
    def generate_index(self) -> str:
        """Main method to generate the index."""
        print("🚀 Starting Luminary Library Auto-Indexer...")
        
        # Get commit hash
        commit_hash = self.get_latest_commit_hash()
        print(f"📌 Using commit hash: {commit_hash[:8]}")
        
        # Get version number
        version = self.get_next_version()
        print(f"📝 Generating version: {version:03d}")
        
        # Generate content
        content = self.generate_index_content(commit_hash, version)
        
        # Build output filename from pattern
        output_filename = self.config['index_filename_pattern'].format(
            version=version,
            date=datetime.now().strftime("%Y%m%d"),
            timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        )
        
        # Write to file
        output_file = self.ll_path / output_filename
        output_file.write_text(content, encoding='utf-8')
        
        print(f"✅ Index generated: {output_file.name}")
        
        # Generate the GitHub URL for the new index
        index_url = f"https://raw.githubusercontent.com/{self.github_user}/{self.repo_name}/refs/heads/{self.branch}/{output_file.relative_to(self.repo_path)}"
        index_url = index_url.replace('\\', '/')
        
        print(f"🔗 Access URL: {index_url}")
        
        return output_file.name


def main():
    """Run the indexer."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate Luminary Library Index")
    parser.add_argument("repo_path", help="Path to local Digiland repository")
    parser.add_argument("--config", help="Path to configuration YAML file")
    
    args = parser.parse_args()
    
    try:
        indexer = LuminaryIndexer(
            repo_path=args.repo_path,
            config_path=args.config
        )
        
        indexer.generate_index()
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("Please create an indexer_config.yaml file or specify path with --config")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())