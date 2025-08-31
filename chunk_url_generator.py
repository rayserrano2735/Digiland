import os
from pathlib import Path

def generate_chunk_urls(local_folder_path, github_base_url):
    """
    Generate raw GitHub URLs for all files in a local folder.
    
    Args:
        local_folder_path: Path to local folder containing chunk files
        github_base_url: Base raw GitHub URL ending with /
    
    Returns:
        List of complete URLs for all files
    """
    # Ensure base URL ends with /
    if not github_base_url.endswith('/'):
        github_base_url += '/'
    
    # Get all files in the folder
    chunk_files = []
    
    # Using Path for cross-platform compatibility
    folder = Path(local_folder_path)
    
    # Get all .txt files, sorted naturally
    txt_files = sorted(folder.glob('*.txt'))
    
    # Generate URLs
    urls = []
    for file_path in txt_files:
        filename = file_path.name
        full_url = f"{github_base_url}{filename}"
        urls.append(full_url)
        print(f"{filename}: {full_url}")
    
    # Also save to a file for easy copying
    output_file = folder / "chunk_urls_full.txt"
    with open(output_file, 'w') as f:
        for url in urls:
            f.write(f"{url}\n")
    
    print(f"\nTotal files found: {len(urls)}")
    print(f"URLs saved to: {output_file}")
    
    return urls

# Example usage:
if __name__ == "__main__":
    # Adjust these paths to match your setup
    #LOCAL_FOLDER = "/path/to/your/local/Digiland/citizens/Aitana/banks/chunked"
    LOCAL_FOLDER = "citizens/Aitana/banks/chunked"
    GITHUB_BASE = "https://raw.githubusercontent.com/rayserrano2735/Digiland/refs/heads/main/citizens/Aitana/banks/chunked/"
    
    # Generate URLs
    urls = generate_chunk_urls(LOCAL_FOLDER, GITHUB_BASE)
    
    # Optional: Generate for specific bank only
    # You could filter by filename pattern, e.g. "Aitana_50_*.txt"
    bank_50_urls = [url for url in urls if "Aitana_50_" in url]
    print(f"\nBank 50 chunks: {len(bank_50_urls)}")
