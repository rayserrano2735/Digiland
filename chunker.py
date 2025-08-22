#!/usr/bin/env python3
"""
Bank File Chunker - Splits markdown files into 375-line chunks
Created by Intelligence² for HDI Infrastructure
"""

import os
from pathlib import Path

def chunk_file(file_path, output_dir, chunk_size=375):
    """Split a file into chunks of specified line count."""
    
    # Read the file
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Get base filename without extension
    base_name = file_path.stem
    extension = file_path.suffix
    
    # Calculate number of chunks needed
    total_lines = len(lines)
    num_chunks = (total_lines + chunk_size - 1) // chunk_size
    
    print(f"Processing {base_name}{extension}: {total_lines} lines → {num_chunks} chunks")
    
    # Create chunks
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_lines)
        
        # Create chunk filename
        chunk_name = f"{base_name}_{i+1}{extension}"
        chunk_path = output_dir / chunk_name
        
        # Write chunk with line IDs
        with open(chunk_path, 'w', encoding='utf-8') as f:
            for line_num, line in enumerate(lines[start_idx:end_idx], start=start_idx+1):
                # Add unique line ID at the beginning
                f.write(f"[LINE_{line_num:04d}] {line}")
        
        print(f"  Created: {chunk_name} (lines {start_idx+1}-{end_idx})")
    
    return num_chunks

def process_folder(input_folder, output_folder, chunk_size=375):
    """Process all markdown files in a folder."""
    
    # Create Path objects
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all text files
    txt_files = list(input_path.glob("*.txt"))
    
    if not txt_files:
        print(f"No text files found in {input_folder}")
        return
    
    print(f"Found {len(txt_files)} text files to process")
    print(f"Output directory: {output_path}")
    print(f"Chunk size: {chunk_size} lines\n")
    
    total_chunks = 0
    
    # Process each file
    for file_path in sorted(txt_files):
        chunks = chunk_file(file_path, output_path, chunk_size)
        total_chunks += chunks
    
    print(f"\n✅ Complete! Created {total_chunks} total chunks in {output_path}")

# Main execution
if __name__ == "__main__":
    # Configure paths
    INPUT_FOLDER = "citizens/Aitana/banks"  # Banks folder
    OUTPUT_FOLDER = "citizens/Aitana/banks/chunked"  # Output subfolder
    CHUNK_SIZE = 375  # Lines per chunk (safely under 400 limit)
    
    # Run the chunker
    process_folder(INPUT_FOLDER, OUTPUT_FOLDER, CHUNK_SIZE)