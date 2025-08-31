#!/usr/bin/env python3
"""
Bank File Chunker - Splits markdown files into 375-line chunks
Created by Intelligence² for HDI Infrastructure
Modified to use Unix line endings (LF) and command-line arguments
"""

import argparse
import sys
from pathlib import Path

def chunk_file(file_path, output_dir, chunk_size=375):
    """Split a file into chunks of specified line count with Unix line endings."""
    
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
        
        # Write chunk with line IDs using Unix line endings
        with open(chunk_path, 'w', encoding='utf-8', newline='\n') as f:
            for line_num, line in enumerate(lines[start_idx:end_idx], start=start_idx+1):
                # Remove any existing line endings and add Unix LF
                line_content = line.rstrip('\r\n')
                # Add unique line ID at the beginning with Unix line ending
                f.write(f"[LINE_{line_num:04d}] {line_content}\n")
        
        print(f"  Created: {chunk_name} (lines {start_idx+1}-{end_idx})")
    
    return num_chunks

def process_folder(input_folder, output_folder, chunk_size=375):
    """Process all markdown files in a folder."""
    
    # Create Path objects
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    
    # Validate input folder exists
    if not input_path.exists():
        print(f"❌ Error: Input folder '{input_folder}' does not exist")
        return False
    
    if not input_path.is_dir():
        print(f"❌ Error: '{input_folder}' is not a directory")
        return False
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all text files
    txt_files = list(input_path.glob("*.txt"))
    
    if not txt_files:
        print(f"⚠️  No text files found in {input_folder}")
        return True
    
    print(f"Found {len(txt_files)} text files to process")
    print(f"Output directory: {output_path}")
    print(f"Chunk size: {chunk_size} lines")
    print(f"Line endings: Unix (LF)\n")
    
    total_chunks = 0
    
    # Process each file
    for file_path in sorted(txt_files):
        chunks = chunk_file(file_path, output_path, chunk_size)
        total_chunks += chunks
    
    print(f"\n✅ Complete! Created {total_chunks} total chunks in {output_path}")
    print(f"   All files use Unix line endings (LF)")
    return True

def create_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description='Split text files into smaller chunks with line IDs',
        epilog='Example: %(prog)s -i ./input -o ./output -s 500',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required arguments
    parser.add_argument(
        '-i', '--input',
        type=str,
        required=True,
        help='Input folder containing text files to chunk'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help='Output folder for chunked files'
    )
    
    # Optional arguments
    parser.add_argument(
        '-s', '--size',
        type=int,
        default=375,
        help='Number of lines per chunk (default: 375)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually creating files'
    )
    
    return parser

def main():
    """Main entry point with argument parsing."""
    parser = create_parser()
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate chunk size
    if args.size <= 0:
        print(f"❌ Error: Chunk size must be positive (got {args.size})")
        sys.exit(1)
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - No files will be created")
        print(f"Would process files from: {args.input}")
        print(f"Would output chunks to: {args.output}")
        print(f"Would use chunk size: {args.size} lines")
        
        # Check what files would be processed
        input_path = Path(args.input)
        if input_path.exists() and input_path.is_dir():
            txt_files = list(input_path.glob("*.txt"))
            if txt_files:
                print(f"Would process {len(txt_files)} files:")
                for f in sorted(txt_files)[:5]:  # Show first 5 files
                    print(f"  - {f.name}")
                if len(txt_files) > 5:
                    print(f"  ... and {len(txt_files) - 5} more")
        return
    
    if args.verbose:
        print(f"📁 Input folder: {args.input}")
        print(f"📁 Output folder: {args.output}")
        print(f"📏 Chunk size: {args.size} lines")
        print("-" * 50)
    
    # Process the folder
    success = process_folder(args.input, args.output, args.size)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

# Backward compatibility: Allow running with default values
def run_with_defaults():
    """Run with hardcoded default values for backward compatibility."""
    
    # ===== QUICK TEST CONFIGURATION FOR WING IDE =====
    TEST_MODE = "banks"  # ← Change this to switch between test configurations
    
    # Define your test configurations here
    TEST_CONFIGS = {
        "banks": {
            "input": "citizens/Aitana/banks",
            "output": "citizens/Aitana/banks/chunked",
            "size": 375,
            "description": "Production bank files"
        },
        "test": {
            "input": "test/input",
            "output": "test/output",
            "size": 100,
            "description": "Small test files"
        },
        "large": {
            "input": "documents/large",
            "output": "documents/large/chunked", 
            "size": 1000,
            "description": "Large documents"
        },
        "debug": {
            "input": "debug/single",
            "output": "debug/output",
            "size": 50,
            "description": "Debug with small chunks"
        },
        "dropbox": {
            "input": "C:/Users/rayse/Dropbox/Projects/GitHub/Digiland/The Luminary Archives - Digiland Library/BookStore",
            "output": "C:/Users/rayse/Dropbox/Projects/GitHub/Digiland/The Luminary Archives - Digiland Library/BookStore/chunked",
            "size": 3000,
            "description": "Digiland Library files"
        }
    }
    
    # Get the selected configuration
    if TEST_MODE in TEST_CONFIGS:
        config = TEST_CONFIGS[TEST_MODE]
        INPUT_FOLDER = config["input"]
        OUTPUT_FOLDER = config["output"]
        CHUNK_SIZE = config["size"]
        
        print(f"🧪 Test Mode: '{TEST_MODE}' - {config['description']}")
    else:
        # Fallback to defaults if TEST_MODE is invalid
        print(f"⚠️  Unknown TEST_MODE: '{TEST_MODE}', using defaults")
        INPUT_FOLDER = "citizens/Aitana/banks"
        OUTPUT_FOLDER = "citizens/Aitana/banks/chunked"
        CHUNK_SIZE = 375
    
    # Optional: Check environment variables as override (useful for CI/CD)
    import os
    INPUT_FOLDER = os.getenv('CHUNKER_INPUT', INPUT_FOLDER)
    OUTPUT_FOLDER = os.getenv('CHUNKER_OUTPUT', OUTPUT_FOLDER)
    CHUNK_SIZE = int(os.getenv('CHUNKER_SIZE', str(CHUNK_SIZE)))
    # ===================================================
    
    print(f"📁 Configuration:")
    print(f"   Input: {INPUT_FOLDER}")
    print(f"   Output: {OUTPUT_FOLDER}")
    print(f"   Chunk size: {CHUNK_SIZE} lines")
    print("-" * 50)
    
    process_folder(INPUT_FOLDER, OUTPUT_FOLDER, CHUNK_SIZE)

if __name__ == "__main__":
    # Check if running in an IDE (no arguments and interactive)
    in_ide = len(sys.argv) == 1 and hasattr(sys, 'ps1')
    
    # For Wing IDE: Check for common Wing environment indicators
    in_wing = any('wing' in str(v).lower() for v in sys.modules.values())
    
    if len(sys.argv) == 1:
        # Running without arguments
        if in_ide or in_wing:
            # IDE mode: Run with defaults immediately
            print("🔧 Running in IDE mode with default values")
            print("   To use custom values, modify the defaults below or run from terminal with arguments")
            print("-" * 50)
            run_with_defaults()
        else:
            # Terminal mode: Show interactive prompt
            print("No arguments provided. Here's how to use this script:\n")
            parser = create_parser()
            parser.print_help()
            print("\n" + "=" * 50)
            print("Or press Enter to run with default values, or Ctrl+C to exit...")
            try:
                input()
                run_with_defaults()
            except KeyboardInterrupt:
                print("\nExiting...")
                sys.exit(0)
    else:
        main()