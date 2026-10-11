"""
Download stock market data from Kaggle using kagglehub.

This script downloads the Indian stock market 1-minute frequency dataset
and copies it to the project's data/raw directory.
"""

import os
import sys
from pathlib import Path
import shutil
import kagglehub

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config_loader import get_config


def download_kaggle_dataset():
    """Download dataset from Kaggle and organize it in project structure."""
    
    # Load configuration
    config = get_config()
    dataset_id = config.get('kaggle.dataset_id')
    raw_data_path = Path(config.get('paths.raw_data'))
    
    print(f"{'='*70}")
    print(f"SmartStock - Kaggle Dataset Download")
    print(f"{'='*70}")
    print(f"\nDataset: {dataset_id}")
    print(f"Target directory: {raw_data_path}")
    
    # Create raw data directory if it doesn't exist
    raw_data_path.mkdir(parents=True, exist_ok=True)
    
    # Download dataset using kagglehub
    print(f"\n[1/3] Downloading dataset from Kaggle...")
    print("(This may take several minutes depending on dataset size)\n")
    
    try:
        # kagglehub downloads to its cache directory
        download_path = kagglehub.dataset_download(dataset_id)
        print(f"✓ Dataset downloaded to: {download_path}")
        
    except Exception as e:
        print(f"✗ Error downloading dataset: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure you have accepted the dataset terms on Kaggle")
        print("2. Verify your Kaggle API credentials are configured")
        print("   - Place kaggle.json in ~/.kaggle/ (Linux/Mac) or %USERPROFILE%/.kaggle/ (Windows)")
        print("3. Check your internet connection")
        return False
    
    # Copy files to project data directory
    print(f"\n[2/3] Copying files to project directory...")
    
    download_path = Path(download_path)
    files_copied = []
    
    try:
        # Copy all files from download directory
        for item in download_path.rglob('*'):
            if item.is_file():
                # Calculate relative path
                rel_path = item.relative_to(download_path)
                target_path = raw_data_path / rel_path
                
                # Create parent directories
                target_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Copy file
                shutil.copy2(item, target_path)
                files_copied.append(str(rel_path))
                print(f"  ✓ Copied: {rel_path}")
        
        print(f"\n✓ Successfully copied {len(files_copied)} file(s)")
        
    except Exception as e:
        print(f"✗ Error copying files: {e}")
        return False
    
    # Display summary
    print(f"\n[3/3] Download Summary")
    print(f"{'='*70}")
    print(f"Files downloaded: {len(files_copied)}")
    print(f"Location: {raw_data_path}")
    print(f"\nFiles:")
    for f in files_copied:
        file_path = raw_data_path / f
        size_mb = file_path.stat().st_size / (1024 * 1024)
        print(f"  - {f} ({size_mb:.2f} MB)")
    
    print(f"\n{'='*70}")
    print("✓ Download complete!")
    print(f"\nNext steps:")
    print(f"  1. Inspect the dataset: python scripts/inspect_dataset.py")
    print(f"  2. Review the discovered schema and statistics")
    print(f"{'='*70}")
    
    return True


if __name__ == "__main__":
    success = download_kaggle_dataset()
    sys.exit(0 if success else 1)
