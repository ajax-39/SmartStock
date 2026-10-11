"""
Find the kagglehub cache directory for the downloaded dataset.

This utility helps locate where kagglehub downloaded the dataset,
which can then be used with the inspection script.
"""

import sys
from pathlib import Path
import kagglehub

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config_loader import get_config


def find_kagglehub_cache():
    """Find and display the kagglehub cache location."""
    
    config = get_config()
    dataset_id = config.get('kaggle.dataset_id')
    
    print("="*70)
    print("Find Kagglehub Cache Location")
    print("="*70)
    print(f"\nDataset: {dataset_id}")
    
    # Get the cache path
    print("\nAttempting to locate dataset...")
    
    try:
        # This will download if not present, or return existing path
        cache_path = kagglehub.dataset_download(dataset_id)
        
        print(f"\n✓ Dataset found at:")
        print(f"  {cache_path}")
        
        # List files in the cache
        cache_path = Path(cache_path)
        if cache_path.exists():
            print(f"\nFiles in cache directory:")
            for item in sorted(cache_path.rglob('*')):
                if item.is_file():
                    size_mb = item.stat().st_size / (1024 * 1024)
                    rel_path = item.relative_to(cache_path)
                    print(f"  - {rel_path} ({size_mb:.2f} MB)")
        
        print("\n" + "="*70)
        print("To inspect this dataset, run:")
        print(f'  python src/ingestion/inspect_dataset.py --path "{cache_path}"')
        print("="*70)
        
        return str(cache_path)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure Kaggle credentials are configured")
        print("2. Check internet connection")
        print("3. Verify dataset ID is correct")
        return None


if __name__ == "__main__":
    find_kagglehub_cache()
