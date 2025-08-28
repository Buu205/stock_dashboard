"""
VN Finance Dashboard - Source Package
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for proper imports
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

__version__ = "1.0.0"
__author__ = "Stock Dashboard Team"
