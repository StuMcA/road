# Claude Rules for Road Quality Project

## Code Standards
- **No debugging prints**: Remove all `print()` statements except in test files
- **Clean imports**: Remove unused imports, organize by standard/third-party/local
- **Error handling**: Use exceptions, not print statements for errors
- **Type hints**: Always include type hints for function parameters and returns
- **Docstrings**: Include docstrings for all public methods and classes

## Code Styling
- **Newlines**: Files should end with exactly one newline character (only add if missing, don't add extra)
- **Line length**: Keep lines under 100 characters when possible
- **Indentation**: Use 4 spaces (no tabs)
- **Blank lines**: 
  - 2 blank lines before class definitions
  - 1 blank line before method definitions
  - 1 blank line after imports
- **Quotes**: Use double quotes for strings consistently
- **Trailing whitespace**: Remove all trailing spaces
- **Import organization**:
  ```python
  # Standard library
  import os
  from pathlib import Path

  # Third party
  import numpy as np
  import cv2

  # Local imports
  from .model import RoadQualityModel
  ```

## Architecture Rules
- **Single model**: Only use YOLOv8 - no multi-model support
- **Clean APIs**: Services should have minimal, focused interfaces
- **Metadata**: Always include timestamp, model name/version in outputs
- **No mock data**: Remove all mock/demo implementations

## File Organization
- **Tests in `/tests`**: All test scripts go in dedicated tests directory
- **Clean structure**: Remove unused files, keep only essential components
- **Dependencies**: Minimal requirements - only include what's actually used

## Git Rules
- **Ignore models**: All `.pt` files should be in `.gitignore`
- **No cache**: Keep `__pycache__`, `.pyc` files out of git
- **Clean commits**: Remove debug files before committing

## Testing Standards
- **Real data**: Use actual sample images, not synthetic data
- **Error validation**: Test both success and failure cases
- **Output verification**: Validate JSON structure and metadata presence

## Specific to Road Quality Service
- **Input**: Service takes image path (string)
- **Output**: Returns `RoadQualityMetrics` object with metadata
- **Dependencies**: OpenCV, NumPy, ultralytics only
- **Models**: YOLOv8 only, auto-download if needed
