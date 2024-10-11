"""
example.py - Template for Creating a New Module in Directory Indexing App

This module serves as a comprehensive template for developing new modules
for the directory indexing application. It outlines the necessary structure,
functions, and documentation required to integrate seamlessly with the app.

"""

# Module Metadata Constants
NAME = "example"                           # REQUIRED: Unique name of the module
FIELD_NAME = "example_version"              # REQUIRED: Field name for versioning
DATA_FIELD_NAMES = []                      # REQUIRED: Data fields to fetch for continue incrementally
MAX_WORKERS = 8                            # REQUIRED: Maximum concurrent workers
VERSION = 1                                # REQUIRED: Module version, will cause all match files to process again if incremented

# Define supported image MIME types
SUPPORTED_MIMES = {
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/tiff',
    'image/bmp',
    'image/webp'
}

def does_support_mime(mime):
    """
    REQUIRED
    
    Determine if the module supports a given MIME type.

    Args:
        mime (str): The MIME type of the file to check.

    Returns:
        bool: True if the MIME type is supported, False otherwise.
    """
    return mime in SUPPORTED_MIMES

async def init():
    """
    REQUIRED
    
    Initialize the module.

    This asynchronous function is called when the module is loaded.
    It can be used to set up resources, load models, or perform initial
    configurations required by the module.


    Returns:
        None
    """
    return

async def cleanup():
    """
    REQUIRED
    
    Clean up resources used by the module.

    This asynchronous function is called when the module is unloaded.
    It should handle the release of any resources or perform necessary
    cleanup tasks to ensure no residual effects remain.

    Returns:
        None
    """
    return

async def get_fields(file_path, doc):
    """
    REQUIRED
    
    Extract metadata fields from the file

    This asynchronous generator function processes the file located
    at `file_path`, extracts its metadata fields, and yields each field
    as a separate dictionary entry. Designed to handle incremental data
    building by yielding partial results.

    Args:
        file_path (str): The full path to the file.
        doc (dict): Document dictionary that will contain DATA_FIELD_NAMES from previous run if previously yielded. 
                    Also, has doc["type"] which is the mime type for the current file

    Yields:
        dict: A dictionary with extracted metadata fields. Can be incomplete and continued later using doc arg.
    """
