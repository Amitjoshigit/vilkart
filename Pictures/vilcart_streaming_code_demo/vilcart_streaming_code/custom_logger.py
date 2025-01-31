# import logging

# def setup_logger(name):
#     logger = logging.getLogger(name)
#     logger.setLevel(logging.INFO)
#     handler = logging.StreamHandler()
#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)
#     return logger

import logging

def setup_logger(name):
    # Create a custom logger
    logger = logging.getLogger(name)
    
    # Set the default logging level (you can adjust this to DEBUG for all logs)
    logger.setLevel(logging.DEBUG)
    
    # Create a stream handler (for console logs)
    stream_handler = logging.StreamHandler()
    
    # Define a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    
    # Add the handler to the logger
    if not logger.handlers:  # Prevent duplicate handlers
        logger.addHandler(stream_handler)
    
    return logger
