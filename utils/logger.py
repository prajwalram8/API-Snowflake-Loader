import logging

def setup_logging(name):
    """
    Setup logger for a specific module.
    """
    logger = logging.getLogger(name)
    
    # Check if handlers have already been added to the root logger
    if not logger.hasHandlers():
        # Set the basic configuration for the root logger
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S %p',
                            filename='app.log',  # log to a file
                            filemode='a')  # append to the file, don't overwrite

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))

        logging.getLogger('').addHandler(console)
    
    return logger
