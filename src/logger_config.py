import logging
import os


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


# Example usage:
if __name__ == '__main__':
    logger = setup_logger('test_logger', 'test.log')
    logger.info("Logger is configured!")
