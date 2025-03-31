import logging

def setup_logger():
    """
    Налаштування логування.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)