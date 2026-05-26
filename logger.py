from loguru import logger
import sys

def get_logger(name: str):
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan> | {message}",
        level="INFO"
    )
    logger.add(
        f"logs/{name}.log",
        rotation="10 MB",
        retention="14 days",
        level="DEBUG"
    )
    return logger.bind(name=name)
