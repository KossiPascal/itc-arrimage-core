import os
import logging
from logging.handlers import RotatingFileHandler

# -----------------------------
# Configuration générale
# -----------------------------
LOG_DIR = "logs"
DEFAULT_MAX_SIZE = 5 * 1024 * 1024  # 5 MB
DEFAULT_BACKUP_COUNT = 3

# -----------------------------
# Logger réutilisable
# -----------------------------
def get_logger(
    name: str,
    level: str = "INFO",
    log_to_file: bool = True,
    filename: str = None,
    max_size: int = DEFAULT_MAX_SIZE,
    backup_count: int = DEFAULT_BACKUP_COUNT,
    console: bool = True
):
    """
    Create & configure a reusable logger with:
    - file logging (optional)
    - console logging (optional)
    - rotation by file size
    """

    logger = logging.getLogger(name)

    # Avoid duplicated handlers on reload
    if logger.handlers:
        return logger

    logger.setLevel(level.upper())

    # Default filename if not provided
    if log_to_file:
        os.makedirs(LOG_DIR, exist_ok=True)

        log_filename = filename or name
        filePath = os.path.join(LOG_DIR, f"{log_filename.replace('.', '_')}.log")

        file_handler = RotatingFileHandler(
            filename=filePath,
            maxBytes=max_size,
            backupCount=backup_count,
            encoding="utf-8"
        )

        file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger


# -----------------------------
# Fonction pour vider les logs
# -----------------------------
def clear_logs():
    """
    Clear all .log files in LOG_DIR safely, including files currently
    used by any FileHandler. Logs the operation without touching active handlers.
    """
    logger = logging.getLogger("log_cleaner")
    if not logger.handlers:
        # Prevent issues if logger has no handlers yet
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if not os.path.exists(LOG_DIR):
        logger.warning(f"Log directory {LOG_DIR} does not exist.")
        return

    log_files = [f for f in os.listdir(LOG_DIR) if f.endswith(".log")]

    for file in log_files:
        file_path = os.path.join(LOG_DIR, file)
        try:
            # Ne pas toucher aux handlers actifs pour éviter ValueError
            # On crée un fichier temporaire et on écrase l'ancien
            temp_path = file_path + ".tmp_clear"
            open(temp_path, 'w', encoding='utf-8').close()  # fichier vide
            os.replace(temp_path, file_path)  # écrase le fichier original
            logger.info(f"Cleared log file safely: {file_path}")
        except Exception as e:
            logger.error(f"Failed to clear log file {file_path}: {e}")

# def clear_logs():
#     """
#     Clear all .log files in the log directory safely, including those
#     currently used by any logger.
#     """
#     logger = get_logger("log_cleaner")

#     if not os.path.exists(LOG_DIR):
#         logger.warning(f"Log directory {LOG_DIR} does not exist.")
#         return
    
#     log_files = [f for f in os.listdir(LOG_DIR) if f.endswith(".log")]

#     for file in log_files:
#         file_path = os.path.join(LOG_DIR, file)
#         try:
#             # Truncate active handlers if they exist
#             truncated = False
#             for logger_name, log_obj in logging.root.manager.loggerDict.items():
#                 if isinstance(log_obj, logging.Logger):
#                     for handler in log_obj.handlers:
#                         if isinstance(handler, logging.FileHandler):
#                             if os.path.abspath(handler.baseFilename) == os.path.abspath(file_path):
#                                 handler.acquire()
#                                 try:
#                                     handler.flush()
#                                     handler.stream.seek(0)
#                                     handler.stream.truncate()
#                                     truncated = True
#                                 finally:
#                                     handler.release()
#             # Fallback if no active handler found
#             if not truncated:
#                 with open(file_path, 'w', encoding='utf-8') as f:
#                     f.truncate(0)
#             # Log success AFTER truncation
#             temp_logger = get_logger("log_cleaner", log_to_file=False)
#             temp_logger.info(f"Cleared log file: {file_path}")
#         except Exception as e:
#             temp_logger = get_logger("log_cleaner", log_to_file=False)
#             temp_logger.error(f"Failed to clear log file {file_path}: {e}")



