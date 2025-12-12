# run_scheduler.py
from server import create_app
from utils.logger import get_logger

logger = get_logger(__name__)

# Crée l'app Flask mais sans lancer le serveur web
app = create_app(init_scheduler=True)

# Scheduler déjà initialisé dans app
if hasattr(app, "scheduler"):
    logger.info("Scheduler process started. Jobs are scheduled automatically.")
    # app.scheduler.start()
else:
    raise RuntimeError("Scheduler non initialisé dans l'app")

# Empêche le script de se terminer pour que les jobs APScheduler continuent de tourner
try:
    import time
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    logger.info("Scheduler stopped manually.")
