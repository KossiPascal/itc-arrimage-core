import time
import threading
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import wraps

from flask import Flask
from flask_apscheduler import APScheduler
from apscheduler.triggers.cron import CronTrigger

from psycopg2 import pool, OperationalError, DatabaseError

from utils.config import config
from utils.dates_utils import get_previous_month
from make_arrimate import Dhis2ArrimateMaker
from clients.postgres_client import PostgresClient

from utils.models import RefreshToken, User, db

from routes.sync_routes_utils import sync_orgunits, sync_dataelements, sync_teis_enrollments_events_attributes

from utils.logger import get_logger, clear_logs
logger = get_logger(__name__)

lock = threading.Lock()


class SchedulerApp:
    """Reusable scheduler + DB pool + retry + matview refresher."""
    # INIT SHEDULER
    def __init__(self, app: Flask):
        self.app = app
        self.scheduler = APScheduler()
        self.scheduler.init_app(app)
        self.scheduler.start()

        self.db_pool = None
        self.view_name=config.MATVIEW_NAME
        self.view_field_id = 'uid'

        self.init_db_pool()
        # Schedule job immediately at startup
        self.register_jobs()

    # DB POOL
    def init_db_pool(self):
        if self.db_pool is None:
            logger.info("Initializing DB connection pool...")

            try:
                self.db_pool = pool.SimpleConnectionPool(
                    minconn=config.DB_MINCONN,
                    maxconn=config.DB_MAXCONN,
                    host=config.POSTGRES_HOST,
                    port=config.POSTGRES_PORT,
                    user=config.POSTGRES_USER,
                    password=config.POSTGRES_PASSWORD,
                    database=config.POSTGRES_DB,
                )
                logger.info("DB pool created successfully.")

            except Exception as e:
                logger.error(f"Failed to create DB pool: {e}", exc_info=True)
                raise

    # GET CONN POOL
    @contextmanager
    def get_conn_cursor(self):
        """Safe pooled connection with rollback + cleanup."""
        conn = None
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            yield conn, cur
            conn.commit()

        except Exception:
            if conn:
                conn.rollback()
            raise

        finally:
            if conn:
                try:
                    cur.close()
                except Exception:
                    pass
                self.db_pool.putconn(conn)

    # RETRY DECORATOR (STATIC)
    @staticmethod
    def retry():
        def decorator(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                attempts = 0
                sleep_delay = config.RETRY_DELAY
                backoff = config.BACK_OFF
                max_attempts = config.MAX_RETRIES

                while attempts < max_attempts:
                    try:
                        return fn(*args, **kwargs)
                    except (OperationalError, DatabaseError, Exception) as e:
                        attempts += 1
                        logger.warning("[Retry] Attempt %s/%s failed: %s", attempts, max_attempts, e)

                        if attempts >= max_attempts:
                            logger.error("[Retry] Max attempts reached.", exc_info=True)
                            raise

                        logger.info("[Retry] Retrying in %s seconds...", sleep_delay)
                        time.sleep(sleep_delay)
                        sleep_delay *= backoff
            return wrapper
        return decorator

    # MATERIALIZED VIEW REFRESH
    @retry()
    def refresh_materialized_view(self, concurrent=True, view_name=None, field_id=None):
        """Refresh MV with optional concurrency and safe index creation."""

        if not lock.acquire(blocking=False):
            logger.warning("MV refresh already in progress → SKIPPED")
            return False

        try:
            view = view_name or self.view_name
            field = field_id or self.view_field_id

            # VALIDATION DES PARAMÈTRES
            if not view or not field:
                raise ValueError("view_name and field_id must not be empty")

            # Empêche les injections SQL
            if ";" in view or ";" in field:
                raise ValueError("Invalid characters in view_name or field_id")

            # SQL préparés proprement
            key_sql = f"CREATE UNIQUE INDEX IF NOT EXISTS {view}_{field}x ON {view} ({field});"

            refresh_sql = (
                f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};"
                if concurrent else
                f"REFRESH MATERIALIZED VIEW {view};"
            )

            logger.info("Refreshing MV '%s' (concurrent=%s)", view, concurrent)

            start = datetime.utcnow()

            # EXÉCUTION AVEC TIMEOUT
            with self.get_conn_cursor() as (conn, cur):
                try:
                    # Timeout pour éviter blocage perpétuel
                    cur.execute("SET LOCAL statement_timeout = 600000;")
                except Exception as e:
                    logger.warning("Could not set statement_timeout: %s", e)

                # Création de l’index
                try:
                    cur.execute(key_sql)
                except Exception as e:
                    logger.error("Failed to create unique index: %s", e)
                    raise

                # Rafraîchissement de la MV
                try:
                    cur.execute(refresh_sql)
                except Exception as e:
                    logger.error("MV refresh failed (%s)", e)
                    raise

            duration = (datetime.utcnow() - start).total_seconds()
            logger.info("MV '%s' refreshed in %.2f seconds", view, duration)

            return True
        finally:
            lock.release()
            return False

    # AUTO ARRIMAGE
    @retry()
    def auto_indicators_arrimage(self):
        """
        Lance automatiquement l’arrimage des indicateurs
        pour le mois précédent, avec retry automatique via @retry.
        """
        period = get_previous_month(period_date=None)
        logger.info(f"[AUTO-ARRIMAGE] Starting for period: {period}")

        try:
            pg = PostgresClient()
            orgunit_ids = pg.list_orgunits(only_ids=True)
            # logger.info(f"[AUTO-ARRIMAGE] Orgunits found: {orgunit_ids}")
            arr = Dhis2ArrimateMaker(send_to_dhis2 = True, save_to_local_file = False)
            outputs = arr.start_indicators_arrimage_with_dhis2([period],orgunit_ids)

            result = { "success":0, "error":0 }
            for output in outputs:
                if output["status"] is True:
                    result["success"] += output["size"]
                else:
                    result["error"] += output["size"]

            if result["error"] > 0:
                raise RuntimeError(f'Arrimage returned success={result["success"]}, error={result["error"]}, for {period}')

            logger.info(f'[AUTO-ARRIMAGE] Completed successfully for {period}. Total={result["success"]}')
            return True

        except Exception as e:
            # Très important : LEVER l’exception pour activer le retry du décorateur
            logger.error(f"[AUTO-ARRIMAGE] ERROR: {e}", exc_info=True)
            raise

    # clear_app_logs Scheduler automatique
    @retry()
    def clear_app_logs(self):
        """Schedule clearing of logs every 10th day of the month at 00:00."""
        try:
            clear_logs()
            logger.info(f"[AUTO-CLEAR_APP_LOGS] SUCCESS", exc_info=True)
            return True
        except Exception as e:
            # Très important : LEVER l’exception pour activer le retry du décorateur
            logger.error(f"[AUTO-ARRIMAGE] ERROR: {e}", exc_info=True)
            raise
    
    # REFRESH JOB
    @retry()
    def refresh_mv_job(self):
        """Try refreshing the materialized view concurrently, fallback to non-concurrent if needed."""
        view = self.view_name
        field = self.view_field_id

        logger.info("Starting scheduled refresh for MV '%s'", view)

        # Première tentative : concurrent
        try:
            self.refresh_materialized_view(concurrent=True, view_name=view, field_id=field)
            logger.info("Concurrent refresh succeeded for '%s'", view)
            return
        except Exception as e:
            logger.warning("Concurrent refresh failed for '%s': %s", view, e)
            logger.info("Retrying NON-concurrent refresh in 1 second…")
            time.sleep(1)

        # Deuxième tentative : non-concurrent
        try:
            self.refresh_materialized_view(concurrent=False, view_name=view, field_id=field)
            logger.info("Non-concurrent refresh succeeded for '%s'", view)
        except Exception as e:
            logger.error("Non-concurrent refresh also failed for '%s': %s", view, e, exc_info=True)
            raise
        
    @retry()
    def auto_sync_orgunits_dataelements(self):
        """
        Automatic synchronization of orgunits and dataelements.
        Raises exception on failure to allow retry mechanism.
        """
        try:
            result1, status1 = sync_orgunits()
            result2, status2 = sync_dataelements()
            if status1 != 200 or status2 != 200:
                msg = f"[AUTO-sync_orgunits_dataelements] ERROR: Status codes {status1}, {status2}"
                logger.error(msg, exc_info=True)
                raise Exception(msg)
            
            logger.info("[AUTO-sync_orgunits_dataelements] Success")

            self.refresh_mv_job()
            return True
        except Exception as e:
            logger.error(f"[AUTO-sync_orgunits_dataelements] Exception: {e}", exc_info=True)
            raise  # propager l'erreur pour le retry

    @retry()
    def auto_sync_teis_enrollments_events_attributes(self):
        """
        Automatic synchronization of TEIs, enrollments, events, and attributes.
        Raises exception on failure to allow retry mechanism.
        """
        try:
            result, status = sync_teis_enrollments_events_attributes()
            if status != 200:
                msg = f"[AUTO-sync_teis_enrollments_events_attributes] ERROR: Status code {status}"
                logger.error(msg, exc_info=True)
                raise Exception(msg)
            logger.info("[AUTO-sync_teis_enrollments_events_attributes] Success")

            self.refresh_mv_job()
            return True
        except Exception as e:
            logger.error(f"[AUTO-sync_teis_enrollments_events_attributes] Exception: {e}", exc_info=True)
            raise  # propager l'erreur pour le retry
    
    @retry()
    def cleanup_old_refresh_tokens(older_than_days: int = 90):
        try:
            cutoff = datetime.utcnow() - timedelta(days=older_than_days)
            removed = RefreshToken.query.filter(
                RefreshToken.revoked == True,
                RefreshToken.issued_at < cutoff
            ).delete(synchronize_session=False)
            db.session.commit()
            logger.info("Cleanup removed %s old revoked refresh tokens", removed)
        except Exception:
            db.session.rollback()
            logger.exception("Failed cleaning up refresh tokens")
            raise

    # JOB REGISTRATION
    def register_jobs(self):
        """Register APScheduler jobs."""

        # 1️⃣ Cron jobs : chaque 8 du mois à 06:30
        self.scheduler.add_job(
            id="monthly_log_cleaner",
            func=self.clear_app_logs,
            trigger=CronTrigger(day=8, hour=6, minute=30, timezone="UTC"),
            # trigger="interval",
            # seconds=10,
            replace_existing=True,
            max_instances=1,
        )
        logger.info("Scheduled Cron jobs 'clear_app_logs' : chaque 8 du mois à 06:30 UTC")

        # 2️⃣ Cron jobs: chaque 9 du mois à 00:30
        self.scheduler.add_job(
            id="monthly_sync_orgunits_dataelements",
            func=self.auto_sync_orgunits_dataelements,
            trigger=CronTrigger(day=9, hour=0, minute=30, timezone="UTC"),
            # trigger="interval",
            # seconds=10,
            replace_existing=True,
            max_instances=1,
        )
        logger.info("Scheduled Cron jobs 'sync_orgunits_dataelements' : chaque 9 du mois à 00:30 UTC")

        # 3️⃣ Cron jobs: chaque 10 du mois à 01:00
        self.scheduler.add_job(
            id="monthly_sync_teis_enrollments_events_attributes",
            func=self.auto_sync_teis_enrollments_events_attributes,
            trigger=CronTrigger(day=10, hour=1, minute=0, timezone="UTC"),
            # trigger="interval",
            # seconds=10,
            replace_existing=True,
            max_instances=1,
        )
        logger.info("Scheduled Cron jobs 'sync_teis_enrollments_events_attributes' : chaque 10 du mois à 01:00 UTC")

        #✅ Cron jobs: chaque 15 du mois à minuit
        self.scheduler.add_job(
            id="monthly_indicators_arrimage",
            func=self.auto_indicators_arrimage,
            trigger=CronTrigger(day=15, hour=0, minute=0, timezone="UTC"),
            # trigger="interval",
            # seconds=10,
            replace_existing=True,
            max_instances=1,
        )
        logger.info("Scheduled Cron jobs 'auto_indicators_arrimage' : chaque 15 du mois à minuit UTC")

        # # Interval job every 180 seconds
        # self.scheduler.add_job(
        #     id="refresh_mv_interval",
        #     func=self.refresh_mv_job,
        #     trigger="interval",
        #     seconds=180,
        #     replace_existing=True,
        #     max_instances=1,
        # )
        # logger.info("Scheduled interval job 'refresh_mv_interval' chaque 180s")


    # MANUAL TRIGGER
    def manual_trigger(self):
        """Call this from Flask route if needed."""
        job_id = f"manual_refresh_{int(time.time())}"
        self.scheduler.add_job(id=job_id, func=self.refresh_mv_job, trigger="date")
        return {"status": "queued", "job_id": job_id}
