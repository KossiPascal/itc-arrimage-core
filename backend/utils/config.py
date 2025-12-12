import os
from dotenv import load_dotenv

# Load .env (optional)
load_dotenv(override=True)


class Config:
    APP_VERSION = int(os.getenv("APP_VERSION", 1))

    APP_ENV = os.getenv("APP_ENV", "production")
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "5801"))
    DEBUG_MODE = os.getenv("APP_ENV", "production") == "development"
    
    JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
    JWT_SECRET = os.getenv("JWT_SECRET", "change_this_secret_in_prod")
    ACCESS_TOKEN_EXPIRES_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRES_MINUTES", 15))  # 15 min default
    REFRESH_TOKEN_EXPIRES_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRES_DAYS", 30))  # 30 days

    TOGO_DHIS2_URL = os.getenv('TOGO_DHIS2_URL')
    TOGO_DHIS2_USER = os.getenv('TOGO_DHIS2_USER')
    TOGO_DHIS2_PASS = os.getenv('TOGO_DHIS2_PASS')

    DHIS2_URL = os.getenv('DHIS2_URL')
    DHIS2_USER = os.getenv('DHIS2_USER')
    DHIS2_PASS = os.getenv('DHIS2_PASS')
    PROGRAM_TRACKER_ID = os.getenv('PROGRAM_TRACKER_ID')
    LAST_SYNC_FILE = os.getenv('LAST_SYNC_FILE')

    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', "5432")
    POSTGRES_DB = os.getenv('POSTGRES_DB')
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

    FORCE_INIT_CLASS = os.getenv('FORCE_INIT_CLASS') == 'true'


    DB_MINCONN = int(os.getenv("DB_MINCONN", 1))
    DB_MAXCONN = int(os.getenv("DB_MAXCONN", 5))

    SCHEDULER_INTERVAL_MINUTES = int(os.getenv('SCHEDULER_INTERVAL_MINUTES', '30'))

    MATVIEW_NAME = 'indicators_matview'



    USE_SSL = os.getenv('USE_SSL', 'true') == 'true'
    TIMEOUT = int(os.getenv('TIMEOUT', '60'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '3'))
    BACK_OFF = int(os.getenv('BACK_OFF', '2'))
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '50'))
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10000'))


    # APScheduler configuration keys (Flask-APScheduler expects APSCHEDULER_* keys)
    SCHEDULER_API_ENABLED = os.getenv("SCHEDULER_API_ENABLED", "false") == 'true'
    APSCHEDULER_TIMEZONE = os.getenv("APSCHEDULER_TIMEZONE", "UTC")
    APSCHEDULER_EXECUTORS = {"default": {"type": "threadpool", "max_workers": int(os.getenv("SCHED_MAX_WORKERS", 10))},}
    APSCHEDULER_JOB_DEFAULTS = {
        "coalesce": False,              # do not combine missed runs
        "max_instances": int(os.getenv("SCHED_MAX_INSTANCES", 1)),
    }
    APSCHEDULER_JOBS = [
        # job definition can also be added programmatically; we show it here as an example.
        # We'll add the refresh job programmatically (below) so you can see how to handle replace_existing etc.
    ]
    # Optional: jobstore (uncomment if you want persistence)
    # APSCHEDULER_JOBSTORES = {
    #     "default": {"type": "sqlalchemy", "url": os.getenv("SCHED_JOBSTORE_URL", "sqlite:///jobs.sqlite")}
    # }


    @property
    def DEFAULT_ADMIN(self):
        return {
            "fullname": os.getenv("DEFAULT_ADMIN_FULLNAME", "Admin"),
            "username": os.getenv("DEFAULT_ADMIN_USERNAME", "admin"),
            "password": os.getenv("DEFAULT_ADMIN_PASSWORD", "admin123"),
            "role": "superadmin"
        }

    @property
    def DATABASE_URL(self):
        # return os.getenv("DATABASE_URL", "sqlite:///data.db")  # change to postgres URL in prod
        return (
            f"dbname={self.POSTGRES_DB} "
            f"user={self.POSTGRES_USER} "
            f"password={self.POSTGRES_PASSWORD} "
            f"host={self.POSTGRES_HOST} "
            f"port={self.POSTGRES_PORT}"
        )

    @property
    def SQLALCHEMY_DATABASE_URI(self):
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:"
            f"{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:"
            f"{self.POSTGRES_PORT}/"
            f"{self.POSTGRES_DB}"
        )

    SQLALCHEMY_TRACK_MODIFICATIONS = False

# instance global
config = Config()