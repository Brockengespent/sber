import os
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent

# -------- Core env --------
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-insecure-key")  # override in prod
DEBUG = os.environ.get("DEBUG", "False").lower() == "true"

# SITE_URL может быть полным URL (https://example.com) или хостом (example.com)
SITE_URL = os.environ.get("SITE_URL")  # например: https://myapp.onrender.com
ALLOWED_HOSTS = []
CSRF_TRUSTED_ORIGINS = []

if SITE_URL:
    # гарантируем схему
    if "://" not in SITE_URL:
        SITE_URL = f"https://{SITE_URL}"
    parsed = urlparse(SITE_URL)
    host = parsed.netloc or parsed.path
    ALLOWED_HOSTS = [host, "127.0.0.1", "localhost"]
    CSRF_TRUSTED_ORIGINS = [f"{parsed.scheme}://{host}"]
else:
    # на всякий случай для первичного прогона
    ALLOWED_HOSTS = ["*"] if DEBUG else []

# -------- Media/Static --------
MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

STATIC_URL = "/static/"
# На PaaS статика должна собираться в каталог, доступный веб-серверу
STATIC_ROOT = BASE_DIR / "staticfiles"

# WhiteNoise: компрессия и кеширование статических файлов
STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

# -------- Django apps --------
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "core",
    "rest_framework",
    "django.contrib.humanize",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",  # WhiteNoise сразу после SecurityMiddleware
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "sber1.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "sber1.wsgi.application"

# -------- Database --------
# Позволим использовать готовый DATABASE_URL (postgres://...), иначе — fallback к локальному
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    # Для простоты используем dj_database_url, если добавите в requirements:
    # import dj_database_url
    # DATABASES = {"default": dj_database_url.parse(DATABASE_URL, conn_max_age=600)}
    # Временно парсим вручную только Postgres:
    from urllib.parse import urlparse as _u

    _p = _u(DATABASE_URL)
    DB_NAME = _p.path.lstrip("/")
    DB_USER = _p.username
    DB_PASSWORD = _p.password
    DB_HOST = _p.hostname
    DB_PORT = _p.port or "5432"
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": DB_NAME,
            "USER": DB_USER,
            "PASSWORD": DB_PASSWORD,
            "HOST": DB_HOST,
            "PORT": DB_PORT,
        }
    }
else:
    # локальный fallback
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("DB_NAME", "cber"),
            "USER": os.environ.get("DB_USER", "postgres"),
            "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
            "HOST": os.environ.get("DB_HOST", "localhost"),
            "PORT": os.environ.get("DB_PORT", "5432"),
            # Если нужен search_path, можно передать через DB_OPTIONS
            "OPTIONS": {},
        }
    }

# -------- Password validators --------
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# -------- I18N --------
LANGUAGE_CODE = "en-us"
TIME_ZONE = os.environ.get("TIME_ZONE", "UTC")
USE_I18N = True
USE_TZ = True

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# -------- Security for production --------
USE_HTTPS = os.environ.get("USE_HTTPS", "true").lower() == "true"
if USE_HTTPS and not DEBUG:
    SECURE_SSL_REDIRECT = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_HSTS_SECONDS = int(os.environ.get("SECURE_HSTS_SECONDS", "31536000"))
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# -------- Logging to stdout/stderr (для PaaS) --------
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": os.environ.get("LOG_LEVEL", "INFO")},
    "loggers": {
        "django.db.backends": {
            "handlers": ["console"],
            "level": os.environ.get("DB_LOG_LEVEL", "WARNING"),
        },
    },
}
