import os

broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

# Optional: Celery task/result settings
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]

timezone = "UTC"
enable_utc = True

# Optional: Result expiration (in seconds)
result_expires = 3600

# Optional: Max retries and rate limiting (if needed)
task_acks_late = True
worker_prefetch_multiplier = 1
