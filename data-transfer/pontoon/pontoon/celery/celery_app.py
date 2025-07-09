from celery import Celery

celery_app = Celery("pontoon")
celery_app.config_from_object("pontoon.celery.celeryconfig")
celery_app.autodiscover_tasks(["pontoon.celery"])
