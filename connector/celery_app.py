from celery import Celery


def create_celery():
    app = Celery(
        'Celery app',
        imports=('pipeline.tasks',)
    )
    app.config_from_object('celeryconfig', force=True)
    return app


celery_app = create_celery()
