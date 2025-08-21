import json
from celery import shared_task
from celery.utils.log import get_task_logger
from pontoon.orchestration.transfer import main


TASK_MAX_RETRIES = 3
TASK_RETRY_DELAY = 60*5

logger = get_task_logger(__name__)


@shared_task(bind=True)
def transfer_task(self, args_json: str):
    print("Running transfer job as celery task...")
    try:
        args = json.loads(args_json).get('commandArgs', [])
        if '--execution-id' not in args:
            # Generate a new execution ID if one is not provided
            args += ['--execution-id', str(self.request.id)]
        args += ['--retry-count', str(self.request.retries)]
        args += ['--retry-limit', str(TASK_MAX_RETRIES)]

        return json.loads(main(args))
    except Exception as e:
        logger.error("Caught unhandled exception from transfer job: ", e, f"(args={args_json})")
        raise self.retry(
            exc=e, 
            max_retries=TASK_MAX_RETRIES, 
            countdown=TASK_RETRY_DELAY
        )


# @shared_task(bind=True)
# def test_task(self, args_json: str):
#     print("Running transfer job as celery task...")
#     try:
#         args = json.loads(args_json).get('commandArgs', [])
#         args += ['--execution-id', self.request.id]
#         args += ['--retry-count', self.request.retries]
#         args += ['--retry-limit', 3]
        
#         return args
#     except Exception as e:
#         logger.error("Caught unhandled exception from test task: ", e, f"(args={args_json})")

