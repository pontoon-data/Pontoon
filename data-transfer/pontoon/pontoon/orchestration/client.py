import uuid
import boto3
import json
import time
import os
from typing import List, Dict, Tuple
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from pontoon import Mode, logger

from celery.schedules import crontab
from pontoon.celery.tasks import transfer_task
from pontoon.celery.celery_app import celery_app
from redbeat import RedBeatSchedulerEntry



class TransferException(Exception):
    """ Represents an exception when creating and managing transfers """ 
    pass


class Transfer:
    """ Represents a data transfer pipeline with a source, destination and replication options """

    # schedule expression constants
    NOW = '__now__'

    # schedule state constants
    PENDING = 'PENDING'
    STARTED = 'STARTED'
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    RETRY = 'RETRY'
    REVOKED = 'REVOKED'
    
    # config options
    SCHEDULE_NAME_PREFIX = None
    PONTOON_API_ENDPOINT = None

    @staticmethod
    def configure(
        schedule_name_prefix:str="PontoonSchedule-", 
        pontoon_api_endpoint:str=None
        ):

        # configure Transfer resources and dependencies
        Transfer.SCHEDULE_NAME_PREFIX = schedule_name_prefix
        Transfer.PONTOON_API_ENDPOINT = pontoon_api_endpoint

    
    @staticmethod
    def create(uuid:str) -> 'Transfer':
        return Transfer(uuid)
    

    @staticmethod
    def test_source(organization_id:str, source_id:str, transfer_id:str=None, run_async:bool=False):
        
        # test a source connection
        job_transfer_id = transfer_id or str(uuid.uuid4())  
        job = Transfer.create(job_transfer_id)
        job.set_command('source-check')
        job.set_organization(organization_id)
        job.set_argument('--source-id', source_id)
        job.run()

        if run_async is False:
            status = job.wait()
            if status == Transfer.SUCCESS:
                output_json = job.output()
                return output_json.get('success'), output_json.get('message')
            else:
                return False, job.error()
        else:
            return job_transfer_id


    
    @staticmethod
    def test_destination(organization_id:str, destination_id:str, transfer_id:str=None, run_async:bool=False):
        
        # test a destination by doing a synthetic incremental load over 2 days
        job_transfer_id = transfer_id or str(uuid.uuid4()) 
        job = Transfer.create(job_transfer_id)
        job.set_organization(organization_id)
        job.set_destination(destination_id)
        job.set_argument('--drop-after-complete', True)
        
        # hardcoded test model that belongs to a memory source
        job.set_models(['c871c3dd-0be2-45ae-b1a0-86a430095a32'])
        
        # force it to transfer one day of data from the test data set
        job.set_mode(Mode({
            'type': Mode.INCREMENTAL, 
            'start': datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 
            'end': datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        }))
        job.run()

        if run_async is False:
            status = job.wait()
            if status != Transfer.SUCCESS:
                error_msg = job.error()
                return False, error_msg
            else:
                return True, job.output()
        else:
            return job_transfer_id

    

    @staticmethod
    def inspect_source(organization_id:str, source_id:str, transfer_id:str=None, run_async:bool=False):
         
        # inspect and return schema info for a source
        job_transfer_id = transfer_id or str(uuid.uuid4())  
        job = Transfer.create(job_transfer_id)
        job.set_command('source-inspect')
        job.set_organization(organization_id)
        job.set_argument('--source-id', source_id)
        job.run()

        if run_async is False:
            status = job.wait()
            if status == Transfer.SUCCESS:
                output_json = job.output()
                return output_json.get('success'), output_json.get('stream_info')
            else:
                return False, job.error()
        else:
            return job_transfer_id


    @staticmethod
    def crontab_to_string(ct:crontab) -> str:
        return f"{ct._orig_minute} {ct._orig_hour} {ct._orig_day_of_month} {ct._orig_month_of_year} {ct._orig_day_of_week}"


    def __init__(self, uuid:str):
        
        self._uuid = uuid
        self._schedule_name = f"{Transfer.SCHEDULE_NAME_PREFIX}{self._uuid}"
        self._schedule_expr = None
 
        self._command = 'transfer'
        self._arguments = {}
        self._api_endpoint = Transfer.PONTOON_API_ENDPOINT

        self._celery_request = None
    

    def _build_command_args(self, launch_type=None) -> List[str]:
        # build the command line args for the transfer job
        args = []
        args += ['--command', self._command]
        args += ['--transfer-id', self._uuid]

        for arg, val in self._arguments.items():
            args += [arg, json.dumps(val) if isinstance(val, (dict,list)) else str(val)]
        
        if self._api_endpoint:
             args += ['--api-endpoint', self._api_endpoint]
        
        result = {'commandArgs': args}

        return json.dumps(result)

    
    def _sync_existing_args(self) -> bool:
        # sync an existing schedules arguments into this Transfer
        try:
            
            schedule_definition = RedBeatSchedulerEntry.load_definition(
                RedBeatSchedulerEntry(self._schedule_name, app=celery_app).key, 
                app=celery_app
            )

            input_json = schedule_definition.get('args', [])

            if not input_json:
                raise TransferException(f"Transfer {self._uuid} does not have inputs configured")
            
            input_obj = json.loads(input_json[0])
            args = input_obj.get('commandArgs', {})
            args_dict = {}
            
            # list of command args should be even length because it's key,value,key,value
            if len(args) % 2 != 0:
                raise TransferException(f"Transfer {self._uuid} invalid args: {args}")

            # parse list of command line args into a dictionary
            for i in [j for j in range(len(args)) if j % 2 == 0]:
                args_dict[args[i]] = args[i+1]
            
            # these don't get synced from existing
            if '--command' in args_dict:
                del args_dict['--command']

            if '--transfer-id' in args_dict:
                del args_dict['--transfer-id']

            # set our params from existing command
            if self._schedule_expr is None:
                self.schedule_expr = Transfer.crontab_to_string(
                    schedule_definition.get('schedule', None)
                )

            if '--api-endpoint' in args_dict and self._api_endpoint is None:
                self._api_endpoint = args_dict['--api-endpoint']
                del args_dict['--api-endpoint']

            for arg, val in args_dict.items():
                if arg not in self._arguments:
                    self._arguments[arg] = val

            return True

        except ClientError as e:
            raise TransferException(e)
        

    def _create(self) -> bool:
        # Create the schedule
        try:
            entry = RedBeatSchedulerEntry(
                self._schedule_name, 
                transfer_task.name, 
                crontab.from_string(self._schedule_expr), 
                app=celery_app, 
                args=(self._build_command_args(),)
            )
            entry.save()

            return True
        except Exception as e:
            raise TransferException(e)


    def _update(self, enabled:bool=True) -> bool:
        # Update the schedule
        self._sync_existing_args()
        try:
            entry = RedBeatSchedulerEntry.from_key(
                RedBeatSchedulerEntry(self._schedule_name, app=celery_app).key, 
                app=celery_app
            )
            entry.enabled = enabled
            entry.schedule = crontab.from_string(self._schedule_expr)
            entry.args = (self._build_command_args(),)
            entry.save()

            return True
        except Exception as e:
            raise TransferException(e)


    def uuid(self):
        # get this Transfer's UUID
        return self._uuid
    
    
    def exists(self) -> bool:
        # check if our schedule already exists
        try:
            RedBeatSchedulerEntry.load_definition(
                RedBeatSchedulerEntry(self._schedule_name, app=celery_app).key, 
                app=celery_app
            )
            return True
        except KeyError as e:
            return False

    
    def is_enabled(self) -> bool:
        # does our step trigger exist and is it enabled
        if self.exists() is False:
            raise TransferException(f"Transfer {self._uuid} does not exist")
        
        entry = RedBeatSchedulerEntry.from_key(
            RedBeatSchedulerEntry(self._schedule_name, app=celery_app).key, 
            app=celery_app
        )
        
        return entry.enabled 



    def enable(self) -> bool:
        # enable the trigger
        return self._update(enabled=True)


    def disable(self) -> bool:
        # disable the trigger
        return self._update(enabled=False)
    

    def status(self):
        # check task status
        if not self._celery_request:
            raise TransferException(f"Transfer {self._uuid} does not have an active task")

        return self._celery_request.state


    def wait(self, timeout=300, check_interval=3):
        # wait sync for task to complete 
        if not self._celery_request:
            raise TransferException(f"Transfer {self._uuid} does not have an active task")

        self._celery_request.get(
            timeout=timeout, 
            interval=check_interval
        )

        return self._celery_request.state


    def output(self):
        # return output from latest execution
        if not self._celery_request:
            raise TransferException(f"Transfer {self._uuid} does not have an active task")
        
        if not self._celery_request.ready():
            raise TransferException(f"Transfer {self._uuid} is still executing, cannot get output")

        return self._celery_request.result


    def error(self):
        # return the error message from latest execution
        
        if not self._celery_request:
            raise TransferException(f"Transfer {self._uuid} does not have an active task")
        
        if not self._celery_request.failed():
            return None

        return self._celery_request.result


    def set_command(self, command:str) -> 'Transfer':
        self._command = command
        return self


    def set_schedule(self, schedule:str) -> 'Transfer':
        # set the cron schedule expression
        self._schedule_expr = schedule
        return self
    

    def set_mode(self, mode:Mode) -> 'Transfer':
        # set the replication mode
        return self.set_argument('--replication-mode', mode)


    def set_destination(self, destination_id:str) -> 'Transfer':
        # set the destination ID
        return self.set_argument('--destination-id', destination_id)


    def set_models(self, model_ids:List[str]) -> 'Transfer':
        # set the model IDs to sync from the source
        return self.set_argument('--model-ids', model_ids)

    
    def set_organization(self, organization_id:str) -> 'Transfer':
        # set the org id for this transfer
        return self.set_argument('--organization-id', organization_id)


    def set_argument(self, arg_name:str, arg_value) -> 'Transfer':
        # set a job argument
        self._arguments[arg_name] = arg_value
        return self

    
    def clone(self) -> 'Transfer':
        # create a new transfer based on this one to run manually
        if self.exists():
            self._sync_existing_args()

        t = Transfer.create(str(uuid.uuid4()))
        t.set_schedule(self._schedule_expr)
        t.set_command(self._command)
        for arg, val in self._arguments.items():
            t.set_argument(arg, val)

        return t

    
    def delete(self) -> bool:
        # delete this schedule
        entry = RedBeatSchedulerEntry.from_key(
            RedBeatSchedulerEntry(self._schedule_name, app=celery_app).key, 
            app=celery_app
        )
        entry.delete()
        return


    def apply(self) -> bool:
        # create/update schedule based on current settings
        if self.exists():
            return self._update()
        else:
            return self._create()
    

    def run(self, expedited=True) -> bool:
        # run this job now (not via schedule)
        self._celery_request = transfer_task.delay(self._build_command_args())
        