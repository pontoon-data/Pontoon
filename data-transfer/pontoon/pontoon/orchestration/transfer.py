import os
import json
import uuid
import sys
import signal
import argparse
import traceback
import requests
from requests.exceptions import JSONDecodeError
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Any
from datetime import datetime, timezone, timedelta
from pathlib import Path
from pontoon import get_source, get_destination, \
                    get_source_by_vendor, get_destination_by_vendor, \
                    logger, configure_logging, \
                    Progress, Mode, SqliteCache, MemoryCache



class APIError(Exception):
    pass


class API:

    DEFAULT_API_ENDPOINT = os.environ.get('PONTOON_API_ENDPOINT', 'http://localhost:8000')

    def __init__(self, endpoint:str):
        self._prefix = "/internal"
        self._endpoint = endpoint or API.DEFAULT_API_ENDPOINT
        self._headers = {}

    def _check(self, res):
        if res.status_code != 200:
            raise APIError(f"API Error: {res.status_code} - {res.json()}")
        else:
            return res.json()
    
    def get(self, path:str):
        url = f"{self._endpoint}{self._prefix}{path}"
        try:
            return self._check(requests.get(url))
        except JSONDecodeError as e:
            logger.warning(f"Invalid JSON from {url} ({e})")
            return None

    def put(self, path:str, body):
        url = f"{self._endpoint}{self._prefix}{path}"
        body_json = json.dumps(body)
        try:
            return self._check(requests.put(url, data=body_json))
        except JSONDecodeError as e:
            logger.warning(f"Invalid JSON from {url} ({e})")
            return None

    def post(self, path:str, body):
        url = f"{self._endpoint}{self._prefix}{path}"
        body_json = json.dumps(body)
        try:
            return self._check(requests.post(url, data=body_json))
        except JSONDecodeError:
            logger.warning(f"Invalid JSON from {url} ({e})")
            return None

    def endpoint(self):
        return self._endpoint


class Command(ABC):
    """ Represents a command that we can run within the transfer job """

    def __init__(self, api:API, transfer_id:str, organization_id:str, execution_id:str, retry_count:int, retry_limit:int):
        self._api = api
        self._transfer_id = transfer_id
        self._organization_id = organization_id
        self._execution_id = execution_id
        self._retry_count = retry_count
        self._retry_max_attempts = retry_limit
        self._progress_update = None
        self._complete = False
        self._run_id = None

        if ':' in self._execution_id:
            self._execution_id = self._execution_id.split(':')[-1]


    def _meta(self): 
        return {
            'execution_id': self._execution_id,
            'retry_count': self._retry_count,
            'retry_max_attempts': self._retry_max_attempts
        }
    
    def _running(self, arguments:dict = {}):
        res = self._api.post('/runs', {
            'transfer_id': self._transfer_id,
            'status': 'RUNNING',
            'meta': self._meta() | {'arguments': arguments}
        })
        self._run_id = res['transfer_run_id']
    

    def _failure(self, cause:str=None, error_code:str=None):
        if self._complete:
            return

        output = {
            "cause": cause, 
            "error": error_code or "UNKNOWN_ERROR",
            "progress": self._progress_update
        }
        output_json = json.dumps(output)
        logger.error(output_json)
        if self._run_id:
            self._api.put(f"/runs/{self._run_id}", {
                'status': 'FAILURE',
                'output': output
            })
        self._complete = True
        return output_json
    
    def _success(self, output:dict=None):
        if self._complete:
            return

        output = output | {"progress": self._progress_update}
        output_json = json.dumps(output)
        logger.info(output_json)
        if self._run_id:
            self._api.put(f"/runs/{self._run_id}", {
                'status': 'SUCCESS',
                'output': output
            })
        self._complete = True
        return output_json

    def _progress(self, progress:Progress):
        if self._complete:
            return

        self._progress_update = progress.summary()
        output = {"progress": self._progress_update}
        output_json = json.dumps(output)
        if self._run_id:
            self._api.put(f"/runs/{self._run_id}", {
                'status': 'RUNNING',
                'output': output
            })
        return output_json
    
 
    @abstractmethod
    def run(self):
        pass



class TransferCommand(Command):
    """ Implements a data transfer for a given destination ID """


    def __init__(
        self, 
        api:API, 
        transfer_id:str,
        organization_id:str,
        execution_id:str, 
        retry_count:int, 
        retry_limit:int,
        destination_id:str, 
        replication_mode:Mode=None, 
        model_ids:List[str]=None,
        drop_after_complete=False):
        
        super().__init__(api, transfer_id, organization_id, execution_id, retry_count, retry_limit)

        self._now = datetime.now(timezone.utc)
        self._destination_id = destination_id
        self._replication_mode = replication_mode
        self._model_ids = model_ids
        self._drop_after_complete = drop_after_complete
        self._run_id = None

        # track whether we're running w/ overrides to ignore some checks + balances
        if replication_mode or model_ids:
            self._override = True
        else:
            self._override = False
    


    def _read_progress_handler(self, progress:Progress):
        self._progress(progress)


    def _write_progress_handler(self, progress:Progress):
        self._progress(progress)


    def _unlink_all(self, paths):
        for path in paths:
            if os.path.exists(path):
                os.remove(path)

    
    def _schedule_to_replication_mode(self, schedule):
        period = schedule.get('frequency')
        rtype = schedule.get('type', Mode.INCREMENTAL)
        
        if period not in [Mode.WEEKLY, Mode.DAILY, Mode.SIXHOURLY, Mode.HOURLY]:
            raise ValueError(f"Schedule provided invalid frequency: {period}")

        if rtype == Mode.FULL_REFRESH:
            # skip schedule drift validation if we're doing a full refresh anyway
            return Mode({'type': Mode.FULL_REFRESH})
        
        schedule_hour = schedule.get('hour', 0)
        schedule_min = schedule.get('minute', 0)

        end_time = self._now.replace(hour=schedule_hour, minute=schedule_min, microsecond=0)
        
        if period == Mode.WEEKLY:
            begin_time = end_time - timedelta(days=7, hours=12)
        elif period == Mode.DAILY:
            begin_time = end_time - timedelta(days=1, hours=3)
        elif period == Mode.SIXHOURLY:
            begin_time = end_time - timedelta(hours=6, minutes=30)
        elif period == Mode.HOURLY:
            begin_time = end_time - timedelta(hours=1, minutes=15)
        
        if period == Mode.WEEKLY:
            schedule_day = schedule.get('day')
            if schedule_day != self._now.day:
                logger.warning(f"@weekly day does not match current day: {schedule_day} != {self._now.day}")
            if abs(end_time - self._now) >= timedelta(hours=3):
                logger.warning(f"@weekly execution time is more than 3hrs off schedule")

        if period == Mode.DAILY:
            if abs(end_time - self._now) >= timedelta(hours=3):
                logger.warning(f"@daily execution time is more than 3hrs off schedule, end_time={end_time}, now={self._now}, delta={abs(end_time - self._now)}")

        if period == Mode.SIXHOURLY:
            if abs(end_time - self._now) >= timedelta(hours=1):
                logger.warning(f"@sixhourly execution time is more than 1hr off schedule")

        if period == Mode.HOURLY:
            if abs(end_time - self._now) >= timedelta(minutes=15):
                logger.warning(f"@hourly execution time is more than 15min off schedule")

        return Mode({
            'type': Mode.INCREMENTAL, 
            'period': period, 
            'start': begin_time, 
            'end': end_time
        })

    
    def _fetch_destination(self):
        self._destination = self._api.get(f"/destinations/{self._destination_id}")
        self._recipient = self._api.get(f"/recipients/{self._destination.get('recipient_id')}")
        if self._replication_mode is None:
            self._replication_mode = self._schedule_to_replication_mode(
                self._destination.get('schedule')
            )

    
    def _fetch_models(self):
        if self._model_ids is None:
            self._model_ids = self._destination.get('models')
        self._models = [
            self._api.get(f"/models/{model_id}") 
                for model_id in self._model_ids
        ]

    
    def _fetch_sources(self):
        source_ids = [model.get('source_id') for model in self._models]
        self._sources = {
            source_id: self._api.get(f"/sources/{source_id}") 
                for source_id in set(source_ids)
        }

    
    def _fetch_last_run(self):
        self._last_run = self._api.get(f"/runs/{self._transfer_id}")

    
    def _detect_run_gap(self) -> bool:

        # we're in manual mode, ignore checking for interval gap
        if self._override is True:
            return False
        
        # we're full refresh, interval covers all time so no gaps
        if self._replication_mode.period == Mode.FULL_REFRESH:
            return False
        
        # we don't have a last successful run to reference from, assume we're good
        if not self._last_run or self._last_run.get('created_at') is None:
            return False

        # check that our last successful run began within current interval:
        #   - we want some overlap to ensure we don't have a gap
        #   - we add buffer to the interval start to look back a little further than we need to
        #   - due to this, our last sync should always fall into the current interval
        last_run_dt = datetime.fromisoformat(self._last_run.get('created_at'))
        current_start_dt = self._replication_mode.start
        
        # our last successful execution _should_ be within our current interval
        return last_run_dt < current_start_dt


    def _fetch_configuration(self):
        self._fetch_destination()
        self._fetch_models()
        self._fetch_sources()
        self._fetch_last_run()
        
    
    def run(self):
        logger.info('Hello, TransferCommand!')
        logger.info(f"Using Pontoon API at {self._api.endpoint()}")
        logger.info(f"transfer_id: {self._transfer_id}")
        logger.info(f"destination_id: {self._destination_id}")

        if self._replication_mode:
            logger.info(f"Override replication mode: {self._replication_mode}")
        
        if self._model_ids:
            logger.info(f"Override model IDs: {self._model_ids}")
    
        if self._drop_after_complete == True:
            logger.info(f"Dropping resources after completion: drop_after_complete={self._drop_after_complete}")

        # add integrity check fields to models
        with_config = {
             'batch_id': True,
             'last_sync': True
        }
        
        try:
            # pull in configuration from the API
            self._fetch_configuration()      

            # create the transfer run record with metadata
            self._running({
                'type': 'transfer',
                'mode': json.loads(str(self._replication_mode)),
                'models': self._model_ids,
                'drop_after_complete': self._drop_after_complete
            })

            if self._detect_run_gap() is True:
                return self._failure(f"Last successful run was outside our current interval.")
                                 
        except Exception as e:
            return self._failure(f"Starting transfer job failed: {e}")

        # configure our data source(s) based on models
        sources = []
        source_caches = []

        try:
            for source_id, source in self._sources.items():
            
                cache_db = f"cache-{uuid.uuid4().hex}.db"

                models = [model for model in self._models if model['source_id'] == source_id]
                
                streams = [{
                    'schema': model['schema_name'],
                    'table': model['table_name'],
                    'primary_field': model['primary_key_column'],
                    'cursor_field': model['last_modified_at_column'],
                    'filters': {model['tenant_id_column']: self._recipient['tenant_id']},
                    'drop_fields': [model['tenant_id_column']]
                } for model in models]

                connector = get_source(
                    get_source_by_vendor(source['vendor_type']),
                    config = {
                        'mode': self._replication_mode,
                        'with': with_config,
                        'streams': streams,
                        'connect': source['connection_info']
                    },
                    cache_implementation=SqliteCache,
                    cache_config = {
                        'chunk_size': 1024,
                        'db': cache_db
                    }
                )
                sources.append(connector)
                source_caches.append(cache_db)

        except Exception as e:
            return self._failure(f"Configuring job source connector(s) failed: {e}")

        # configure our destination
        try:
            destination = get_destination(
                get_destination_by_vendor(self._destination.get('vendor_type')),
                config = {
                    'mode': self._replication_mode,
                    'drop_after_complete': self._drop_after_complete,
                    'connect': self._destination['connection_info']
                }
            )
        except Exception as e:
            return self._failure(f"Configuring job destination connector failed: {e}")

        
        # move data
        logger.info(f"Starting to move data")
        for source in sources:
            
            try:
                # read records into cache
                ds = source.read(progress_callback=self._read_progress_handler)
            except Exception as e:
                self._unlink_all(source_caches)
                return self._failure(f"Reading source failed: {e}")
            
            try:
                # write records to destination
                destination.write(ds, progress_callback=self._write_progress_handler)
            
                # clean up source caches
                self._unlink_all(source_caches)

                # integrity checks
                if self._drop_after_complete == False:
                    destination.integrity().check_batch_volume(ds)

            except Exception as e:
                self._unlink_all(source_caches)
                tb = "\n".join(traceback.format_tb(e.__traceback__))
                return self._failure(f"Transfer to destination failed: {e}, Traceback: {tb}")
        

        # complete our execution
        return self._success({"success": True, "message": f"Job complete."})



class SourceCheckCommand(Command):
    """ Implements a source connection check for a given source ID """

    def __init__(self, api:API, transfer_id:str, organization_id:str, source_id:str, execution_id:str, retry_count:int, retry_limit:int):
        super().__init__(api, transfer_id, organization_id, execution_id, retry_count, retry_limit)
        self._source_id = source_id

    
    def run(self):
        logger.info('Hello, SourceCheckCommand!')
        logger.info(f"Using Pontoon API at {self._api.endpoint()}")
        logger.info(f"transfer_id: {self._transfer_id}")
        logger.info(f"source_id: {self._source_id}")
    
        # put the job into RUNNING state
        try:
            self._running()
        except Exception as e:
            return self._failure(f"Creating execution record for job failed: {e}")

        
        try:
            source = self._api.get(f"/sources/{self._source_id}")

            connector = get_source(
                get_source_by_vendor(source['vendor_type']),
                config = {
                    'connect': source['connection_info']
                }
            )

            connector.test_connect()
            
            # everything worked
            return self._success({"success": True, "message": "Connection successful"})

        except Exception as e:
            return self._success({"success": False, "message": str(e)})




class SourceInspectCommand(Command):
    """ Gets schema info and returns it for a given source ID """

    def __init__(self, api:API, transfer_id:str, organization_id:str, source_id:str, execution_id:str, retry_count:int, retry_limit:int):
        super().__init__(api, transfer_id, organization_id, execution_id, retry_count, retry_limit)
        self._source_id = source_id

    
    def run(self):
        logger.info('Hello, SourceInspectCommand!')
        logger.info(f"Using Pontoon API at {self._api.endpoint()}")
        logger.info(f"transfer_id: {self._transfer_id}")
        logger.info(f"source_id: {self._source_id}")
    
        # put the job into RUNNING state
        try:
            self._running()
        except Exception as e:
            return self._failure(f"Creating execution record for job failed: {e}")

        try:
            source = self._api.get(f"/sources/{self._source_id}")

            connector = get_source(
                get_source_by_vendor(source['vendor_type']),
                config = {
                    'connect': source['connection_info']
                }
            )

            stream_info = {
                'source_id': self._source_id,
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'streams': connector.inspect_streams()
            }
            
            # everything worked
            return self._success({"success": True, "stream_info": stream_info})
        
        except Exception as e:
            return self._success({"success": False, "message": str(e)})




def handle_signal(signum, frame):
    logger.info("Signal {signum} received, shutting down...")
    system.exit(1)


def main(argv: List[str] = None):
    parser = argparse.ArgumentParser(description="Pontoon Transfer Job 0.1.0")
    parser.add_argument("--command", type=str, choices=['transfer','source-check','source-inspect'], default='transfer', help="The transfer command to run")
    parser.add_argument("--transfer-id", type=str, help="The ID of the corresponding transfer")
    parser.add_argument("--organization-id", type=str, help="The organization ID of this transfer")
    parser.add_argument("--destination-id", type=str, default=None, help="The ID of the destination")
    parser.add_argument("--replication-mode", type=str, default=None, help="Override the replication mode for the transfer")
    parser.add_argument("--model-ids", type=str, default=None, help="Override model IDs to transfer")
    parser.add_argument("--source-id", type=str, default=None, help="Source ID to inspect")
    parser.add_argument("--api-endpoint", type=str, default=None, help="The Pontoon API endpoint to use")
    parser.add_argument("--execution-id", type=str, default=None, help="The task execution ID for this job")
    parser.add_argument("--retry-count", type=int, default=0, help="Number of retries for this job run")
    parser.add_argument("--retry-limit", type=int, default=3, help="Max number of retries for this job run")
    parser.add_argument("--dry-run", type=bool, default=False, help="Do not create or alter any data or resources")
    parser.add_argument("--drop-after-complete", type=bool, default=False, help="Drop any data or resources created after completion")


    # Configure pontoon logging
    configure_logging(os.environ.get('ENV', 'dev'))

    # Parse arguments
    args = parser.parse_args(argv)

    # Run job
    try:
        if args.command == 'transfer':
            cmd = TransferCommand(
                api=API(args.api_endpoint),
                transfer_id=args.transfer_id,
                organization_id=args.organization_id,
                destination_id=args.destination_id,
                replication_mode=Mode(json.loads(args.replication_mode)) if args.replication_mode else None,
                model_ids=json.loads(args.model_ids) if args.model_ids else None,
                execution_id=args.execution_id,
                retry_count=args.retry_count,
                retry_limit=args.retry_limit,
                drop_after_complete=args.drop_after_complete
            )
        elif args.command == 'source-check':
            cmd = SourceCheckCommand(
                api=API(args.api_endpoint),
                transfer_id=args.transfer_id,
                organization_id=args.organization_id,
                source_id=args.source_id,
                execution_id=args.execution_id,
                retry_count=args.retry_count,
                retry_limit=args.retry_limit
            )
        elif args.command == 'source-inspect':
            cmd = SourceInspectCommand(
                api=API(args.api_endpoint),
                transfer_id=args.transfer_id,
                organization_id=args.organization_id,
                source_id=args.source_id,
                execution_id=args.execution_id,
                retry_count=args.retry_count,
                retry_limit=args.retry_limit
            )
        else:
            logger.error(f"Unknown transfer command: {args.command}")
            sys.exit(1)

        return cmd.run()
    except Exception as e:
        logger.error("Caught unhandled exception from job: ", e)
        return json.dumps({"cause": str(e), "error": 'UNKNOWN_ERROR'})


if __name__ == '__main__':
    # Running as a CLI
    try:
        signal.signal(signal.SIGTERM, handle_signal)  # Handle termination signal
        signal.signal(signal.SIGHUP, handle_signal)   # Handle hangup signal

        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        logger.info("Caught KeyboardInterrupt, shutting down...")
        sys.exit(1)
    