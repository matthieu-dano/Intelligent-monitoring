from dagster import op, Field, Int, String, RunsFilter
from datetime import datetime, timedelta
from .logger import logging_decorator

class MaintenanceOps:
    @staticmethod
    def create_purge_old_runs_op(brick_name: str, op_name: str):
        @op(
            name=op_name,
            description="Purges old run records and event logs older than the number of days given in the config.",
            tags={"operation": f"{brick_name}"},
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "days_to_keep": Field(Int, default_value=7, description="Number of days to keep records"),
            },
        )
        @logging_decorator
        def purge_old_runs(context):
            instance = context.instance
            days_to_keep = context.op_config["days_to_keep"]
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Create a RunsFilter object
            runs_filter = RunsFilter(created_before=cutoff_date)
            
            # Purge old run records
            old_runs = instance.get_run_records(filters=runs_filter)
            for run_record in old_runs:
                instance.delete_run(run_record.pipeline_run.run_id)
            
            # Purge old event logs
            for run_record in old_runs:
                old_event_logs = instance.event_log_storage.get_logs_for_run(run_record.pipeline_run.run_id)
                for event_log in old_event_logs:
                    if event_log.timestamp < cutoff_date.timestamp():
                        instance.event_log_storage.delete_events(run_record.pipeline_run.run_id)
            
            context.log.info(f"Purged runs and event logs older than {days_to_keep} days.")
        
        return purge_old_runs