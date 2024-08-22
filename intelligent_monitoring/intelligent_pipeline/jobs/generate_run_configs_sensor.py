from dagster import DagsterInstance, DagsterRunStatus, RunRequest, RunsFilter, SkipReason
from datetime import datetime

class GenerateRunConfigSensor:
    
    @staticmethod
    def get_last_run(runs, job_name, context):
        if not runs:
            context.log.info(f"No runs found for the {job_name} job.")
            return SkipReason(f"No runs found for the {job_name} job.")

        # Ensure there are at least two runs to access `previous_run`
        if len(runs) > 1:
            last_run = runs[0]
            previous_run = runs[1]
        else:
            # Only one run available
            last_run = runs[0]
            previous_run = None
        
        # Determine which run to use
        if last_run.status in [DagsterRunStatus.SUCCESS]:
            context.log.info(f"Found a completed run: {last_run.run_id} with status: {last_run.status}")
            return last_run
        elif (
            last_run.status in [DagsterRunStatus.STARTED]
            and previous_run
            and previous_run.status in [DagsterRunStatus.SUCCESS]
        ):
            context.log.info(f"Found a completed previous run: {previous_run.run_id} with status: {previous_run.status}")
            return previous_run
        else:
            context.log.info(f"Last run status is not 'SUCCESS' : {last_run.status}")
            return SkipReason(f"No completed {job_name} job run found.")

    
    @staticmethod
    def basic_requests(context, job_name, job_type, metrics, tags):
        runs_filter = RunsFilter(job_name=job_name)
        runs = context.instance.get_runs(limit=2, filters=runs_filter)
        last_run = GenerateRunConfigSensor.get_last_run(runs, job_name, context)
        
        if isinstance(last_run, SkipReason):
            context.log.info(f"Skipping requests due to: {last_run}")
            return []
        
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                run_key = f"{metric}_{tag}_{job_type}_{last_run.run_id}"
                run_config = {
                    "ops": {
                        f"build_query_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"get_data_{job_type}": {"config": {"date": date}},
                        f"process_data_{job_type}": {
                            "config": {"date": date, "metric": metric[0]}
                        },
                        f"push_data_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                    }
                }
                run_configs.append(
                    RunRequest(
                        run_key=run_key, run_config=run_config, tags={"job": "my_unique_job"}
                    )
                )
        return run_configs

    @staticmethod
    def ml_predict_requests(context, job_name, job_type, metrics, tags):
        runs_filter = RunsFilter(job_name=job_name)
        runs = context.instance.get_runs(limit=2, filters=runs_filter)
        last_run = GenerateRunConfigSensor.get_last_run(runs, job_name, context)
        
        if isinstance(last_run, SkipReason):
            context.log.info(f"Skipping requests due to: {last_run}")
            return []
        
        current_datetime = datetime.now()
        date = current_datetime.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                run_key = f"{metric[0]}_{tag}_{job_type}_{last_run.run_id}"
                run_config = {
                    "ops": {
                        f"get_model_{job_type}": {
                            "config": {
                                "date": date,
                                "metric": metric[0],
                                "metrics_multivariate": metric[1:],
                                "tag": tag,
                            }
                        },
                        f"build_query_{job_type}": {
                            "config": {
                                "date": date,
                                "metric": metric[0],
                                "metrics_multivariate": metric[1:],
                                "tag": tag,
                            }
                        },
                        f"get_data_{job_type}": {"config": {"date": date}},
                        f"build_query_multi_{job_type}": {
                            "config": {
                                "date": date,
                                "metric": metric[0],
                                "metrics_multivariate": metric[1:],
                                "tag": tag,
                            }
                        },
                        f"get_data_multi_{job_type}": {"config": {"date": date}},
                        f"using_model_{job_type}": {
                            "config": {"date": date, "metric": metric[0], "tag": tag}
                        },
                        f"pushing_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                    }
                }
                run_configs.append(
                    RunRequest(
                        run_key=run_key, run_config=run_config, tags={"job": "my_unique_job"}
                    )
                )
        return run_configs

    @staticmethod
    def export_notebook_requests(context, job_name, job_type, metrics, tags):
        runs_filter = RunsFilter(job_name=job_name)
        runs = context.instance.get_runs(limit=2, filters=runs_filter)
        
        last_run = GenerateRunConfigSensor.get_last_run(runs, job_name, context)
        
        if isinstance(last_run, SkipReason):
            context.log.info(f"Skipping requests due to: {last_run}")
            return []
        
        instance = DagsterInstance.get()
        logs = instance.all_logs(last_run.run_id)
        current_datetime = datetime.now()
        date = current_datetime.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                metrics_str = "_".join(metric)  # Join the list into a single string
                tags_str = "_".join(tag)  # Join the list into a single string
                run_key = f"{metrics_str}_{tags_str}_{job_type}_{last_run.run_id}"
                run_config = {
                    "ops": {
                        f"notebook_export_{job_type}_op": {
                            "config": {"notebook_name": job_type, "logs": logs, "date":date}
                        }
                    }
                }
                run_configs.append(
                    RunRequest(run_key=run_key, run_config=run_config, tags={"job": "my_unique_job"})
                )
        return run_configs
