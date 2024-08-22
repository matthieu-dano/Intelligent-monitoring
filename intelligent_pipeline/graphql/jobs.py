import time

from graphql_client import GraphQLClient
from queries import LAUNCH_JOB, LIST_JOB, UPDATE_SCHEDULE, CANCEL_SCHEDULE, START_SCHEDULE

REPOSITORY_LOCATION_NAME = "intelligent_pipeline"
REPOSITORY_NAME = "__repository__"


class Aggregation:
    def __init__(self, client: GraphQLClient):
        self.client = client

    def launch(self, metrics, tags=[]):
        print("Launching Aggregation job")
        date = time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                print(f"Launching Aggregation job for metric: {metric} and tag: {tag}")
                run_config = {
                    "ops": {
                        f"build_query_aggregation": {
                            "config": {"date": date, "metric": [metric], "tag": tag}
                        },
                        f"get_data_aggregation": {"config": {"date": date}},
                        f"process_data_aggregation": {
                            "config": {"date": date, "metric": [metric]}
                        },
                        f"push_data_aggregation": {
                            "config": {"date": date, "metric": [metric], "tag": tag}
                        },
                    }
                }
                run_configs.append(run_config)
        
        for run_config in run_configs:
            variables = {
                "repositoryLocationName": REPOSITORY_LOCATION_NAME,
                "repositoryName": REPOSITORY_NAME,
                "jobName": "aggregation",
                "runConfigData": run_config,
            }

            response = self.client.send_query(LAUNCH_JOB, variables)
            print(response)

class NotebookRunReport:
    def __init__(self, client: GraphQLClient):
        self.client = client

    def launch(self, metrics, tags=[]):
        print("Launching NotebookRunReport job")
        date = time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                print(f"Launching NotebookRunReport job for metric: {metric} and tag: {tag}")
                run_config = {
                    "ops": {
                    "notebook_run_report_op": {"config": {"date": date, "metric": [metric], "notebook_name": "report", "tag": tag}},
                }
                }
                run_configs.append(run_config)

        for run_config in run_configs:
            variables = {
                "repositoryLocationName": REPOSITORY_LOCATION_NAME,
                "repositoryName": REPOSITORY_NAME,
                "jobName": "notebook_run_report",
                "runConfigData": run_config,
            }

            response = self.client.send_query(LAUNCH_JOB, variables)
            print(response)

class Schedule:
    def __init__(self, client: GraphQLClient):
        self.client = client

    def cancel_schedule(self, schedule_name):
        print("Cancelling Schedule job")
        variables = {
            "repositoryLocationName": REPOSITORY_LOCATION_NAME,
            "repositoryName": REPOSITORY_NAME,
            "scheduleName": schedule_name
        }
        response = self.client.send_query(CANCEL_SCHEDULE, variables)
        print(response)
        pass
    
    def start_schedule(self, schedule_name):
        print("Starting Schedule job")
        variables = {
            "repositoryLocationName": REPOSITORY_LOCATION_NAME,
            "repositoryName": REPOSITORY_NAME,
            "scheduleName": schedule_name
        }
        response = self.client.send_query(START_SCHEDULE, variables)
        print(response)
        pass
    
    def update_schedule(self, schedule_name, cron_expression):
        print("Updating Schedule job")
        variables = {
            "repositoryLocationName": REPOSITORY_LOCATION_NAME,
            "repositoryName": REPOSITORY_NAME,
            "scheduleName": schedule_name,
            "cronSchedule": cron_expression
        }
        response = self.client.send_query(UPDATE_SCHEDULE, variables)
        print(response)
        pass

class GetJobList:
    def __init__(self, client: GraphQLClient):
        self.client = client

    def launch(self):
        print("Getting job list")
        variables = {
            "repositoryLocationName": REPOSITORY_LOCATION_NAME,
            "repositoryName": REPOSITORY_NAME,
        }
        response = self.client.send_query(LIST_JOB, variables)
        print(response)
