from graphql_client import GraphQLClient
from jobs import Aggregation, NotebookRunReport, Schedule, GetJobList


class JobFactory:
    def __init__(self, client: GraphQLClient):
        self.client = client
        
    def create_launch_job(self, job_name):
        if job_name == "aggregation":
            print("Creating Aggregation job")
            return Aggregation(self.client)
        elif job_name == "notebook_run_report":
            print("Creating NotebookRunReport job")
            return NotebookRunReport(self.client)
        else:
            raise ValueError(f"Unknown job name: {job_name}")
    
    def create_cancel_schedule(self, job_name):
        if job_name == "notebook_run_report":
            print("Creating Schedule job for notebook_run_report")
            return Schedule(self.client)
        elif job_name == "aggregation":
            print("Creating Schedule job for aggregation")
            return Schedule(self.client)
        elif job_name == "daily_purge":
            print("Creating Schedule job for maintenance")
            return Schedule(self.client)
        else:
            raise ValueError(f"Unknown job name: {job_name}")
    
    def create_start_schedule(self, job_name):
        if job_name == "notebook_run_report":
            print("Creating Schedule job for notebook_run_report")
            return Schedule(self.client)
        elif job_name == "aggregation":
            print("Creating Schedule job for aggregation")
            return Schedule(self.client)
        else:
            raise ValueError(f"Unknown job name: {job_name}")
    
    def create_update_schedule(self, job_name):
        if job_name == "notebook_run_report":
            print("Creating Schedule job for notebook_run_report")
            return Schedule(self.client)
        elif job_name == "aggregation":
            print("Creating Schedule job for aggregation")
            return Schedule(self.client)
        else:
            raise ValueError(f"Unknown job name: {job_name}")
    
    def create_get_job_list(self):
        print("Creating GetJobList job")
        return GetJobList(self.client)
