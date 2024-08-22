import os
import sys
from graphql_client import GraphQLClient
from job_factory import JobFactory


if __name__ == "__main__":
    graphql_url = os.getenv("GRAPHQL_URL")
    client = GraphQLClient(graphql_url)
    factory = JobFactory(client)

    # Command-line argument processing
    if len(sys.argv) < 1:
        print("Usage: python file.py <job_function> [job_name] [cron_expression] [metrics] [tags] ")
        sys.exit(1)

    command = sys.argv[1]
    job_name = sys.argv[2] if len(sys.argv) > 2 else ""
    cron_expression = sys.argv[3] if len(sys.argv) > 3 else "* * * * *"
    metrics = sys.argv[4].split(',') if len(sys.argv) > 4 else ["SCH.M1"]
    tags = sys.argv[5].split(',') if len(sys.argv) > 5 else [{"satellite":"SAT2"}]

    # Create job object based on the command
    job = None
    if command == "launch_job":
        job = factory.create_launch_job(job_name)
        job.launch(metrics, tags)
    elif command == "cancel_schedule":
        job = factory.create_cancel_schedule(job_name)
        job.cancel_schedule(job_name)
    elif command == "start_schedule":
        job = factory.create_start_schedule(job_name)
        job.start_schedule(job_name, cron_expression)
    elif command == "get_job_list":
        job = factory.create_get_job_list()
        job.launch()
    elif command == "update_schedule":
        job = factory.create_update_schedule(job_name, cron_expression)
        job.update_schedule(job_name, cron_expression)
    else:
        print(f"Unknown command: {command}")
        print("Available commands: launch_job, cancel_schedule, start_schedule, get_job_list")
        sys.exit(1)
    
