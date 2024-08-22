import time

from dagster import RunRequest

class GenerateRunConfig:
    @staticmethod
    def basic_run_configs(context, job_type, metrics, tags):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                run_key = f"{metric[0]}_{tag}_{job_type}_{int(time.time())}"
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
        context.log.info(len(run_configs))
        return run_configs

    @staticmethod
    def training_run_configs(context, job_type, metrics, tags):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []

        for metric in metrics:
            for tag in tags:
                run_key = f"{metric[0]}_{tag}_{job_type}_{int(time.time())}"
                run_config = {
                    "ops": {
                        f"check_model_{job_type}": {"config": {"date": date, "metric": [metric[0]],
                                                               "metrics_multivariate": metric[1:],"tag" : tag}},
                        f'build_query_first_date_{job_type}': {'config': {'date':date, 'metric': [metric[0]],
                                                                          "metrics_multivariate": metric[1:], "tag" : tag}},
                        f'get_data_first_date_{job_type}': {"config": {"date": date, "metric": [metric[0]],
                                                                       "metrics_multivariate": metric[1:]}},
                        f"build_query_{job_type}": {"config": {"date": date, "metric": [metric[0]],
                                                               "metrics_multivariate": metric[1:], "tag" : tag}},
                        f"get_data_{job_type}": {"config": {"date": date, "metric": [metric[0]],
                                                            "metrics_multivariate": metric[1:]}},
                        f"model_{job_type}": {"config": {"date": date, "metric": [metric[0]],
                                                         "metrics_multivariate": metric[1:], "tag" : tag}},
                        f"save_model_{job_type}": {"config": {"date": date, "metric": [metric[0]],
                                                              "metrics_multivariate": metric[1:], "tag" : tag}},
                    }
                }
                run_configs.append(
                    RunRequest(
                        run_key=run_key, run_config=run_config, tags={"training": "my_unique_training"}
                    )
                )
        return run_configs

    @staticmethod
    def predict_run_configs(context, job_type, metrics, tags):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []

        for metric in metrics:
            for tag in tags:
                run_key = f"{metric[0]}_{tag}_{job_type}_{int(time.time())}"
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
                        run_key=run_key, run_config=run_config, tags={"predict": "my_unique_predict"}
                    )
                )
        return run_configs

    # Only work with one metric inside the list of list.
    @staticmethod
    def notebook_run_configs(context, notebook, metrics, tags):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                run_key = f"{metric}_{tag}_{notebook}_{int(time.time())}"
                run_config = {
                    "ops": {
                        f"notebook_run_{notebook}_op": {
                            "config": {
                                "date": date,
                                "metric": [metric[0]],
                                "notebook_name": notebook,
                                "tag": tag,
                            }
                        }
                    }
                }
                run_configs.append(
                    RunRequest(
                        run_key=run_key, run_config=run_config, tags={"notebook": "my_unique_notebook"}
                    )
                )
        return run_configs

    @staticmethod
    def lower_part_run_config(context, job_type, metrics, tags):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                run_key = f"{metric[0]}_{tag}_{job_type}_{int(time.time())}"
                metric = [metric[0]]
                run_config = {
                    "ops": {
                        f"build_query_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"get_data_{job_type}": {"config": {"date": date}},
                        f"process_data_{job_type}_aggregation": {
                            "config": {"date": date, "metric": metric}
                        },
                        f"process_data_{job_type}_isolation_forest": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"process_data_{job_type}_rolling_average" : {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"push_data_{job_type}_isolation_forest": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"push_data_{job_type}_rolling_average" : {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        }
                    }
                }
                run_configs.append(
                    RunRequest(
                        run_key=run_key, run_config=run_config, tags={"lower_part": "my_unique_lower_part"}
                    )
                )
        context.log.info(len(run_configs))
        return run_configs

    @staticmethod
    def resample_anomaly_run_config(context, job_type, metrics, tags, extension):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                run_key = f"{metric[0]}_{tag}_{job_type}_{int(time.time())}"
                run_config = {
                    "ops": {
                        f"build_query_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"get_data_{job_type}": {"config": {"date": date}},
                        f"build_query_lb_ub_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"get_data_lb_ub_{job_type}": {"config": {"date": date}},
                        f"process_data_{job_type}": {"config": {"date": date, "metric": metric}},
                        f"push_data_anomaly_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag}
                        },
                        f"push_data_resample_{job_type}": {
                            "config": {"date": date, "metric": metric, "tag": tag, "extension": extension}
                        },
                    }
                }
                run_configs.append(
                    RunRequest(
                        run_key=run_key, run_config=run_config, tags={"resample_anomaly": "my_unique_resample_anomaly"}
                    )
                )
        context.log.info(len(run_configs))
        return run_configs

    @staticmethod
    def linear_prediction_run_config(context, job_type, metrics, tags):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_configs = []
        for metric in metrics:
            for tag in tags:
                run_key = f"{metric[0]}_{tag}_{job_type}_{int(time.time())}"
                metric = [metric[0]]
                run_config = {
                    "ops": {
                        f"build_query_{job_type}": {"config": {"date": date, "metric":metric, "tag": tag, "metrics_multivariate": []}},
                        f"get_data_{job_type}": {"config": {"date": date, "metric": metric, "metrics_multivariate": []}},
                        f"prediction_{job_type}": {"config": {"date": date, "metric":metric}},
                        f"push_data_{job_type}": {"config": {"date": date, "metric": metric, "tag": tag}},
                    }
                }
                run_configs.append(RunRequest(run_key=run_key, run_config=run_config,
                                              tags={'linear_prediction' : 'my_unique_linear_prediction'}))
        context.log.info(len(run_configs))
        return run_configs
    
    @staticmethod
    def maintenance_run_config(context, job_type, days_to_keep):
        date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        run_key = f"{job_type}_{int(time.time())}"
        run_config = {
            "ops": {
                f"purge_old_runs_{job_type}": {"config": {"date": date, "days_to_keep": days_to_keep}}
            }
        }
        return RunRequest(run_key=run_key, run_config=run_config, tags={'maintenance_job':'my_unique_maintenance_job'})

