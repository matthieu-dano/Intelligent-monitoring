import os

from joblib import load

model_path = os.getenv("MODEL_PATH")

class ModelCheck:
    def __init__(self, metric_name=""):
        self.metric_name = metric_name

    def get_model(self, file_path, metric, metrics_multivariate, tag={}):
        try:
            if file_path and os.path.exists(file_path):
                return load(file_path)
            else:
                print(
                    f"Model file for {metric} and {metrics_multivariate} and tag = {tag} does not exist."
                )
                return None
        except Exception as e:
            print(f"Error loading model for {metric} and {metrics_multivariate}: {e}")
            return None

    def save_model(self, model, date, metric, metrics_multivariate=[], tag={}):
        try:
            metrics_str = metric[0]
            tags_name = list(tag.keys())
            if metrics_multivariate:
                metrics_str += "_" + "_".join(metrics_multivariate)
            for tag_name in tags_name:
                metrics_str += "_" + tag_name + "_" + tag[tag_name]
            name = metrics_str + "-" + date
            model.save(file_path=model_path, name=name)
            return model_path, name
        except Exception as e:
            error_message = f"Error saving model {metric} and {metrics_multivariate}: {str(e)}"
            return "Error", error_message

    def _create_file_path_model(self, metrics, date, tag={}):
        metrics_str = "_".join(metrics)
        tags_name = list(tag.keys())
        for tag_name in tags_name:
            metrics_str += "_" + tag_name + "_" + tag[tag_name]
        return os.path.join(model_path, f"{metrics_str}-{date}.joblib")

    def update_model(self, model, metrics, date, tag={}):
        old_model_file_path = self.get_model_file_path(metrics, tag=tag)
        # Remove the old one and save the new one
        if os.path.exists(old_model_file_path):
            self.save_model(model, metrics, date, tag=tag)
            os.remove(old_model_file_path)
        else:
            print(f"Model file for {metrics} does not exist.")

    def get_model_file_path(self, metric, metrics_multivariate=[], tag={}):
        try:
            files = os.listdir(model_path)
            metrics_str = metric[0]
            tags_name = list(tag.keys())
            if metrics_multivariate != []:
                metrics_str += "_" + "_".join(metrics_multivariate)
            for tag_name in tags_name:
                metrics_str += "_" + tag_name + "_" + tag[tag_name]
            metrics_str += "-"
            for file in files:
                if file.startswith(metrics_str) and file.endswith(".joblib"):
                    return os.path.join(model_path, file)
            return ""
        except Exception as e:
            print(
                f"Error constructing model file path for {metric} with multivariate metrics {metrics_multivariate}: {e}"
            )
            return ""
