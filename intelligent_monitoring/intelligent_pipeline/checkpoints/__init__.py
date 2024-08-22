import os
import time

checkpoint_path = os.getenv("CHECKPOINT_PATH")


class MetricCheckpoint:
    def __init__(self, metric_name="", brick_name=""):
        self.metric_name = metric_name
        self.brick_name = brick_name

    def get(self, brick_name, metric, metrics_multivariate=[], tag={}):
        file_path = self._get_file_path(brick_name, metric, metrics_multivariate, tag=tag)
        self._ensure_directory_exists(file_path)
        try:
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    content = f.read().strip()
                    return int(content) if content else self.default_checkpoint()
            else:
                default_checkpoint = self.default_checkpoint()
                self._write_checkpoint(file_path, default_checkpoint)
                return default_checkpoint
        except Exception as e:
            print(f"Error reading or initializing checkpoint for {metric} in {brick_name}: {e}")
            return self.default_checkpoint()

    def set(self, new_checkpoint, brick_name, metric, metrics_multivariate=[], tag={}):
        file_path = self._get_file_path(brick_name, metric, metrics_multivariate, tag=tag)
        self._ensure_directory_exists(file_path)
        try:
            self._write_checkpoint(file_path, new_checkpoint)
            return f"Checkoint saved successfully at {file_path}"
        except Exception as e:
            return f"Error setting checkpoint for {metric} in {brick_name} inside {file_path}: {e}"

    def default_checkpoint(self):
        try:
            return int((time.time() - 3600 * 24) * 1000)
        except Exception as e:
            print(f"Error calculating default checkpoint: {e}")
            return 0  # Fallback to 0 if there's an error

    def _get_file_path(self, brick_name, metric, metrics_multivariate=[], tag={}):
        try:
            tags_name = list(tag.keys())
            metrics_str = "_".join(metric)
            if metrics_multivariate != []:
                metrics_str += "_" + "_".join(metrics_multivariate)
            for tag_name in tags_name:
                metrics_str += "_" + tag_name + "_" + str(tag[tag_name])
            # Construct the file name
            file_name = f"{metrics_str}.txt"
            # Construct the full file path
            return os.path.join(checkpoint_path, brick_name, file_name)
        except Exception as e:
            print(f"Error constructing file path for {metric} in {brick_name}: {e}")
            return ""

    def _write_checkpoint(self, file_path, checkpoint):
        try:
            with open(file_path, "w") as f:
                f.write(str(checkpoint))
        except Exception as e:
            print(f"Error writing checkpoint to {file_path}: {e}")

    def _ensure_directory_exists(self, file_path):
        try:
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
        except Exception as e:
            print(f"Error ensuring directory exists for {file_path}: {e}")
