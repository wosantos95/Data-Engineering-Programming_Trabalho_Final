from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self, app_name: str, master: str):
        self.app_name = app_name
        self.master = master
        self._spark = None

    def get_spark(self):
        if self._spark is None:
            self._spark = (
                SparkSession.builder
                .appName(self.app_name)
                .master(self.master)
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
        return self._spark

    def stop(self):
        if self._spark:
            self._spark.stop()
            self._spark = None