
import os
import sys


# Force Spark workers to use the same Python
# interpreter as the active virtual environment.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Windows local Spark networking.
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
