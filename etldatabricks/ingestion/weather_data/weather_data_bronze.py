"""
Ingest raw data contains weather conditions.
"""

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

