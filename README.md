# Insurance Member SCD2 History Tracking


**SCD2 implementation for tracking member address changes with full audit capabilities**

Project Summary

The project implements Slowly Changing Dimension Type 2 (SCD2) using PySpark and Delta Lake on Databricks.
It tracks historical changes (like address updates) for insurance members while preserving full change history.

✅ Key Features:

Maintain full history of changes.

Automatically expire old records when changes are detected.

Insert new "current" records when changes occur.

Built with Delta Lake for ACID guarantees.


🛠 Tech Stack 
PySpark 3.3+	Distributed data transformations

Delta Lake 2	ACID-compliant data lake (Versioned, reliable data storage)

Databricks	Managed Spark environment (Analytics platform)

Python 3.8+	Pipeline control flow (Script orchestration)

Spark SQL	Table DDL and analytics (Table creation and management)




