# Insurance Member SCD2 History Tracking

![PySpark](https://img.shields.io/badge/PySpark-3.3%2B-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.0%2B-0099E5?logo=delta)
![Databricks](https://img.shields.io/badge/Databricks-10.4%2B-FF3621?logo=databricks)

**SCD2 implementation for tracking member address changes with full audit capabilities**

Features
Temporal Tracking - Complete history of member address changes
Auto-Expiration - Intelligent versioning with valid_from/valid_to timestamps
ACID Compliance - Delta Lake ensures reliable updates
Audit Ready - created_at/updated_at metadata columns
Production-Grade - Handles concurrent writes and schema evolution

🛠 Tech Stack 
PySpark 3.3+	Distributed data transformations
Delta Lake 2	ACID-compliant data lake
Databricks	Managed Spark environment
Python 3.8+	Pipeline control flow
Spark SQL	Table DDL and analytics

