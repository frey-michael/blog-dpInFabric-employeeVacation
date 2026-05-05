# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "482acfe0-006f-41cc-9a6c-9416ec3addf4",
# META       "default_lakehouse_name": "lh_internal",
# META       "default_lakehouse_workspace_id": "36e30446-adc8-4760-b21e-a24e4f664603",
# META       "known_lakehouses": [
# META         {
# META           "id": "482acfe0-006f-41cc-9a6c-9416ec3addf4"
# META         },
# META         {
# META           "id": "4c0bdfb1-83e0-4ba0-aabe-0d4972847cdc"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "854c19d7-5c23-b83e-4953-69a90df77607",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Read input data
employee_df = spark.read.format("delta").table("lh_internal.employees")
public_holiday_df = spark.read.format("delta").table("lh_internal.publicholidays")

display(employee_df)
display(public_holiday_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compute result
from pyspark.sql import functions as F

joined_df = employee_df.join(
    public_holiday_df, 
    employee_df["Country_Of_Residence"] == public_holiday_df["countryRegionCode"], 
    how="inner"
)

result_df = joined_df.groupBy(["First_Name", "Surname", F.year("date").alias("Year")]).agg(
    F.count("holidayName").alias("Vacation_Days"),
).orderBy(["Year", "First_Name", "Surname"])

display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write to outport
result_df.write.format("delta") \
  .option("header", "true") \
  .mode("overwrite") \
  .saveAsTable("lh_out_default.employee_vacation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
