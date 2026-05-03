# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "854c19d7-5c23-b83e-4953-69a90df77607",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Read input data

lh_internal_path = "abfss://36e30446-adc8-4760-b21e-a24e4f664603@onelake.dfs.fabric.microsoft.com/482acfe0-006f-41cc-9a6c-9416ec3addf4/Tables/"

employee_df = spark.read.format("delta").load(lh_internal_path + "employees")
public_holiday_df = spark.read.format("delta").load(lh_internal_path + "publicholidays")

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

lh_out_path = "abfss://36e30446-adc8-4760-b21e-a24e4f664603@onelake.dfs.fabric.microsoft.com/4c0bdfb1-83e0-4ba0-aabe-0d4972847cdc/Tables/employee_vacation"

result_df.write.format("delta") \
  .option("header", "true") \
  .mode("overwrite") \
  .save(lh_out_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
