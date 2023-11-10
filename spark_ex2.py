from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
reload(sys)

sys.setdefaultencoding('utf-8')
# Create a SparkSession

spark = SparkSession.builder \
    .appName("EmployeeData") \
    .master("local[*]") \
    .getOrCreate()

# Create a SparkContext
sc = spark.sparkContext

file_path = "/tmp/bduk1710/yas/employee_data.csv"
employee_df = spark.read.csv(file_path, header=True, inferSchema=True)

avg_age = employee_df \
    .groupBy("department") \
    .agg(F.avg("age") \
    .alias("average_age"))

total_salary = employee_df \
    .groupBy("department") \
    .agg(F.sum("salary") \
    .alias("total_salary"))

salary_threshold = 50000
filtered_emp = employee_df.filter(employee_df["salary"] <= salary_threshold)

avg_sal_filtered = filtered_emp.agg(F.avg("salary").alias("average_salary"))

emp_count = filtered_emp.groupBy("department").count()

avg_age.show()
total_salary.show()
avg_sal_filtered.show()