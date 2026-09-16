from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import (
    col,
    sum,
    trim,
    to_date,
    struct,
    collect_list,
    size,
    explode
)

spark = SparkSession.builder \
                    .appName("customer_order_transformation") \
                    .getOrCreate()
                    
# defifne customer schema
customer_schema = StructType([
    StructField("customer_id",StringType(),True),
    StructField("first_name",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("email",StringType(),True),
    StructField("city",StringType(),True),
    StructField("state",StringType(),True),
    StructField("signup_date",StringType(),True),
    StructField("customer_segment",StringType(),True)    
])
                    
# define order schema

order_schema = StructType([
    StructField("order_id",StringType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",StringType(),True),
    StructField("order_status",StringType(),True),
    StructField("order_channel",StringType(),True),
    StructField("payment_method",StringType(),True) ,
    StructField("shipping_city",StringType(),True)   
])

# Creating dataframe

customer_df = spark.read \
                   .option("header",True) \
                   .schema(customer_schema) \
                   .csv("data/customers.csv")
            
            
order_df = spark.read \
                .option("header",True) \
                .schema(order_schema) \
                .csv("data/orders.csv")


# Data Validation

print("------------------- Data Validation --------------------------------")

print("\nCustomer_count : ", customer_df.count())
print("Order_count : ", order_df.count())    

# Check for null values
def check_nulls(df,df_name):
    print(f"\n Null check for {df_name}")
    
    null_count = df.select([
        sum(col(column).isNull().cast("int")).alias(column)
        for column in df.columns
    ])
    null_count.show()
  
 
check_nulls(customer_df,"Customer")
check_nulls(order_df,"Order")

# Checking for duplicates of customer_id in customer_df
print("\nDuplicate Customer Ids: ")

customer_df.groupBy("customer_id") \
           .count() \
           .filter(col("count")>1) \
            .show()

# Checking for duplicates of order_id in order_df
print("\nDuplicate Order Ids: ")

order_df.groupBy("order_id") \
           .count() \
           .filter(col("count")>1) \
            .show()
     
# Checing for orphan orders --> orders who customer_id doesn't exist in the cutomer table.

orphan_orders = order_df.join(
    customer_df.select("customer_id"),
    on = "customer_id",
    how = "left_anti"
)       

orphan_orders.show()

print("Orphan order count : ", orphan_orders.count())

def trim_string_columns(df):
    for field in df.schema.fields:
        if field.dataType == StringType():
            df = df.withColumn(field.name, trim(col(field.name)))
    return df

customer_df = trim_string_columns(customer_df)
order_df = trim_string_columns(order_df)

print("------------------------ Converting to date ---------------------")

# Converting the data_type of signup_date in customer_df from string to date data type

customer_df = customer_df.withColumn(
    "signup_date",
    to_date(col("signup_date"), "yyyy-MM-dd")
)
# Converting the data_type of order_date in order_df from string to date data type

order_df = order_df.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

print("Conversion done")

print("------------------------ Join Customer + Order data ---------------------")

# Join Customer + Order data

joined_df = customer_df.join(
    order_df,
    customer_df.customer_id == order_df.customer_id,
    "left"
)

orders_grouped_df = joined_df.groupBy(
    
    customer_df.customer_id
).agg(
    
    collect_list(
        struct(
            order_df.order_id.alias("order_id"),
            order_df.order_date.alias("order_date"),
            order_df.order_status.alias("order_status"),
            order_df.order_channel.alias("order_channel"),
            order_df.payment_method.alias("payment_method"),
            order_df.shipping_city.alias("shipping_city")
        )
    ).alias("orders")
)

final_customer_order_df = customer_df.join(
    orders_grouped_df,
    on = "customer_id",
    how = "left"
)

print("Final customer count:", final_customer_order_df.count())

print(
    "Customers with no orders:",
    final_customer_order_df.filter(size("orders") == 0).count()
)

print(
    "Total nested orders:",
    final_customer_order_df.select(explode("orders").alias("order")).count()
)