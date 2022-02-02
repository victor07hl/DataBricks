# Databricks notebook source
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

x = np.array([0,1,2,3,4])
y = x**2

# COMMAND ----------

plt.plot(x,y)
plt.show()

# COMMAND ----------

plt.plot(x,y)
plt.grid(True)
plt.show()

# COMMAND ----------


