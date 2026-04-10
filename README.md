# lakehouse-energy-trading
Data lakehouse project that contains 3 layers of data storage, bronze, silver and gold.

Silver layer functions are triggered by S3 Event notification on bucket 'data-lake-energy-trade', there is trigger per function and each datasource has a function.
