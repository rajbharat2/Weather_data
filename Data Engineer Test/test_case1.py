#This modules is used to test the result by reading the parquet file

import pyarrow.parquet as pq

filename = r".\mydata\year=2016\month=3\delta_data.parquet"

pq_file = pq.ParquetFile(filename)



# to get the min max value from data
column = 7 # Screen temperature column index
data = [["rowgroup","min", "max"]]
for rg in range(pq_file.metadata.num_row_groups):
    rg_meta = pq_file.metadata.row_group(rg)
    data.append([rg, str(rg_meta.column(column).statistics.min),str(rg_meta.column(column).statistics.max)])
print("The row gruop value & min,max value for a column can be identified \n")
print(data)

# To obtain metadata of specific row group from a file
data = [["columns:", pq_file.metadata.num_columns],
        ["rows:", pq_file.metadata.num_rows],
        ["row_roups:", pq_file.metadata.num_row_groups]
        ]

rg_meta = pq_file.metadata.row_group(16)
print(rg_meta.column(7),'\n')



#
#test result for file 2
#From file 1 data obesrvation row group 16 & 15.8 is higest temp

parquet_file = pq.ParquetFile(filename)
table = parquet_file.read_row_group(16)
df = table.to_pandas()
rslt_df = df[df['ScreenTemperature']== 15.8]

print("test result file one----------")
print((rslt_df))

