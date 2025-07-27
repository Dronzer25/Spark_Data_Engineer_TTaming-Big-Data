from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Total Spent")
sc = SparkContext(conf = conf)

def parseLine(line):
    data = line.split(",")
    cust_ID = int(data[0])
    amount = float(data[2])
    return (cust_ID , amount)


input = sc.textFile("file:///sparkcourse/ml-100k/customer-orders.csv")
parsedData = input.map(parseLine)

# Step 1: Sum amounts by customer
totalsByCustomer = parsedData.reduceByKey(lambda x, y: x + y)

# Step 2: Swap to (amount, customerID) and sort by amount descending
sortedByAmount = totalsByCustomer.map(lambda pair: (pair[1], pair[0])).sortByKey(ascending=True)

# Output
results = sortedByAmount.collect()
for customer, total in results:
    print(f"{customer}, {total}")
    
    
 