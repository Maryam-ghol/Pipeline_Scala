
import org.apache.spark.sql.DataFrame


def extractDf(fileName: String) : DataFrame = {
  val df= spark.read.option("header","true").option("inferSchema","true").csv(fileName)
  return df
  }

def transformDf(df:DataFrame) : DataFrame={
  val outDf= df.orderBy(asc("VendorID"))
  return outDf
}

def loadDf(df:DataFrame): Boolean={
  df.write.parquet("NYCfiles/green_tripdata_2021-2.parquet")
  return true
}

def NYCMain():Unit={
  val df= extractDf("NYCfiles/green_tripdata_2021-2.csv")
  val tranfomedDf=transformDf(df)
  for (row <- tranfomedDf.head(2)){
    println(row)
  }
  val result= loadDf(tranfomedDf)
}

NYCMain()
