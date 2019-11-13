import org.apache.spark.{SparkConf, SparkContext}

object second {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    val conf = new SparkConf().setAppName("secondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("inputs\\shakespeare.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => (k(0), k(1)) }

    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))

    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    resultRDD.saveAsTextFile("outputs")
  }
}