import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object filtering002
{
    def main(args: Array[String])
    {
        println("\n\n>>>>> START OF PROGRAM <<<<<\n\n")
        val conf = new SparkConf().setAppName("filtering")
        val sc = new SparkContext(conf);

        val logfile = "./../FilterSpark/input/E02016.csv"
        val logrdd = sc.textFile(logfile)

        val liverpool_matches   = logrdd.filter(s => s.contains("Liverpool"))
        println("\n\n>>>>> LIVERPOOL MATCHES : "+liverpool_matches.count()+" <<<<<\n\n")

        val liverpool_vs_chelsea = logrdd.filter(s => (s.contains("Liverpool") & s.contains("Chelsea")))

        println("\n\n>>>>> LIVERPOOL VS CHELSEA MATCHES : "+liverpool_vs_chelsea.count()+" <<<<<\n\n")

        println("\n\n>>>>> END OF PROGRAM <<<<<\n\n")
    }
}