import org.apache.spark.sql.SparkSession
import java.io._

object LineCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Line Count")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Charger le fichier CSV
    val filePath = "src/main/resources/agg_match_stats_0.csv"
    val data = sc.textFile(filePath)

    // Compter les lignes
    val lineCount = data.count()

    println(s"Le fichier contient $lineCount lignes.")

    // Prendre les 100 000 premières lignes
    val sampleData = data.take(100000)

    // Sauvegarder dans un fichier
    val sampleFilePath = "src/main/resources/agg_match_stats_0_100000.csv"
    val writer = new PrintWriter(new File(sampleFilePath))

    sampleData.foreach(line => writer.println(line))
    writer.close()

    println(s"Un échantillon de 100 000 lignes a été créé et sauvegardé dans $sampleFilePath.")

    spark.stop()
  }
}
