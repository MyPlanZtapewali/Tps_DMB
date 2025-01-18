// Déclarez les classes pour les stations et les trajets
case class Station(id: String, name: String, latitude: Double, longitude: Double)
case class Trip(src: String, dst: String, startTime: Long, endTime: Long)

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object CityBikeGraph {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("CityBike GraphX")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Charger le fichier CSV
    val filePath = "src/main/resources/JC-202112-citibike-tripdata.csv"
    val data = sc.textFile(filePath)

    // Ignorer l'en-tête
    val header = data.first()
    val rows = data.filter(line => line != header)

    // Fonction pour convertir une date en millisecondes (Epoch)
    def timeToLong(tString: String): Long = {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.parse(tString.trim).getTime() // Supprimer les espaces inutiles
    }

    // Fonction pour convertir en Double avec une validation
    def safeToDouble(value: String): Double = {
      try {
        value.trim.toDouble // Supprimer les espaces inutiles avant la conversion
      } catch {
        case _: NumberFormatException => 0.0 // Retourner une valeur par défaut si conversion échoue
      }
    }

    // Créer les nœuds (stations) avec validation
    val stations: RDD[(VertexId, Station)] = rows.flatMap { line =>
      val cols = line.split(",")

      val srcId = cols(5)
      val srcName = cols(4)
      val srcLat = safeToDouble(cols(8))
      val srcLng = safeToDouble(cols(9))

      val dstId = cols(7)
      val dstName = cols(6)
      val dstLat = safeToDouble(cols(10))
      val dstLng = safeToDouble(cols(11))

      // Retourner deux stations : source et destination
      Seq(
        (srcId.hashCode.toLong, Station(srcId, srcName, srcLat, srcLng)),
        (dstId.hashCode.toLong, Station(dstId, dstName, dstLat, dstLng))
      )
    }.distinct()

    // Créer les arêtes (trajets) avec validation
    val trips: RDD[Edge[Trip]] = rows.flatMap { line =>
      val cols = line.split(",")

      try {
        // Convertir les valeurs et retourner un Edge
        val srcId = cols(5).hashCode.toLong
        val dstId = cols(7).hashCode.toLong
        val startTime = timeToLong(cols(2)) // Convertir le temps de début
        val endTime = timeToLong(cols(3)) // Convertir le temps de fin

        // Retourner l'arête dans un Seq
        Seq(Edge(srcId, dstId, Trip(cols(5), cols(7), startTime, endTime)))
      } catch {
        case _: Exception =>
          // Si une exception survient, retourner un Seq vide
          Seq.empty[Edge[Trip]]
      }
    }


    // Créer le graphe
    val graph = Graph(stations, trips)

    // Afficher un aperçu des nœuds et des arêtes
    println("Stations (nœuds) :")
    graph.vertices.take(10).foreach(println)

    println("Trajets (arêtes) :")
    graph.edges.take(10).foreach(println)

    spark.stop()
  }
}
