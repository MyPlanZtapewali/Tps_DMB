import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object CityBikeGraphDegrees {
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
      format.parse(tString.trim).getTime()
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

    // Extraire le sous-graphe selon l'intervalle temporel
    val startEpoch = timeToLong("2021-12-05 00:00:00")
    val endEpoch = timeToLong("2021-12-25 23:59:59")

    val subgraph = graph.subgraph(
      edge => edge.attr.startTime >= startEpoch && edge.attr.endTime <= endEpoch
    )

    // Calcul des degrés sortants (nombre de trajets sortants)
    val outDegrees = subgraph.aggregateMessages[Int](
      sendMsg = triplet => triplet.sendToSrc(1), // Incrémenter le compteur pour le noeud source
      mergeMsg = (a, b) => a + b                // Combiner les messages
    )

    // Calcul des degrés entrants (nombre de trajets entrants)
    val inDegrees = subgraph.aggregateMessages[Int](
      sendMsg = triplet => triplet.sendToDst(1), // Incrémenter le compteur pour le noeud destination
      mergeMsg = (a, b) => a + b                // Combiner les messages
    )

    // Afficher les 10 stations avec le plus de trajets sortants
    println("Top 10 stations par trajets sortants :")
    outDegrees.sortBy(_._2, ascending = false).take(10).foreach {
      case (vertexId, count) => println(s"Station ID: $vertexId, Trajets sortants: $count")
    }

    // Afficher les 10 stations avec le plus de trajets entrants
    println("Top 10 stations par trajets entrants :")
    inDegrees.sortBy(_._2, ascending = false).take(10).foreach {
      case (vertexId, count) => println(s"Station ID: $vertexId, Trajets entrants: $count")
    }

    spark.stop()
  }
}
