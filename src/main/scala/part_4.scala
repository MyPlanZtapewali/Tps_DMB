import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import java.time.Instant

object PlayerPersistenceAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PUBG Player Persistence Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Charger le fichier CSV
    val filePath = "src/main/resources/agg_match_stats_0.csv"
    val data = sc.textFile(filePath)

    // Ignorer la première ligne (en-tête)
    val header = data.first()
    val rows = data.filter(line => line != header)

    // Extraire les noms des joueurs et calculer leurs scores
    val playerScores = rows.map { line =>
      val cols = line.split(",")
      val playerName = cols(11) // Colonne avec le nom du joueur
      val kills = cols(10).toInt // Colonne avec les kills
      val assists = cols(5).toInt // Colonne avec les assists
      val damageDealt = cols(9).toDouble // Colonne avec les dommages infligés
      val placement = cols(14).toInt // Colonne avec le placement

      val score = (kills * 100) + (assists * 50) + damageDealt + (1000 - (placement - 1) * 10)
      (playerName, score)
    }

    // Fonction pour mesurer le temps
    def time[R](block: => R): (R, Long) = {
      val start = Instant.now.toEpochMilli
      val result = block
      val end = Instant.now.toEpochMilli
      (result, end - start)
    }

    // Agréger les scores par joueur
    val aggregatedScores = playerScores.reduceByKey(_ + _)

    // Obtenir les joueurs distincts
    val distinctPlayers = aggregatedScores.map(_._1).distinct()

    // Persister l'état pour éviter de recalculer
    aggregatedScores.persist(StorageLevel.MEMORY_AND_DISK)
    distinctPlayers.persist(StorageLevel.MEMORY_AND_DISK)

    // Obtenir les 10 meilleurs joueurs
    val top10Players = aggregatedScores
      .sortBy({ case (_, score) => score }, ascending = false)
      .take(10)

    // Sans persistance
    val (resultWithoutPersist, timeWithoutPersist) = time {
      val top10 = aggregatedScores.sortBy({ case (_, score) => score }, ascending = false).take(10)
      val totalPlayers = distinctPlayers.count()
      (top10, totalPlayers)
    }

    // Avec persistance
    val (resultWithPersist, timeWithPersist) = time {
      aggregatedScores.persist(StorageLevel.MEMORY_AND_DISK)
      distinctPlayers.persist(StorageLevel.MEMORY_AND_DISK)

      val top10 = aggregatedScores.sortBy({ case (_, score) => score }, ascending = false).take(10)
      val totalPlayers = distinctPlayers.count()
      (top10, totalPlayers)
    }

    // Obtenir le nombre total de joueurs distincts
    val totalDistinctPlayers = distinctPlayers.count()

    // Afficher les résultats
    println("Top 10 joueurs par score :")
    top10Players.foreach { case (playerName, score) =>
      println(s"Nom: $playerName, Score total: $score")
    }

    println(s"Nombre total de joueurs distincts : $totalDistinctPlayers")

    println(s"Temps sans persistance : $timeWithoutPersist ms")
    println(s"Temps avec persistance : $timeWithPersist ms")

    spark.stop()
  }
}
