import org.apache.spark.sql.SparkSession

object PlayerScoreAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PUBG Player Score Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Charger le fichier CSV
    val filePath = "src/main/resources/agg_match_stats_0_100000.csv"
    val data = sc.textFile(filePath)

    // Ignorer la première ligne (en-tête)
    val header = data.first()
    val rows = data.filter(line => line != header)

    // Fonction pour calculer le score
    def calculateScore(kills: Int, assists: Int, damageDealt: Double, placement: Int): Double = {
      val placementScore = 1000 - (placement - 1) * 10 // Score décroissant en fonction du placement
      val score = (kills * 100) + (assists * 50) + damageDealt + placementScore
      score
    }

    // Extraire les stats des joueurs et calculer leur score
    val playerScores = rows.map { line =>
      val cols = line.split(",")
      val playerName = cols(11) // Colonne avec le nom du joueur
      val kills = cols(10).toInt // Colonne avec les kills
      val assists = cols(5).toInt // Colonne avec les assists
      val damageDealt = cols(9).toDouble // Colonne avec les dommages infligés
      val placement = cols(14).toInt // Colonne avec le placement

      val score = calculateScore(kills, assists, damageDealt, placement)
      (playerName, (score, 1)) // (Nom du joueur, (Score, 1 Partie))
    }

    // Agréger les scores et les parties jouées par joueur
    val aggregatedScores = playerScores
      .reduceByKey { (a, b) =>
        val totalScore = a._1 + b._1 // Somme des scores
        val totalGames = a._2 + b._2 // Somme des parties jouées
        (totalScore, totalGames)
      }

    // Filtrer les joueurs ayant joué au moins 4 parties
    val filteredScores = aggregatedScores.filter { case (_, (_, totalGames)) =>
      totalGames >= 4
    }

    // Obtenir les 10 meilleurs joueurs selon le score
    val top10PlayersByScore = filteredScores
      .mapValues { case (totalScore, _) => totalScore } // On ne garde que le score total
      .sortBy({ case (_, score) => score }, ascending = false)
      .take(10)

    // Afficher les résultats
    println("Top 10 joueurs par score (avec leurs noms, ayant joué au moins 4 parties) :")
    top10PlayersByScore.foreach { case (playerName, score) =>
      println(s"Nom: $playerName, Score total: $score")
    }

    spark.stop()
  }
}
