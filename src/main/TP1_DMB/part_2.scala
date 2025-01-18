import org.apache.spark.sql.SparkSession

object part_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PUBG Player Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Charger le fichier CSV
    val filePath = "src/main/resources/agg_match_stats_0_100000.csv"
    val data = sc.textFile(filePath)

    // Ignorer la première ligne (en-tête)
    val header = data.first()
    val rows = data.filter(line => line != header)

    // Extraire les stats des joueurs
    val playerStats = rows.map { line =>
      val cols = line.split(",")
      val playerName = cols(11) // Colonne avec le nom du joueur
      val kills = cols(10).toInt // Colonne avec les kills
      val placement = cols(14).toInt // Colonne avec le classement
      (playerName, (kills, placement, 1))
    }

    // Agréger les données et calculer les moyennes
    val aggregatedStats = playerStats
      .reduceByKey { (a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      }
      .mapValues { case (totalKills, totalPlacement, totalGames) =>
        (totalKills.toDouble / totalGames, totalPlacement.toDouble / totalGames, totalGames)
      }

    // Filtrer les joueurs avec au moins 4 parties
    val filteredPlayers = aggregatedStats.filter { case (_, (_, _, totalGames)) =>
      totalGames >= 4
    }

    // Obtenir les 10 meilleurs joueurs par kills
    val top10Kills = filteredPlayers
      .sortBy({ case (_, (avgKills, _, _)) => avgKills }, ascending = false)
      .take(10)

    // Obtenir les 10 meilleurs joueurs par position
    val top10Placement = filteredPlayers
      .sortBy({ case (_, (_, avgPlacement, _)) => avgPlacement }, ascending = true)
      .take(10)

    // Afficher les résultats
    println("Top 10 joueurs par kills (avec leurs noms) :")
    top10Kills.foreach { case (playerName, (avgKills, _, totalGames)) =>
      println(s"Nom: $playerName, Moyenne des kills: $avgKills, Parties jouées: $totalGames")
    }

    println("\nTop 10 joueurs par position (avec leurs noms) :")
    top10Placement.foreach { case (playerName, (_, avgPlacement, totalGames)) =>
      println(s"Nom: $playerName, Moyenne de position: $avgPlacement, Parties jouées: $totalGames")
    }

    spark.stop()
  }
}
