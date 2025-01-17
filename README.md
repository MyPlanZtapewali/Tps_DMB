# TP1 DMB

## A. Préparation du jeu de données

### 1 Décompresser le jeu de données dans l'espace de travail 
(C'est fait)
J'ai téléchargé le fichier `agg_match_stats_0.csv` de 2 Go depuis kaggle.

### 2 Combien de lignes fait ce fichier ?

```scala
import org.apache.spark.sql.SparkSession

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

    spark.stop()
  }
}
```

`Le fichier contient 13849288 lignes.`

### 3 Gérer le travail sur l'échantillon contenant les 100 mille premières lignes

D'abord penser à importer la bibliothèque `import java.io._` dans le code et ensuite :

ajouter dans le code précedent cet bout de code : 

```scala
// Prendre les 100 000 premières lignes
    val sampleData = data.take(100000)

    // Sauvegarder dans un fichier
    val sampleFilePath = "src/main/resources/agg_match_stats_0_100000.csv"
    val writer = new PrintWriter(new File(sampleFilePath))

    sampleData.foreach(line => writer.println(line))
    writer.close()

    println(s"Un échantillon de 100 000 lignes a été créé et sauvegardé dans $sampleFilePath.")
```

## B. Les meilleurs joueurs

### 1. Charger le jeu de données

Pour charger le fichier CSV : 
```scala
import org.apache.spark.sql.SparkSession

object PlayerAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PUBG Player Analysis")
      .master("local[*]") // Mode local
      .getOrCreate()

    val sc = spark.sparkContext

    // Charger le fichier CSV
    val filePath = "src/main/resources/agg_match_stats_0_100000.csv"
    val data = sc.textFile(filePath)

    // Ignorer la première ligne (en-tête)
    val header = data.first()
    val rows = data.filter(line => line != header)

    // Étapes suivantes ici
  }
}
```
### 2. Obtenir le nom du joueur et ses stats (kills ou position)

Pour extraire les informations nécessaires :
```scala
// Extraire le nom du joueur, les kills et la position
    val playerStats = rows.map { line =>
      val cols = line.split(",")
      val playerName = cols(11) // Colonne avec le nom du joueur
      val kills = cols(10).toInt // Colonne avec les kills
      val placement = cols(14).toInt // Colonne avec le classement
      (playerName, (kills, placement, 1))
    }
```
### 3. Obtenir le nom du joueur et ses stats (kills ou position)

Pour regrouper les joueurs et calculer la moyenne des kills ou de la position :
```scala
// Agréger les données et calculer les moyennes
    val aggregatedStats = playerStats
      .reduceByKey { (a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      }
      .mapValues { case (totalKills, totalPlacement, totalGames) =>
        (totalKills.toDouble / totalGames, totalPlacement.toDouble / totalGames, totalGames)
      }
```

### 4. Obtenir les 10 meilleurs joueurs

Pour trier et récupérer les meilleurs joueurs selon les kills ou la position :

Top 10 par kills :
```scala
    // Obtenir les 10 meilleurs joueurs par kills
    val top10Kills = filteredPlayers
      .sortBy({ case (_, (avgKills, _, _)) => avgKills }, ascending = false)
      .take(10)
```

Top 10 par position :
```scala
// Obtenir les 10 meilleurs joueurs par position
    val top10Placement = filteredPlayers
      .sortBy({ case (_, (_, avgPlacement, _)) => avgPlacement }, ascending = true)
      .take(10)
```

### 5. Filtrer les joueurs avec au moins 4 parties

Pour ne garder que les joueurs ayant participé à 4 parties ou plus :
```scala
// Filtrer les joueurs avec au moins 4 parties
    val filteredPlayers = aggregatedStats.filter { case (_, (_, _, totalGames)) =>
      totalGames >= 4
    }
```

### 6. Gérer un joueur spécifique

```scala
val specificPlayer = aggregatedStats.filter { case (playerName, _) =>
  playerName == "NomDuJoueur"
}
```

### 7.  Partager et analyser l’affirmation

Conclusion pour la question 7 :

* Faire beaucoup de kills n’est pas nécessaire pour obtenir une bonne position dans une partie PUBG. Les résultats montrent que certains joueurs avec peu de kills peuvent avoir une position moyenne excellente grâce à un style de jeu axé sur la survie.

* Inversement, les joueurs avec beaucoup de kills peuvent ne pas atteindre les premières positions, ce qui suggère qu’un style de jeu agressif n’est pas toujours efficace pour gagner.

* L’affirmation initiale (« il est nécessaire d’éliminer un maximum de concurrents pour être performant ») est donc fausse ou tout au moins incomplète. La performance dépend d’un équilibre entre stratégie, survie, et agressivité.

## C. Score des joueurs

