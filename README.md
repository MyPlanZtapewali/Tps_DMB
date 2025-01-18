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

### 1.  Partager et analyser l’affirmation

La fonction de score sera basée sur les règles suivantes :

* 50 points par assistance (assists),
* 1 point par dommage causé (damage_dealt),
* 100 points par élimination (kills),
* 1000 points pour la 1re position, 990 pour la 2ᵉ, etc.

Fonction Scala :
```scala
def calculateScore(kills: Int, assists: Int, damageDealt: Double, placement: Int): Double = {
  val placementScore = 1000 - (placement - 1) * 10 // Ex : 1ère place = 1000, 2e place = 990, etc.
  val score = (kills * 100) + (assists * 50) + damageDealt + placementScore
  score
}
```

### 2. Comparez ce classement avec les deux précédents critères

```scala
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
```



Comparaison avec les classements précédents :

#### a. Classement par position (système initial) :

Top 3 :
* ChanronG (Moyenne de position : 9.0, Parties jouées : 4)
* JustTuatuatua (Moyenne de position : 10.75, Parties jouées : 4)
* dman4771 (Moyenne de position : 11.5, Parties jouées : 4)

Observations :
* Les joueurs ayant une position moyenne basse (proche de 1) dominent ce classement, ce qui reflète leur capacité à survivre plus longtemps.
* Les joueurs classés ici ne prennent pas nécessairement en compte leurs kills, assists ou dommages infligés.

#### b. Classement par kills (système initial) :

Top 3 :
* LawngD-a-w-n-g (Moyenne des kills : 2.2, Parties jouées : 5)
* siliymaui125 (Moyenne des kills : 2.0, Parties jouées : 4)
* Dcc-ccD (Moyenne des kills : 1.75, Parties jouées : 4)

Observations :
* Ce classement met en avant les joueurs les plus agressifs et actifs dans les parties (meilleur ratio kills/partie).
* La performance de survie (position) n’est pas prise en compte ici.

#### c. Classement par score (nouveau système) :

Top 3 :
* Joueur anonyme ("") (Score total : 140698.0, Parties jouées : 142)
* LawngD-a-w-n-g (Score total : 6153.0, Parties jouées : 5)
* Dcc-ccD (Score total : 5249.0, Parties jouées : 4)

Observations :
* Le joueur anonyme domine ce classement, principalement parce qu'il a participé à 142 parties, accumulant un grand score malgré des moyennes faibles dans les kills et positions. Cela reflète un avantage pour les joueurs réguliers.
* LawngD-a-w-n-g reste en bonne position, confirmant que ses kills élevés et sa participation à plusieurs parties influencent positivement son score.
* ChanronG, leader du classement par position, est maintenant 7ᵉ, ce qui montre que sa performance en kills, assists et dégâts est moins marquante que sa capacité à bien se placer.

| Joueur          | Moyenne de position | Moyenne de kills | Score total | Classement par position | Classement par kills | Classement par score |
|------------------|---------------------|------------------|-------------|--------------------------|-----------------------|-----------------------|
| LawngD-a-w-n-g  | Non classé          | 2.2              | 6153.0      | Non classé               | 1                     | 2                     |
| siliymaui125     | 22.75              | 2.0              | 4731.0      | 9                        | 2                     | 5                     |
| Dcc-ccD          | 14.5               | 1.75             | 5249.0      | 7                        | 3                     | 3                     |
| dman4771         | 11.5               | 1.75             | 4861.0      | 3                        | 4                     | 4                     |
| ChanronG         | 9.0                | Non classé       | 4510.0      | 1                        | Non classé            | 7                     |
| JustTuatuatua    | 10.75              | 0.75             | 4693.0      | 2                        | 9                     | 6                     |

### Conclusion : 

### a. Impact des kills et de la position :

* Le classement par score est plus équilibré, car il prend en compte plusieurs aspects du jeu :
** Kills (100 points chacun),
** Assists (50 points chacun),
** Dommages infligés,
** Position (score décroissant avec le rang).
* Les joueurs avec beaucoup de kills dominent toujours, mais ceux avec de bonnes positions (comme ChanronG) restent compétitifs.

### b. Joueur anonyme ("") :

* Ce joueur bénéficie de sa forte participation (142 parties) pour accumuler des points, même avec des performances individuelles modestes (kills et position).

### c. Effet de la participation :

* Le système de points favorise les joueurs constants et actifs, comme le joueur anonyme. Cela peut désavantager les joueurs très performants mais ayant joué peu de parties.

### d. Conclusion générale :

* Le système par score est plus complet, car il considère à la fois la survie (position) et l’agressivité (kills, assists, dommages).
* Cependant, il peut introduire un biais en favorisant les joueurs ayant participé à un grand nombre de parties, indépendamment de leur efficacité.
