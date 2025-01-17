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



