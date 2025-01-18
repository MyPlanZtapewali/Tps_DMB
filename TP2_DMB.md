# TP2 DMB

## A. Préparation des données

### 1. Télécharger le jeu de données 
____

le jeu de données à été téléchargé à l'addresse suivante : `https://s3.amazonaws.com/tripdata/index.html`

### 2. Créez un graphe dont les noeuds représentent des stations de vélos et les relations représentent des trajets de vélos entre deux stations 

____
* Création des noeuds representant les stations

J'ai en premier lieu faire la définition de la classe `station` : 

```scala
case class Station(id: String, name: String, latitude: Double, longitude: Double)
```

Cette classe modélise les stations avec leurs attributs (`id`, `name`, ``latitude``, ``longitude``).

J'extrais ensuite les informations des stations depuis le fichier CSV dans la section 

```scala
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

  Seq(
    (srcId.hashCode.toLong, Station(srcId, srcName, srcLat, srcLng)),
    (dstId.hashCode.toLong, Station(dstId, dstName, dstLat, dstLng))
  )
}.distinct()
```

Les identifiants (``srcId`` et ``dstId``) sont transformés en ``VertexId`` (type ``Long``) à l'aide de ``hashCode.toLong``.
Chaque station est associée à un objet ``Station``.
____
* Création des arêtes représentant les trajets

J'ai également en premier lieu définit la classe `trip` :

```scala
case class Trip(src: String, dst: String, startTime: Long, endTime: Long)
```

Cette classe modélise les trajets entre les stations.

J'extrais ensuite les informations des trajets depuis le fichier CSV

```scala
val trips: RDD[Edge[Trip]] = rows.flatMap { line =>
  val cols = line.split(",")

  try {
    val srcId = cols(5).hashCode.toLong
    val dstId = cols(7).hashCode.toLong
    val startTime = timeToLong(cols(2))
    val endTime = timeToLong(cols(3))

    Seq(Edge(srcId, dstId, Trip(cols(5), cols(7), startTime, endTime)))
  } catch {
    case _: Exception =>
      Seq.empty[Edge[Trip]]
  }
}
```
Chaque trajet est modélisé par un objet ``Edge`` contenant :
* ``srcId`` (station source),
* ``dstId`` (station destination),
* Les informations du trajet encapsulées dans un objet ``Trip``.


____
*  Création du graphe
```scala
val graph = Graph(stations, trips)
```
____
* Conversion des dates en millisecondes depuis Epoch
```scala
def timeToLong(tString: String): Long = {
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  format.parse(tString.trim).getTime()
}
```
____
* Affichage des résultats
````scala
println("Stations (nœuds) :")
graph.vertices.take(10).foreach(println)

println("Trajets (arêtes) :")
graph.edges.take(10).foreach(println)
````
____

## B. Calcul de degré

### 1. Extraire le sous-graphe

L'objectif est de filtrer le graphe initial pour ne conserver que les trajets dont les horaires (startTime, endTime) sont compris entre le 05-12-2021 et le 25-12-2021.

````scala
// Définir les bornes temporelles en millisecondes (Epoch)
val startEpoch = timeToLong("2021-12-05 00:00:00")
val endEpoch = timeToLong("2021-12-25 23:59:59")

// Extraire le sous-graphe
val subgraph = graph.subgraph(
  edge => edge.attr.startTime >= startEpoch && edge.attr.endTime <= endEpoch
)
````
``subgraph :`` Un graphe contenant uniquement les trajets (arêtes) dont les horaires respectent les bornes spécifiées.

### 2. Calculer les degrés et Afficher les 10 stations avec le plus de trajets

> ### Calcul des degrés entrants et sortants
> J'ai utilisé ``aggregateMessages`` pour calculer : 
> * Le nombre de trajets sortants pour chaque station (degré sortant). 
> * Le nombre de trajets entrants pour chaque station (degré entrant).
>  ````scala
>  // Calcul des degrés sortants (nombre de trajets sortants)
>  val outDegrees = subgraph.aggregateMessages[Int](
>  sendMsg = triplet => triplet.sendToSrc(1), // Incrémenter le compteur pour le noeud source
>  mergeMsg = (a, b) => a + b                // Combiner les messages
>  )
>  // Calcul des degrés entrants (nombre de trajets entrants)
>  val inDegrees = subgraph.aggregateMessages[Int]( 
>  sendMsg = triplet => triplet.sendToDst(1), // Incrémenter le compteur pour le noeud destination 
>  mergeMsg = (a, b) => a + b                // Combiner les messages
>  )

____

> ### Afficher les 10 stations avec le plus de trajets entrants et sortants
> Une fois les degrés calculés, je les ai triés pour afficher les 10 stations ayant le plus de trajets entrants et sortants. :
> ````scala
> // Top 10 des stations avec le plus de trajets sortants
> println("Top 10 stations par trajets sortants :")
> outDegrees.sortBy(_._2, ascending = false).take(10).foreach {
> case (vertexId, count) => println(s"Station ID: $vertexId, Trajets sortants: $count")
> }
> 
> // Top 10 des stations avec le plus de trajets entrants
> println("Top 10 stations par trajets entrants :")
> inDegrees.sortBy(_._2, ascending = false).take(10).foreach {
> case (vertexId, count) => println(s"Station ID: $vertexId, Trajets entrants: $count")
> }
____



Resultat après exécution du code :

(Top 10 stations par trajets entrants)
> Station ID: 70384220, Trajets entrants: 1640 

> Station ID: 68508345, Trajets entrants: 1408 

> Station ID: 68508344, Trajets entrants: 1149 

> Station ID: 70384376, Trajets entrants: 1094 

> Station ID: 70384224, Trajets entrants: 954 

> Station ID: 68508348, Trajets entrants: 908 

> Station ID: 70384407, Trajets entrants: 879 

> Station ID: 68508346, Trajets entrants: 835 

> Station ID: 70384223, Trajets entrants: 809 

> Station ID: 70385181, Trajets entrants: 790

(Top 10 stations par trajets sortants)
> Station ID: 70384220, Trajets sortants: 1650

> Station ID: 68508345, Trajets sortants: 1500

> Station ID: 70384376, Trajets sortants: 1229

> Station ID: 68508344, Trajets sortants: 1136

> Station ID: 70384224, Trajets sortants: 929

> Station ID: 70384407, Trajets sortants: 903

> Station ID: 68508348, Trajets sortants: 864

> Station ID: 68508346, Trajets sortants: 844

> Station ID: 70384223, Trajets sortants: 819

> Station ID: 68513151, Trajets sortants: 747