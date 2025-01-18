# TP2 DMB

## A. Préparation des données

### 1. Télécharger le jeu de données 
____

le jeu de données à été téléchargé à l'addresse suivante : `https://s3.amazonaws.com/tripdata/index.html`
____

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

