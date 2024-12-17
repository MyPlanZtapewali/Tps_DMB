// Code Test

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object GraphXExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("GraphX Example")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // Exemple de création d'un graphe
    val vertices = sc.parallelize(Seq(
      (1L, ("A")),
      (2L, ("B")),
      (3L, ("C")),
      (4L, ("D"))
    ))

    val edges = sc.parallelize(Seq(
      Edge(1L, 2L, "ab"),
      Edge(2L, 3L, "bc"),
      Edge(3L, 4L, "cd")
    ))

    val graph = Graph(vertices, edges)

    // Afficher les sommets et les arêtes du graphe
    graph.vertices.collect().foreach(println)
    graph.edges.collect().foreach(println)

    spark.stop()
  }
}
