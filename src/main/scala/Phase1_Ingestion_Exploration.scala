// ============================================================================
// PHASE 1 : INGESTION ET EXPLORATION INITIALE
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Phase1_IngestionExploration {
  
  def main(args: Array[String]): Unit = {
    
    // 1. Créer la SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Analysis - Phase 1")
      .master("local[*]")  // Utilise tous les cœurs disponibles
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    
    import spark.implicits._
    
    // Configuration du niveau de log
    spark.sparkContext.setLogLevel("WARN")
    
    println("=" * 80)
    println("PHASE 1 : INGESTION ET EXPLORATION INITIALE")
    println("=" * 80)
    
    // 2. Charger les données des taxis
    val taxiDataPath = "C:/Users/LENOVO/Documents/NYC-Taxi-Analysis/data/raw/yellow_tripdata_2024-01.parquet"

    
    println("\n📂 Chargement des données...")
    val taxiDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(taxiDataPath)
    
    // Mettre en cache pour améliorer les performances
    taxiDF.cache()
    
    // 3. EXPLORATION BASIQUE
    println("\n" + "=" * 80)
    println("📊 STATISTIQUES GÉNÉRALES")
    println("=" * 80)
    
    // Nombre total de trajets
    val totalTrips = taxiDF.count()
    println(s"\n✓ Nombre total de trajets : ${totalTrips}")
    
    // Afficher le schéma
    println("\n📋 Schéma des données :")
    taxiDF.printSchema()
    
    // Aperçu des premières lignes
    println("\n👀 Aperçu des 10 premières lignes :")
    taxiDF.show(10, truncate = false)
    
    // 4. ANALYSE DE LA PÉRIODE COUVERTE
    println("\n" + "=" * 80)
    println("📅 PÉRIODE COUVERTE")
    println("=" * 80)
    
    val periodStats = taxiDF.agg(
      min("tpep_pickup_datetime").alias("date_debut"),
      max("tpep_pickup_datetime").alias("date_fin")
    )
    
    periodStats.show(false)
    
    // Calculer la durée de la période
    val periodDF = periodStats.select(
      col("date_debut"),
      col("date_fin"),
      datediff(col("date_fin"), col("date_debut")).alias("nombre_jours")
    )
    
    periodDF.show(false)
    
    // 5. ANALYSE DES VALEURS MANQUANTES
    println("\n" + "=" * 80)
    println("❓ ANALYSE DES VALEURS MANQUANTES")
    println("=" * 80)
    
    val columnNames = taxiDF.columns
    val nullCounts = columnNames.map { colName =>
      // Pour les colonnes String, vérifier aussi les chaînes vides
      val dataType = taxiDF.schema(colName).dataType.typeName
      val nullCount = if (dataType == "string") {
        taxiDF.filter(col(colName).isNull || col(colName) === "").count()
      } else {
        taxiDF.filter(col(colName).isNull).count()
      }
      (colName, nullCount)
    }
    
    println("\nNombre de valeurs manquantes par colonne :")
    nullCounts.foreach { case (colName, nullCount) =>
      val percentage = (nullCount.toDouble / totalTrips * 100)
      println(f"  $colName%-30s : $nullCount%10d (${percentage}%.2f%%)")
    }
    
    // 6. DÉTECTION DES VALEURS ABERRANTES
    println("\n" + "=" * 80)
    println("⚠️  DÉTECTION DES VALEURS ABERRANTES")
    println("=" * 80)
    
    // Distance = 0 ou négative
    val zeroDistance = taxiDF.filter(col("trip_distance") <= 0).count()
    println(f"\n✗ Trajets avec distance <= 0 : $zeroDistance%,d (${zeroDistance.toDouble / totalTrips * 100}%.2f%%)")
    
    // Tarif négatif
    val negativeFare = taxiDF.filter(col("fare_amount") < 0).count()
    println(f"✗ Trajets avec tarif négatif : $negativeFare%,d (${negativeFare.toDouble / totalTrips * 100}%.2f%%)")
    
    // Nombre de passagers = 0
    val zeroPassengers = taxiDF.filter(col("passenger_count") <= 0).count()
    println(f"✗ Trajets avec 0 passager : $zeroPassengers%,d (${zeroPassengers.toDouble / totalTrips * 100}%.2f%%)")
    
    // Tarif total négatif
    val negativeTotalAmount = taxiDF.filter(col("total_amount") < 0).count()
    println(f"✗ Trajets avec montant total négatif : $negativeTotalAmount%,d (${negativeTotalAmount.toDouble / totalTrips * 100}%.2f%%)")
    
    // 7. STATISTIQUES DESCRIPTIVES
    println("\n" + "=" * 80)
    println("📈 STATISTIQUES DESCRIPTIVES")
    println("=" * 80)
    
    println("\n📊 Statistiques sur les colonnes numériques principales :")
    taxiDF.select(
      "trip_distance",
      "fare_amount",
      "total_amount",
      "passenger_count"
    ).describe().show()
    
    // 8. DISTRIBUTION DES TYPES DE PAIEMENT
    println("\n" + "=" * 80)
    println("💳 DISTRIBUTION DES TYPES DE PAIEMENT")
    println("=" * 80)
    
    val paymentDistribution = taxiDF.groupBy("payment_type")
      .agg(
        count("*").alias("nombre_trajets"),
        (count("*") / totalTrips * 100).alias("pourcentage")
      )
      .orderBy(desc("nombre_trajets"))
    
    paymentDistribution.show()
    
    // Mapping des codes de paiement
    println("\nLégende des types de paiement :")
    println("  1 = Carte de crédit")
    println("  2 = Espèces")
    println("  3 = Sans frais")
    println("  4 = Litige")
    println("  5 = Inconnu")
    println("  6 = Trajet annulé")
    
    // 9. DISTRIBUTION DES CODES TARIFAIRES
    println("\n" + "=" * 80)
    println("💰 DISTRIBUTION DES CODES TARIFAIRES")
    println("=" * 80)
    
    val rateCodeDistribution = taxiDF.groupBy("RatecodeID")
      .agg(
        count("*").alias("nombre_trajets"),
        (count("*") / totalTrips * 100).alias("pourcentage")
      )
      .orderBy(desc("nombre_trajets"))
    
    rateCodeDistribution.show()
    
    println("\nLégende des codes tarifaires :")
    println("  1 = Tarif standard")
    println("  2 = JFK")
    println("  3 = Newark")
    println("  4 = Nassau ou Westchester")
    println("  5 = Tarif négocié")
    println("  6 = Trajet de groupe")
    
    // 10. SAUVEGARDER UN RÉSUMÉ
    println("\n" + "=" * 80)
    println("💾 SAUVEGARDE DU RÉSUMÉ")
    println("=" * 80)
    
    val summaryPath = "output/phase1_summary"
    
    // Créer un DataFrame de résumé
    val summary = Seq(
      ("total_trips", totalTrips.toString),
      ("zero_distance_trips", zeroDistance.toString),
      ("negative_fare_trips", negativeFare.toString),
      ("zero_passenger_trips", zeroPassengers.toString)
    ).toDF("metric", "value")
    
    summary.write
      .mode("overwrite")
      .option("header", "true")
      .csv(summaryPath)
    
    println(s"✓ Résumé sauvegardé dans : $summaryPath")
    
    // Nettoyer
    taxiDF.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ PHASE 1 TERMINÉE AVEC SUCCÈS")
    println("=" * 80)
    
    spark.stop()
  }
}
