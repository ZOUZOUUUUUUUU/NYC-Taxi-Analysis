// ============================================================================
// PHASE 2 : NETTOYAGE ET TRANSFORMATION DES DONNÉES
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Phase2_NettoyageTransformation {
  
  def main(args: Array[String]): Unit = {
    
    // 1. Créer la SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Analysis - Phase 2")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("=" * 80)
    println("PHASE 2 : NETTOYAGE ET TRANSFORMATION DES DONNÉES")
    println("=" * 80)
    
    // 2. Charger les données
    val taxiDataPath = "C:/Users/LENOVO/Documents/NYC-Taxi-Analysis/data/raw/yellow_tripdata_2024-01.parquet"
    
    println("\n📂 Chargement des données...")
    var taxiDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .parquet(taxiDataPath)
    
    val totalTripsInitial = taxiDF.count()
    println(s"✓ Nombre de trajets initial : ${totalTripsInitial}")
    
    // 3. NETTOYAGE DES DONNÉES
    println("\n" + "=" * 80)
    println("🧹 NETTOYAGE DES DONNÉES")
    println("=" * 80)
    
    // Supprimer les lignes avec des valeurs aberrantes
    println("\nSuppression des trajets invalides...")
    
    val cleanedDF = taxiDF.filter(
      col("trip_distance") > 0 &&                    // Distance positive
      col("trip_distance") < 200 &&                  // Distance raisonnable (< 200 miles)
      col("fare_amount") > 0 &&                      // Tarif positif
      col("fare_amount") < 500 &&                    // Tarif raisonnable
      col("total_amount") > 0 &&                     // Montant total positif
      col("total_amount") < 600 &&                   // Montant total raisonnable
      col("passenger_count") > 0 &&                  // Au moins 1 passager
      col("passenger_count") <= 6 &&                 // Maximum 6 passagers
      col("tpep_pickup_datetime").isNotNull &&       // Date de départ non nulle
      col("tpep_dropoff_datetime").isNotNull &&      // Date d'arrivée non nulle
      col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")  // Arrivée après départ
    )
    
    val totalTripsAfterCleaning = cleanedDF.count()
    val removedTrips = totalTripsInitial - totalTripsAfterCleaning
    val percentageRemoved = (removedTrips.toDouble / totalTripsInitial * 100)
    
    println(s"✓ Trajets après nettoyage : ${totalTripsAfterCleaning}")
    println(f"✓ Trajets supprimés : $removedTrips%,d ($percentageRemoved%.2f%%)")
    
    // 4. TRANSFORMATION DES DONNÉES
    println("\n" + "=" * 80)
    println("⚙️  TRANSFORMATION ET ENRICHISSEMENT")
    println("=" * 80)
    
    println("\nCréation des colonnes dérivées...")
    
    val enrichedDF = cleanedDF
      // Calcul de la durée du trajet en minutes
      .withColumn("trip_duration_minutes", 
        (unix_timestamp(col("tpep_dropoff_datetime")) - 
         unix_timestamp(col("tpep_pickup_datetime"))) / 60)
      
      // Calcul de la vitesse moyenne en km/h (1 mile = 1.60934 km)
      .withColumn("trip_distance_km", col("trip_distance") * 1.60934)
      .withColumn("average_speed_kmh", 
        when(col("trip_duration_minutes") > 0,
          (col("trip_distance_km") / col("trip_duration_minutes")) * 60)
        .otherwise(0))
      
      // Extraction de l'heure de la journée
      .withColumn("hour", hour(col("tpep_pickup_datetime")))
      
      // Extraction du jour de la semaine (1 = Lundi, 7 = Dimanche)
      .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
      .withColumn("day_name", 
        when(col("day_of_week") === 1, "Dimanche")
        .when(col("day_of_week") === 2, "Lundi")
        .when(col("day_of_week") === 3, "Mardi")
        .when(col("day_of_week") === 4, "Mercredi")
        .when(col("day_of_week") === 5, "Jeudi")
        .when(col("day_of_week") === 6, "Vendredi")
        .otherwise("Samedi"))
      
      // Extraction de la date
      .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
      
      // Période de la journée
      .withColumn("time_period",
        when(col("hour").between(6, 11), "Matin (6h-11h)")
        .when(col("hour").between(12, 17), "Après-midi (12h-17h)")
        .when(col("hour").between(18, 22), "Soirée (18h-22h)")
        .otherwise("Nuit (23h-5h)"))
      
      // Calcul du pourcentage de pourboire
      .withColumn("tip_percentage",
        when(col("fare_amount") > 0, 
          (col("tip_amount") / col("fare_amount")) * 100)
        .otherwise(0))
      
      // Catégorisation des trajets par distance (en km)
      .withColumn("trip_category",
        when(col("trip_distance_km") < 3, "Très court (< 3 km)")
        .when(col("trip_distance_km") < 10, "Court (3-10 km)")
        .when(col("trip_distance_km") < 20, "Moyen (10-20 km)")
        .when(col("trip_distance_km") < 50, "Long (20-50 km)")
        .otherwise("Très long (> 50 km)"))
      
      // Catégorisation binaire pour l'analyse
      .withColumn("is_short_trip", 
        when(col("trip_distance_km") < 10, true).otherwise(false))
      
      // Ajout du type de paiement en texte
      .withColumn("payment_type_label",
        when(col("payment_type") === 1, "Carte de crédit")
        .when(col("payment_type") === 2, "Espèces")
        .when(col("payment_type") === 3, "Sans frais")
        .when(col("payment_type") === 4, "Litige")
        .when(col("payment_type") === 5, "Inconnu")
        .otherwise("Annulé"))
      
      // Indicateur de weekend
      .withColumn("is_weekend",
        when(col("day_of_week").isin(1, 7), true).otherwise(false))
    
    // Filtrer les vitesses aberrantes (> 200 km/h ou < 0)
    val finalDF = enrichedDF.filter(
      col("average_speed_kmh") >= 0 && 
      col("average_speed_kmh") <= 200 &&
      col("trip_duration_minutes") > 0 &&
      col("trip_duration_minutes") < 300  // Moins de 5 heures
    )
    
    val totalTripsFinal = finalDF.count()
    println(s"✓ Trajets après enrichissement : ${totalTripsFinal}")
    
    // Mettre en cache pour les analyses
    finalDF.cache()
    
    // 5. ANALYSE DES DISTRIBUTIONS
    println("\n" + "=" * 80)
    println("📊 ANALYSE DES DISTRIBUTIONS")
    println("=" * 80)
    
    // Distribution des distances
    println("\n📏 Distribution des distances (km) :")
    finalDF.select("trip_distance_km")
      .describe()
      .show()
    
    // Distribution des durées
    println("\n⏱️  Distribution des durées (minutes) :")
    finalDF.select("trip_duration_minutes")
      .describe()
      .show()
    
    // Distribution des catégories de trajets
    println("\n📦 Distribution des catégories de trajets :")
    finalDF.groupBy("trip_category")
      .agg(
        count("*").alias("nombre"),
        (count("*") / totalTripsFinal * 100).alias("pourcentage")
      )
      .orderBy(desc("nombre"))
      .show(false)
    
    // Proportion trajets courts vs longs
    println("\n🔄 Proportion trajets courts (< 10 km) vs longs (>= 10 km) :")
    finalDF.groupBy("is_short_trip")
      .agg(
        count("*").alias("nombre"),
        (count("*") / totalTripsFinal * 100).alias("pourcentage")
      )
      .show()
    
    // 6. ANALYSE DES VITESSES MOYENNES
    println("\n" + "=" * 80)
    println("🚗 ANALYSE DES VITESSES MOYENNES")
    println("=" * 80)
    
    // Vitesse moyenne par heure
    println("\n⏰ Vitesse moyenne par heure de la journée :")
    finalDF.groupBy("hour")
      .agg(
        avg("average_speed_kmh").alias("vitesse_moyenne_kmh"),
        count("*").alias("nombre_trajets")
      )
      .orderBy("hour")
      .show(24, false)
    
    // Vitesse moyenne par jour de la semaine
    println("\n📅 Vitesse moyenne par jour de la semaine :")
    finalDF.groupBy("day_name", "day_of_week")
      .agg(
        avg("average_speed_kmh").alias("vitesse_moyenne_kmh"),
        count("*").alias("nombre_trajets")
      )
      .orderBy("day_of_week")
      .show(false)
    
    // Vitesse moyenne par période de la journée
    println("\n🌅 Vitesse moyenne par période de la journée :")
    finalDF.groupBy("time_period")
      .agg(
        avg("average_speed_kmh").alias("vitesse_moyenne_kmh"),
        count("*").alias("nombre_trajets")
      )
      .orderBy(desc("vitesse_moyenne_kmh"))
      .show(false)
    
    // Vitesse moyenne weekend vs semaine
    println("\n📊 Vitesse moyenne : Weekend vs Semaine :")
    finalDF.groupBy("is_weekend")
      .agg(
        avg("average_speed_kmh").alias("vitesse_moyenne_kmh"),
        count("*").alias("nombre_trajets")
      )
      .show()
    
    // 7. ANALYSE DES TARIFS
    println("\n" + "=" * 80)
    println("💰 ANALYSE DES TARIFS")
    println("=" * 80)
    
    println("\n💵 Statistiques des tarifs par catégorie de trajet :")
    finalDF.groupBy("trip_category")
      .agg(
        avg("fare_amount").alias("tarif_moyen"),
        avg("total_amount").alias("montant_total_moyen"),
        avg("tip_percentage").alias("pourboire_moyen_pct"),
        count("*").alias("nombre_trajets")
      )
      .show(false)
    
    // 8. SAUVEGARDE DES DONNÉES NETTOYÉES
    println("\n" + "=" * 80)
    println("💾 SAUVEGARDE DES DONNÉES")
    println("=" * 80)
    
    val outputPath = "output/cleaned_taxi_data"
    
    println(s"\nSauvegarde des données nettoyées dans : $outputPath")
    finalDF.write
      .mode("overwrite")
      .partitionBy("trip_date")  // Partitionner par date
      .parquet(outputPath)
    
    println("✓ Données sauvegardées avec succès")
    
    // Sauvegarder un échantillon CSV pour visualisation
    val samplePath = "output/cleaned_taxi_sample.csv"
    println(s"\nSauvegarde d'un échantillon CSV dans : $samplePath")
    
    finalDF.sample(0.01)  // 1% des données
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(samplePath)
    
    println("✓ Échantillon CSV sauvegardé")
    
    // Afficher un aperçu des colonnes créées
    println("\n" + "=" * 80)
    println("📋 APERÇU DES DONNÉES ENRICHIES")
    println("=" * 80)
    
    finalDF.select(
      "tpep_pickup_datetime",
      "trip_distance_km",
      "trip_duration_minutes",
      "average_speed_kmh",
      "hour",
      "day_name",
      "time_period",
      "trip_category",
      "fare_amount",
      "tip_percentage"
    ).show(10, false)
    
    // Nettoyer
    finalDF.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ PHASE 2 TERMINÉE AVEC SUCCÈS")
    println("=" * 80)
    
    spark.stop()
  }
}
