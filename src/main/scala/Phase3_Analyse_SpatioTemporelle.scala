// ============================================================================
// PHASE 3 : ANALYSE SPATIO-TEMPORELLE
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Phase3_AnalyseSpatioTemporelle {
  
  def main(args: Array[String]): Unit = {
    
    // 1. Créer la SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Analysis - Phase 3")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("=" * 80)
    println("PHASE 3 : ANALYSE SPATIO-TEMPORELLE")
    println("=" * 80)
    
    // 2. Charger les données nettoyées
    val cleanedDataPath = "C:/Users/LENOVO/Documents/NYC-Taxi-Analysis/output/cleaned_taxi_data"

    
    println("\n📂 Chargement des données nettoyées...")
    val taxiDF = spark.read.parquet(cleanedDataPath)
    taxiDF.cache()
    
    val totalTrips = taxiDF.count()
    println(s"✓ Nombre de trajets chargés : ${totalTrips}")
    
    // 3. Charger le fichier de zones
    val zoneLookupPath = "C:/Users/LENOVO/Documents/NYC-Taxi-Analysis/data/raw/taxi_zone_lookup.csv"

    
    println("\n📂 Chargement du fichier de zones...")
    val zoneDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zoneLookupPath)
    
    zoneDF.cache()
    println(s"✓ Nombre de zones chargées : ${zoneDF.count()}")
    
    println("\n📋 Aperçu des zones :")
    zoneDF.show(10, false)
    
    // 4. ENRICHISSEMENT AVEC LES NOMS DE ZONES
    println("\n" + "=" * 80)
    println("🗺️  ENRICHISSEMENT AVEC LES NOMS DE ZONES")
    println("=" * 80)
    
    // Jointure pour la zone de départ
    val taxiWithPickupZone = taxiDF
      .join(
        zoneDF.select(
          col("LocationID").alias("PULocationID"),
          col("Borough").alias("pickup_borough"),
          col("Zone").alias("pickup_zone")
        ),
        Seq("PULocationID"),
        "left"
      )
    
    // Jointure pour la zone d'arrivée
    val taxiWithZones = taxiWithPickupZone
      .join(
        zoneDF.select(
          col("LocationID").alias("DOLocationID"),
          col("Borough").alias("dropoff_borough"),
          col("Zone").alias("dropoff_zone")
        ),
        Seq("DOLocationID"),
        "left"
      )
    
    taxiWithZones.cache()
    
    println("✓ Zones intégrées avec succès")
    
    // Vérifier les jointures réussies
    val withZonesCount = taxiWithZones.filter(
      col("pickup_zone").isNotNull && col("dropoff_zone").isNotNull
    ).count()
    
    println(f"✓ Trajets avec zones identifiées : $withZonesCount%,d (${withZonesCount.toDouble / totalTrips * 100}%.2f%%)")
    
    // 5. ANALYSE DES ZONES DE DÉPART
    println("\n" + "=" * 80)
    println("🚕 TOP ZONES DE DÉPART")
    println("=" * 80)
    
    println("\n🏆 Top 20 zones de départ (plus de trajets) :")
    val topPickupZones = taxiWithZones
      .filter(col("pickup_zone").isNotNull)
      .groupBy("pickup_borough", "pickup_zone")
      .agg(
        count("*").alias("nombre_departs"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_distance_km").alias("distance_moyenne_km")
      )
      .orderBy(desc("nombre_departs"))
      .limit(20)
    
    topPickupZones.show(20, false)
    
    // 6. ANALYSE DES ZONES D'ARRIVÉE
    println("\n" + "=" * 80)
    println("🎯 TOP ZONES D'ARRIVÉE")
    println("=" * 80)
    
    println("\n🏆 Top 20 zones d'arrivée (plus de trajets) :")
    val topDropoffZones = taxiWithZones
      .filter(col("dropoff_zone").isNotNull)
      .groupBy("dropoff_borough", "dropoff_zone")
      .agg(
        count("*").alias("nombre_arrivees"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_distance_km").alias("distance_moyenne_km")
      )
      .orderBy(desc("nombre_arrivees"))
      .limit(20)
    
    topDropoffZones.show(20, false)
    
    // 7. ANALYSE PAR BOROUGH
    println("\n" + "=" * 80)
    println("🏙️  ANALYSE PAR BOROUGH (ARRONDISSEMENT)")
    println("=" * 80)
    
    println("\n📊 Statistiques de départ par Borough :")
    taxiWithZones
      .filter(col("pickup_borough").isNotNull)
      .groupBy("pickup_borough")
      .agg(
        count("*").alias("nombre_departs"),
        (count("*") / totalTrips * 100).alias("pourcentage"),
        avg("fare_amount").alias("tarif_moyen")
      )
      .orderBy(desc("nombre_departs"))
      .show(false)
    
    println("\n📊 Statistiques d'arrivée par Borough :")
    taxiWithZones
      .filter(col("dropoff_borough").isNotNull)
      .groupBy("dropoff_borough")
      .agg(
        count("*").alias("nombre_arrivees"),
        (count("*") / totalTrips * 100).alias("pourcentage"),
        avg("fare_amount").alias("tarif_moyen")
      )
      .orderBy(desc("nombre_arrivees"))
      .show(false)
    
    // 8. HEURES DE POINTE
    println("\n" + "=" * 80)
    println("⏰ ANALYSE DES HEURES DE POINTE")
    println("=" * 80)
    
    println("\n📈 Distribution des trajets par heure :")
    val hourlyDistribution = taxiWithZones
      .groupBy("hour")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("average_speed_kmh").alias("vitesse_moyenne_kmh")
      )
      .orderBy("hour")
    
    hourlyDistribution.show(24, false)
    
    // Identifier les heures de pointe
    println("\n🔥 Top 5 heures de pointe :")
    hourlyDistribution
      .orderBy(desc("nombre_trajets"))
      .limit(5)
      .show(false)
    
    // 9. ANALYSE PAR JOUR DE LA SEMAINE
    println("\n" + "=" * 80)
    println("📅 ANALYSE PAR JOUR DE LA SEMAINE")
    println("=" * 80)
    
    println("\n📊 Distribution des trajets par jour :")
    taxiWithZones
      .groupBy("day_name", "day_of_week")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_distance_km").alias("distance_moyenne_km")
      )
      .orderBy("day_of_week")
      .show(false)
    
    // 10. ANALYSE PAR PÉRIODE DE LA JOURNÉE
    println("\n" + "=" * 80)
    println("🌅 ANALYSE PAR PÉRIODE DE LA JOURNÉE")
    println("=" * 80)
    
    println("\n⏱️  Distribution par période :")
    taxiWithZones
      .groupBy("time_period")
      .agg(
        count("*").alias("nombre_trajets"),
        (count("*") / totalTrips * 100).alias("pourcentage"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("average_speed_kmh").alias("vitesse_moyenne_kmh")
      )
      .show(false)
    
    // 11. TOP POINTS DE DÉPART/ARRIVÉE POUR TRAJETS COURTS
    println("\n" + "=" * 80)
    println("🔸 TOP 3 POINTS POUR TRAJETS COURTS (< 10 km)")
    println("=" * 80)
    
    val shortTrips = taxiWithZones.filter(col("is_short_trip") === true)
    
    println("\n📍 Top 3 zones de départ (trajets courts) :")
    shortTrips
      .filter(col("pickup_zone").isNotNull)
      .groupBy("pickup_borough", "pickup_zone")
      .agg(count("*").alias("nombre_departs"))
      .orderBy(desc("nombre_departs"))
      .limit(3)
      .show(false)
    
    println("\n🎯 Top 3 zones d'arrivée (trajets courts) :")
    shortTrips
      .filter(col("dropoff_zone").isNotNull)
      .groupBy("dropoff_borough", "dropoff_zone")
      .agg(count("*").alias("nombre_arrivees"))
      .orderBy(desc("nombre_arrivees"))
      .limit(3)
      .show(false)
    
    // 12. TOP POINTS DE DÉPART/ARRIVÉE POUR TRAJETS LONGS
    println("\n" + "=" * 80)
    println("🔹 TOP 3 POINTS POUR TRAJETS LONGS (>= 10 km)")
    println("=" * 80)
    
    val longTrips = taxiWithZones.filter(col("is_short_trip") === false)
    
    println("\n📍 Top 3 zones de départ (trajets longs) :")
    longTrips
      .filter(col("pickup_zone").isNotNull)
      .groupBy("pickup_borough", "pickup_zone")
      .agg(count("*").alias("nombre_departs"))
      .orderBy(desc("nombre_departs"))
      .limit(3)
      .show(false)
    
    println("\n🎯 Top 3 zones d'arrivée (trajets longs) :")
    longTrips
      .filter(col("dropoff_zone").isNotNull)
      .groupBy("dropoff_borough", "dropoff_zone")
      .agg(count("*").alias("nombre_arrivees"))
      .orderBy(desc("nombre_arrivees"))
      .limit(3)
      .show(false)
    
    // 13. TRAJETS INTER-BOROUGH
    println("\n" + "=" * 80)
    println("🌉 ANALYSE DES TRAJETS INTER-BOROUGH")
    println("=" * 80)
    
    println("\n🔄 Top 10 flux entre boroughs :")
    taxiWithZones
      .filter(
        col("pickup_borough").isNotNull && 
        col("dropoff_borough").isNotNull
      )
      .groupBy("pickup_borough", "dropoff_borough")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_duration_minutes").alias("duree_moyenne_min")
      )
      .orderBy(desc("nombre_trajets"))
      .limit(10)
      .show(false)
    
    // 14. MATRICE HEURE x JOUR
    println("\n" + "=" * 80)
    println("🔥 HEATMAP : HEURES DE POINTE PAR JOUR")
    println("=" * 80)
    
    println("\n📊 Nombre de trajets par heure et jour (aperçu) :")
    val heatmapData = taxiWithZones
      .groupBy("day_name", "hour")
      .agg(count("*").alias("nombre_trajets"))
      .orderBy(col("day_name"), col("hour"))

    
    // Afficher un échantillon
    heatmapData.show(50)
    
    // 15. SAUVEGARDE DES RÉSULTATS
    println("\n" + "=" * 80)
    println("💾 SAUVEGARDE DES ANALYSES")
    println("=" * 80)
    
    // Sauvegarder les tops zones
    println("\nSauvegarde des top zones de départ...")
    topPickupZones.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase3_top_pickup_zones")
    
    println("Sauvegarde des top zones d'arrivée...")
    topDropoffZones.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase3_top_dropoff_zones")
    
    println("Sauvegarde de la distribution horaire...")
    hourlyDistribution.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase3_hourly_distribution")
    
    println("Sauvegarde de la heatmap...")
    heatmapData.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase3_heatmap_data")
    
    println("✓ Tous les résultats sauvegardés")
    
    // Nettoyer
    taxiDF.unpersist()
    zoneDF.unpersist()
    taxiWithZones.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ PHASE 3 TERMINÉE AVEC SUCCÈS")
    println("=" * 80)
    
    spark.stop()
  }
}
