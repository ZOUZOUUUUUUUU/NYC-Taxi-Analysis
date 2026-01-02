// ============================================================================
// PHASE 5 : EXPLORATION DU COVOITURAGE (RIDE-SHARING)
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object Phase5_RideSharing {
  
  def main(args: Array[String]): Unit = {
    
    // 1. Créer la SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Analysis - Phase 5 - Ride Sharing")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("=" * 80)
    println("PHASE 5 : EXPLORATION DU COVOITURAGE (RIDE-SHARING)")
    println("=" * 80)
    
    // 2. Charger les données nettoyées
    val cleanedDataPath = "C:/Users/LENOVO/Documents/NYC-Taxi-Analysis/output/cleaned_taxi_data"
    
    println("\n📂 Chargement des données nettoyées...")
    val taxiDF = spark.read.parquet(cleanedDataPath)
    
    val totalTrips = taxiDF.count()
    println(s"✓ Nombre de trajets chargés : ${totalTrips}")
    
    // 3. FILTRER LES TRAJETS COURTS
    println("\n" + "=" * 80)
    println("🔸 FILTRAGE DES TRAJETS COURTS POUR COVOITURAGE")
    println("=" * 80)
    
    // Critères pour le covoiturage : trajets courts uniquement
    val shortTrips = taxiDF
      .filter(
        col("is_short_trip") === true &&
        col("PULocationID").isNotNull &&
        col("DOLocationID").isNotNull
      )
      .select(
        col("tpep_pickup_datetime"),
        col("PULocationID"),
        col("DOLocationID"),
        col("trip_distance_km"),
        col("fare_amount"),
        col("trip_duration_minutes"),
        col("passenger_count")
      )
    
    shortTrips.cache()
    
    val shortTripsCount = shortTrips.count()
    println(s"✓ Nombre de trajets courts (< 10 km) : ${shortTripsCount}")
    println(f"✓ Proportion : ${shortTripsCount.toDouble / totalTrips * 100}%.2f%% des trajets")
    
    // 4. DÉFINIR LES FENÊTRES TEMPORELLES
    println("\n" + "=" * 80)
    println("⏰ REGROUPEMENT PAR FENÊTRES TEMPORELLES")
    println("=" * 80)
    
    // Créer des fenêtres de 5 minutes
    val timeWindowMinutes = 5
    
    println(s"\n🕐 Création de fenêtres temporelles de ${timeWindowMinutes} minutes...")
    
    val tripsWithTimeWindow = shortTrips
      .withColumn("time_window",
        (unix_timestamp(col("tpep_pickup_datetime")) / (timeWindowMinutes * 60)).cast(LongType))
      .withColumn("window_start",
        from_unixtime(col("time_window") * (timeWindowMinutes * 60)))
    
    // 5. REGROUPER LES TRAJETS PROCHES DANS LE TEMPS ET L'ESPACE
    println("\n" + "=" * 80)
    println("🗺️  IDENTIFICATION DES OPPORTUNITÉS DE COVOITURAGE")
    println("=" * 80)
    
    println("\n🔍 Recherche de trajets partageables...")
    
    // Regrouper par fenêtre temporelle et zone de départ
    val potentialSharedTrips = tripsWithTimeWindow
      .groupBy("time_window", "PULocationID", "DOLocationID")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("trip_distance_km").alias("distance_moyenne"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_duration_minutes").alias("duree_moyenne"),
        sum("passenger_count").alias("total_passagers"),
        min("tpep_pickup_datetime").alias("premier_pickup"),
        max("tpep_pickup_datetime").alias("dernier_pickup")
      )
      .filter(col("nombre_trajets") >= 2)  // Au moins 2 trajets
      .withColumn("ecart_temps_minutes",
        (unix_timestamp(col("dernier_pickup")) - 
         unix_timestamp(col("premier_pickup"))) / 60)
    
    potentialSharedTrips.cache()
    
    val shareableGroupsCount = potentialSharedTrips.count()
    println(s"✓ Groupes de trajets partageables identifiés : ${shareableGroupsCount}")
    
    // 6. STATISTIQUES DES OPPORTUNITÉS
    println("\n📊 Statistiques des opportunités de covoiturage :")
    potentialSharedTrips
      .select("nombre_trajets", "distance_moyenne", "tarif_moyen", 
              "duree_moyenne", "ecart_temps_minutes")
      .describe()
      .show()
    
    // 7. TOP OPPORTUNITÉS DE COVOITURAGE
    println("\n" + "=" * 80)
    println("🏆 TOP 20 OPPORTUNITÉS DE COVOITURAGE")
    println("=" * 80)
    
    println("\n🔝 Groupes avec le plus de trajets :")
    val topOpportunities = potentialSharedTrips
      .orderBy(desc("nombre_trajets"))
      .limit(20)
    
    topOpportunities.show(false)
    
    // 8. ANALYSE PAR ZONE
    println("\n" + "=" * 80)
    println("🗺️  ZONES AVEC LE PLUS D'OPPORTUNITÉS")
    println("=" * 80)
    
    println("\n📍 Top zones de départ pour covoiturage :")
    val topPickupZones = potentialSharedTrips
      .groupBy("PULocationID")
      .agg(
        sum("nombre_trajets").alias("total_trajets_partageables"),
        count("*").alias("nombre_groupes"),
        avg("nombre_trajets").alias("trajets_par_groupe")
      )
      .orderBy(desc("total_trajets_partageables"))
      .limit(10)
    
    topPickupZones.show(false)
    
    // 9. CALCUL DES ÉCONOMIES POTENTIELLES
    println("\n" + "=" * 80)
    println("💰 CALCUL DES ÉCONOMIES POTENTIELLES")
    println("=" * 80)
    
    val savings = potentialSharedTrips
      .withColumn("trajets_individuels", col("nombre_trajets"))
      .withColumn("trajets_partages_estimes", 
        ceil(col("nombre_trajets") / 2).cast(IntegerType))  // Supposons 2 passagers par taxi
      .withColumn("trajets_economises", 
        col("trajets_individuels") - col("trajets_partages_estimes"))
      .withColumn("cout_total_actuel", 
        col("nombre_trajets") * col("tarif_moyen"))
      .withColumn("cout_partage_estime",
        col("trajets_partages_estimes") * col("tarif_moyen") * 1.1)  // +10% pour détours
      .withColumn("economie_argent",
        col("cout_total_actuel") - col("cout_partage_estime"))
      .withColumn("economie_pct",
        (col("economie_argent") / col("cout_total_actuel")) * 100)
      .withColumn("temps_economise_total",
        col("trajets_economises") * col("duree_moyenne"))
    
    savings.cache()
    
    // Statistiques d'économie globales
    val totalSavings = savings.agg(
      sum("trajets_economises").alias("total_trajets_economises"),
      sum("economie_argent").alias("total_economie_argent"),
      sum("temps_economise_total").alias("total_temps_economise_minutes")
    ).collect()(0)
    
    val tripsReduced = totalSavings.getLong(0)
    val moneySaved = totalSavings.getDouble(1)
    val timeSaved = totalSavings.getDouble(2)
    
    println("\n💡 RÉSUMÉ DES ÉCONOMIES POTENTIELLES :")
    println("=" * 80)
    println(f"📉 Trajets économisés : $tripsReduced%,d")
    println(f"💵 Économie d'argent : $$${moneySaved}%,.2f")
    println(f"⏱️  Temps économisé : ${timeSaved}%,.0f minutes (${timeSaved / 60}%,.1f heures)")
    println(f"🌱 Réduction des trajets : ${tripsReduced.toDouble / shortTripsCount * 100}%.2f%%")
    
    // 10. TOP ÉCONOMIES
    println("\n🏆 Top 10 groupes avec les plus grandes économies :")
    savings
      .select(
        "PULocationID",
        "DOLocationID",
        "nombre_trajets",
        "trajets_partages_estimes",
        "trajets_economises",
        "economie_argent",
        "economie_pct",
        "temps_economise_total"
      )
      .orderBy(desc("economie_argent"))
      .limit(10)
      .show(false)
    
    // 11. ANALYSE PAR HEURE DE LA JOURNÉE
    println("\n" + "=" * 80)
    println("⏰ OPPORTUNITÉS DE COVOITURAGE PAR HEURE")
    println("=" * 80)
    
    val opportunitiesByHour = tripsWithTimeWindow
      .withColumn("hour", hour(col("tpep_pickup_datetime")))
      .groupBy("hour", "time_window", "PULocationID", "DOLocationID")
      .agg(count("*").alias("nombre_trajets"))
      .filter(col("nombre_trajets") >= 2)
      .groupBy("hour")
      .agg(
        count("*").alias("nombre_groupes_partageables"),
        sum("nombre_trajets").alias("total_trajets_partageables"),
        avg("nombre_trajets").alias("trajets_moyen_par_groupe")
      )
      .orderBy("hour")
    
    println("\n📊 Distribution horaire des opportunités :")
    opportunitiesByHour.show(24, false)
    
    // 12. ANALYSE PAR JOUR DE LA SEMAINE
    println("\n" + "=" * 80)
    println("📅 OPPORTUNITÉS DE COVOITURAGE PAR JOUR")
    println("=" * 80)
    
    val opportunitiesByDay = tripsWithTimeWindow
      .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
      .groupBy("day_of_week", "time_window", "PULocationID", "DOLocationID")
      .agg(count("*").alias("nombre_trajets"))
      .filter(col("nombre_trajets") >= 2)
      .groupBy("day_of_week")
      .agg(
        count("*").alias("nombre_groupes_partageables"),
        sum("nombre_trajets").alias("total_trajets_partageables")
      )
      .orderBy("day_of_week")
    
    println("\n📊 Distribution par jour de la semaine :")
    opportunitiesByDay.show(false)
    
    // 13. SCÉNARIOS D'OPTIMISATION
    println("\n" + "=" * 80)
    println("🎯 SCÉNARIOS D'OPTIMISATION")
    println("=" * 80)
    
    // Scénario 1 : Covoiturage avec 2 passagers
    println("\n📊 Scénario 1 : Regroupement par 2 passagers")
    val scenario1 = calculateScenario(savings, 2)
    scenario1.show(false)
    
    // Scénario 2 : Covoiturage avec 3 passagers
    println("\n📊 Scénario 2 : Regroupement par 3 passagers")
    val scenario2 = calculateScenario(savings, 3)
    scenario2.show(false)
    
    // 14. RECOMMANDATIONS
    println("\n" + "=" * 80)
    println("📝 RECOMMANDATIONS")
    println("=" * 80)
    
    println("\n✅ Recommandations pour la mise en œuvre du covoiturage :")
    println("  1. Cibler les zones avec forte densité de trajets courts")
    println("  2. Concentrer les efforts sur les heures de pointe")
    println("  3. Implémenter une fenêtre temporelle de 5 minutes maximum")
    println("  4. Privilégier les trajets avec même origine et destination")
    println("  5. Offrir une réduction de 20-30% pour encourager le partage")
    
    // 15. SAUVEGARDE DES RÉSULTATS
    println("\n" + "=" * 80)
    println("💾 SAUVEGARDE DES ANALYSES")
    println("=" * 80)
    
    println("\nSauvegarde des opportunités de covoiturage...")
    potentialSharedTrips.write
      .mode("overwrite")
      .option("header", "true")
      .parquet("output/phase5_ridesharing_opportunities")
    
    println("Sauvegarde des top opportunités...")
    topOpportunities.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase5_top_opportunities")
    
    println("Sauvegarde des calculs d'économies...")
    savings.write
      .mode("overwrite")
      .option("header", "true")
      .parquet("output/phase5_savings_analysis")
    
    println("Sauvegarde de l'analyse horaire...")
    opportunitiesByHour.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase5_hourly_opportunities")
    
    println("✓ Tous les résultats sauvegardés")
    
    // Nettoyer
    shortTrips.unpersist()
    potentialSharedTrips.unpersist()
    savings.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ PHASE 5 TERMINÉE AVEC SUCCÈS")
    println("=" * 80)
    
    spark.stop()
  }
  
  // Fonction pour calculer différents scénarios
  def calculateScenario(savingsDF: DataFrame, passengersPerTrip: Int): DataFrame = {
    import savingsDF.sparkSession.implicits._
    
    savingsDF
      .withColumn(s"trajets_partages_${passengersPerTrip}p",
        ceil(col("nombre_trajets") / passengersPerTrip).cast(IntegerType))
      .withColumn(s"trajets_economises_${passengersPerTrip}p",
        col("nombre_trajets") - col(s"trajets_partages_${passengersPerTrip}p"))
      .withColumn(s"economie_${passengersPerTrip}p",
        col(s"trajets_economises_${passengersPerTrip}p") * col("tarif_moyen"))
      .agg(
        sum(s"trajets_economises_${passengersPerTrip}p").alias("trajets_economises"),
        sum(s"economie_${passengersPerTrip}p").alias("economie_totale"),
        avg(s"trajets_economises_${passengersPerTrip}p").alias("trajets_economises_moyen")
      )
      .withColumn("scenario", lit(s"${passengersPerTrip} passagers/taxi"))
  }
}
