// ============================================================================
// PHASE 4 : ANALYSE DES MODES DE PAIEMENT
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Phase4_AnalyseModePaiement {
  
  def main(args: Array[String]): Unit = {
    
    // 1. Créer la SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Analysis - Phase 4")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("=" * 80)
    println("PHASE 4 : ANALYSE DES MODES DE PAIEMENT")
    println("=" * 80)
    
    // 2. Charger les données nettoyées
    val cleanedDataPath = "output/cleaned_taxi_data"
    
    println("\n📂 Chargement des données nettoyées...")
    val taxiDF = spark.read.parquet(cleanedDataPath)
    taxiDF.cache()
    
    val totalTrips = taxiDF.count()
    println(s"✓ Nombre de trajets chargés : ${totalTrips}")
    
    // 3. DISTRIBUTION GÉNÉRALE DES MODES DE PAIEMENT
    println("\n" + "=" * 80)
    println("💳 DISTRIBUTION GÉNÉRALE DES MODES DE PAIEMENT")
    println("=" * 80)
    
    println("\n📊 Répartition des modes de paiement :")
    val paymentDistribution = taxiDF
      .groupBy("payment_type", "payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        (count("*") / totalTrips * 100).alias("pourcentage"),
        avg("total_amount").alias("montant_moyen"),
        avg("tip_amount").alias("pourboire_moyen")
      )
      .orderBy(desc("nombre_trajets"))
    
    paymentDistribution.show(false)
    
    // 4. MODES DE PAIEMENT POUR TRAJETS COURTS VS LONGS
    println("\n" + "=" * 80)
    println("🔸🔹 PAIEMENT : TRAJETS COURTS VS LONGS")
    println("=" * 80)
    
    println("\n💳 Modes de paiement pour TRAJETS COURTS (< 10 km) :")
    val shortTripsPayment = taxiDF
      .filter(col("is_short_trip") === true)
      .groupBy("payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        (count("*") / taxiDF.filter(col("is_short_trip") === true).count() * 100).alias("pourcentage"),
        avg("total_amount").alias("montant_moyen"),
        avg("tip_percentage").alias("pourboire_pct_moyen")
      )
      .orderBy(desc("nombre_trajets"))
    
    shortTripsPayment.show(false)
    
    println("\n💳 Modes de paiement pour TRAJETS LONGS (>= 10 km) :")
    val longTripsPayment = taxiDF
      .filter(col("is_short_trip") === false)
      .groupBy("payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        (count("*") / taxiDF.filter(col("is_short_trip") === false).count() * 100).alias("pourcentage"),
        avg("total_amount").alias("montant_moyen"),
        avg("tip_percentage").alias("pourboire_pct_moyen")
      )
      .orderBy(desc("nombre_trajets"))
    
    longTripsPayment.show(false)
    
    // 5. ÉVOLUTION DES MODES DE PAIEMENT DANS LE TEMPS
    println("\n" + "=" * 80)
    println("📈 ÉVOLUTION DES MODES DE PAIEMENT DANS LE TEMPS")
    println("=" * 80)
    
    println("\n📅 Évolution par jour :")
    val paymentByDate = taxiDF
      .groupBy("trip_date", "payment_type_label")
      .agg(count("*").alias("nombre_trajets"))
      .orderBy(col("trip_date"), desc("nombre_trajets"))
    
    // Afficher un échantillon
    paymentByDate.show(30, false)
    
    // Calculer les tendances hebdomadaires
    println("\n📊 Évolution par semaine :")
    val paymentByWeek = taxiDF
      .withColumn("week", weekofyear(col("trip_date")))
      .groupBy("week", "payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("total_amount").alias("montant_moyen")
      )
      .orderBy(col("week"), desc("nombre_trajets"))
    
    paymentByWeek.show(20, false)
    
    // 6. ÉVOLUTION PAR JOUR DE LA SEMAINE
    println("\n" + "=" * 80)
    println("📅 MODES DE PAIEMENT PAR JOUR DE LA SEMAINE")
    println("=" * 80)
    
    println("\n📊 Distribution par jour :")
    taxiDF
      .groupBy("day_name", "payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("total_amount").alias("montant_moyen")
      )
      .orderBy(col("day_of_week"), desc("nombre_trajets"))
      .show(50, false)
    
    // 7. MODES DE PAIEMENT PAR HEURE
    println("\n" + "=" * 80)
    println("⏰ MODES DE PAIEMENT PAR HEURE DE LA JOURNÉE")
    println("=" * 80)
    
    println("\n📊 Distribution par heure (carte vs espèces) :")
    val paymentByHour = taxiDF
      .filter(col("payment_type").isin(1, 2))  // Seulement carte et espèces
      .groupBy("hour", "payment_type_label")
      .agg(count("*").alias("nombre_trajets"))
      .orderBy(col("hour"), desc("nombre_trajets"))
    
    paymentByHour.show(50, false)
    
    // 8. RELATION MONTANT TOTAL ET MODE DE PAIEMENT
    println("\n" + "=" * 80)
    println("💰 RELATION ENTRE MONTANT ET MODE DE PAIEMENT")
    println("=" * 80)
    
    println("\n📊 Statistiques des montants par mode de paiement :")
    taxiDF
      .groupBy("payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        min("total_amount").alias("montant_min"),
        avg("total_amount").alias("montant_moyen"),
        max("total_amount").alias("montant_max"),
        stddev("total_amount").alias("ecart_type")
      )
      .orderBy(desc("montant_moyen"))
      .show(false)
    
    // Distribution par tranches de montant
    println("\n📊 Distribution par tranches de montant :")
    val amountRanges = taxiDF
      .withColumn("tranche_montant",
        when(col("total_amount") < 10, "< 10$")
        .when(col("total_amount") < 20, "10-20$")
        .when(col("total_amount") < 30, "20-30$")
        .when(col("total_amount") < 50, "30-50$")
        .when(col("total_amount") < 100, "50-100$")
        .otherwise("> 100$"))
      .groupBy("tranche_montant", "payment_type_label")
      .agg(count("*").alias("nombre_trajets"))
      .orderBy(col("tranche_montant"), desc("nombre_trajets"))
    
    amountRanges.show(50, false)
    
    // 9. ANALYSE DES POURBOIRES
    println("\n" + "=" * 80)
    println("💵 ANALYSE DES POURBOIRES PAR MODE DE PAIEMENT")
    println("=" * 80)
    
    println("\n📊 Statistiques des pourboires :")
    taxiDF
      .filter(col("payment_type").isin(1, 2))  // Carte et espèces
      .groupBy("payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("tip_amount").alias("pourboire_moyen"),
        avg("tip_percentage").alias("pourboire_pct_moyen"),
        sum(when(col("tip_amount") > 0, 1).otherwise(0)).alias("nombre_avec_pourboire"),
        (sum(when(col("tip_amount") > 0, 1).otherwise(0)) / count("*") * 100).alias("pct_avec_pourboire")
      )
      .show(false)
    
    println("\nℹ️  Note : Les pourboires en espèces ne sont généralement pas enregistrés.")
    
    // 10. DISTRIBUTION DES POURBOIRES (CARTE UNIQUEMENT)
    println("\n📊 Distribution des pourboires pour paiements par CARTE :")
    val cardTips = taxiDF
      .filter(col("payment_type") === 1)  // Carte seulement
      .filter(col("tip_amount") > 0)
      
    cardTips
      .withColumn("tranche_pourboire",
        when(col("tip_percentage") < 10, "< 10%")
        .when(col("tip_percentage") < 15, "10-15%")
        .when(col("tip_percentage") < 20, "15-20%")
        .when(col("tip_percentage") < 25, "20-25%")
        .otherwise("> 25%"))
      .groupBy("tranche_pourboire")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("tip_amount").alias("pourboire_moyen"),
        avg("tip_percentage").alias("pourboire_pct_moyen")
      )
      .orderBy("tranche_pourboire")
      .show(false)
    
    // 11. COMPARAISON WEEKEND VS SEMAINE
    println("\n" + "=" * 80)
    println("📅 MODES DE PAIEMENT : WEEKEND VS SEMAINE")
    println("=" * 80)
    
    println("\n📊 Distribution weekend vs semaine :")
    taxiDF
      .groupBy("is_weekend", "payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("total_amount").alias("montant_moyen"),
        avg("tip_percentage").alias("pourboire_pct_moyen")
      )
      .orderBy(col("is_weekend"), desc("nombre_trajets"))
      .show(false)
    
    // 12. ANALYSE PAR PÉRIODE DE LA JOURNÉE
    println("\n" + "=" * 80)
    println("🌅 MODES DE PAIEMENT PAR PÉRIODE DE LA JOURNÉE")
    println("=" * 80)
    
    println("\n📊 Distribution par période :")
    taxiDF
      .groupBy("time_period", "payment_type_label")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("total_amount").alias("montant_moyen")
      )
      .orderBy(col("time_period"), desc("nombre_trajets"))
      .show(false)
    
    // 13. CORRÉLATION MONTANT - POURBOIRE
    println("\n" + "=" * 80)
    println("📈 CORRÉLATION MONTANT DU TRAJET - POURBOIRE")
    println("=" * 80)
    
    println("\n📊 Pourboires moyens par tranche de tarif (carte uniquement) :")
    taxiDF
      .filter(col("payment_type") === 1)
      .withColumn("tranche_tarif",
        when(col("fare_amount") < 10, "< 10$")
        .when(col("fare_amount") < 20, "10-20$")
        .when(col("fare_amount") < 30, "20-30$")
        .when(col("fare_amount") < 50, "30-50$")
        .otherwise("> 50$"))
      .groupBy("tranche_tarif")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("tip_amount").alias("pourboire_moyen"),
        avg("tip_percentage").alias("pourboire_pct_moyen"),
        avg("fare_amount").alias("tarif_moyen")
      )
      .orderBy("tranche_tarif")
      .show(false)
    
    // 14. SAUVEGARDE DES RÉSULTATS
    println("\n" + "=" * 80)
    println("💾 SAUVEGARDE DES ANALYSES")
    println("=" * 80)
    
    println("\nSauvegarde de la distribution générale...")
    paymentDistribution.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase4_payment_distribution")
    
    println("Sauvegarde de l'évolution temporelle...")
    paymentByDate.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase4_payment_by_date")
    
    println("Sauvegarde de l'analyse horaire...")
    paymentByHour.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase4_payment_by_hour")
    
    println("Sauvegarde de l'analyse par tranches de montant...")
    amountRanges.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/phase4_amount_ranges")
    
    println("✓ Tous les résultats sauvegardés")
    
    // Nettoyer
    taxiDF.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ PHASE 4 TERMINÉE AVEC SUCCÈS")
    println("=" * 80)
    
    spark.stop()
  }
}
