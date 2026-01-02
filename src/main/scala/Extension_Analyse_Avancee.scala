// ============================================================================
// EXTENSION : ANALYSE AVANCÉE ET DÉTECTION D'ANOMALIES
// ============================================================================

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.stat.Correlation

object Extension_AnalyseAvancee {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("NYC Taxi - Extension Analyse Avancée")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    
    println("=" * 80)
    println("EXTENSION : ANALYSE AVANCÉE ET DÉTECTION D'ANOMALIES")
    println("=" * 80)
    
    // Charger les données
    val taxiDF = spark.read.parquet("output/cleaned_taxi_data")
    taxiDF.cache()
    
    // ========================================================================
    // 1. TENDANCES TEMPORELLES AVANCÉES
    // ========================================================================
    println("\n" + "=" * 80)
    println("📈 ANALYSE DES TENDANCES TEMPORELLES")
    println("=" * 80)
    
    // Tendances horaires par jour de la semaine
    println("\n📊 Tendances horaires par jour :")
    val hourlyTrends = taxiDF
      .groupBy("day_name", "hour")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_distance_km").alias("distance_moyenne"),
        avg("average_speed_kmh").alias("vitesse_moyenne"),
        avg("tip_percentage").alias("pourboire_pct_moyen")
      )
      .orderBy(col("day_name"), col("hour"))
    
    hourlyTrends.show(50, false)
    
    // Tendances mensuelles (si données multi-mois)
    println("\n📅 Tendances journalières :")
    val dailyTrends = taxiDF
      .groupBy("trip_date")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("fare_amount").alias("tarif_moyen"),
        sum("total_amount").alias("revenus_totaux"),
        avg("passenger_count").alias("passagers_moyen")
      )
      .orderBy("trip_date")
    
    dailyTrends.show(31, false)
    
    // ========================================================================
    // 2. DÉTECTION D'ANOMALIES
    // ========================================================================
    println("\n" + "=" * 80)
    println("🔍 DÉTECTION D'ANOMALIES")
    println("=" * 80)
    
    // Calculer les statistiques pour détecter les outliers
    val stats = taxiDF.select(
      mean("trip_distance_km").alias("mean_dist"),
      stddev("trip_distance_km").alias("std_dist"),
      mean("fare_amount").alias("mean_fare"),
      stddev("fare_amount").alias("std_fare"),
      mean("average_speed_kmh").alias("mean_speed"),
      stddev("average_speed_kmh").alias("std_speed")
    ).collect()(0)
    
    val meanDist = stats.getDouble(0)
    val stdDist = stats.getDouble(1)
    val meanFare = stats.getDouble(2)
    val stdFare = stats.getDouble(3)
    val meanSpeed = stats.getDouble(4)
    val stdSpeed = stats.getDouble(5)
    
    // Détecter les anomalies (valeurs > 3 écarts-types)
    println("\n⚠️  Détection des trajets anormaux (> 3 écarts-types) :")
    val anomalies = taxiDF
      .withColumn("z_score_dist", 
        abs((col("trip_distance_km") - lit(meanDist)) / lit(stdDist)))
      .withColumn("z_score_fare",
        abs((col("fare_amount") - lit(meanFare)) / lit(stdFare)))
      .withColumn("z_score_speed",
        abs((col("average_speed_kmh") - lit(meanSpeed)) / lit(stdSpeed)))
      .withColumn("is_anomaly",
        col("z_score_dist") > 3 || 
        col("z_score_fare") > 3 || 
        col("z_score_speed") > 3)
    
    val anomalyCount = anomalies.filter(col("is_anomaly") === true).count()
    val totalCount = taxiDF.count()
    
    println(f"✓ Anomalies détectées : $anomalyCount%,d (${anomalyCount.toDouble / totalCount * 100}%.2f%%)")
    
    // Afficher quelques exemples d'anomalies
    println("\n🔎 Exemples d'anomalies :")
    anomalies
      .filter(col("is_anomaly") === true)
      .select(
        "tpep_pickup_datetime",
        "trip_distance_km",
        "fare_amount",
        "average_speed_kmh",
        "trip_duration_minutes",
        "z_score_dist",
        "z_score_fare",
        "z_score_speed"
      )
      .orderBy(desc("z_score_dist"))
      .limit(10)
      .show(false)
    
    // Catégoriser les types d'anomalies
    println("\n📊 Types d'anomalies :")
    anomalies
      .filter(col("is_anomaly") === true)
      .withColumn("anomaly_type",
        when(col("z_score_dist") > 3, "Distance excessive")
        .when(col("z_score_fare") > 3, "Tarif anormal")
        .when(col("z_score_speed") > 3, "Vitesse anormale")
        .otherwise("Autre"))
      .groupBy("anomaly_type")
      .agg(
        count("*").alias("nombre"),
        avg("trip_distance_km").alias("distance_moyenne"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("average_speed_kmh").alias("vitesse_moyenne")
      )
      .show(false)
    
    // ========================================================================
    // 3. CATÉGORISATION AVANCÉE
    // ========================================================================
    println("\n" + "=" * 80)
    println("📦 CATÉGORISATION AVANCÉE DES TRAJETS")
    println("=" * 80)
    
    // Catégorisation multidimensionnelle
    val categorized = taxiDF
      .withColumn("trip_profile",
        when(col("trip_distance_km") < 5 && col("trip_duration_minutes") < 15, "Court et rapide")
        .when(col("trip_distance_km") < 5 && col("trip_duration_minutes") >= 15, "Court mais lent")
        .when(col("trip_distance_km") >= 5 && col("average_speed_kmh") > 30, "Long et fluide")
        .when(col("trip_distance_km") >= 5 && col("average_speed_kmh") <= 30, "Long et congestionné")
        .otherwise("Autre"))
      
      .withColumn("revenue_category",
        when(col("fare_amount") < 10, "Faible revenu")
        .when(col("fare_amount") < 25, "Revenu moyen")
        .when(col("fare_amount") < 50, "Bon revenu")
        .otherwise("Excellent revenu"))
      
      .withColumn("tip_category",
        when(col("tip_percentage") < 10, "Faible pourboire")
        .when(col("tip_percentage") < 20, "Pourboire standard")
        .otherwise("Généreux pourboire"))
    
    println("\n📊 Distribution par profil de trajet :")
    categorized
      .groupBy("trip_profile")
      .agg(
        count("*").alias("nombre"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_distance_km").alias("distance_moyenne")
      )
      .orderBy(desc("nombre"))
      .show(false)
    
    println("\n💰 Distribution par catégorie de revenu :")
    categorized
      .groupBy("revenue_category")
      .agg(
        count("*").alias("nombre"),
        sum("fare_amount").alias("revenus_totaux"),
        avg("trip_distance_km").alias("distance_moyenne")
      )
      .show(false)
    
    // ========================================================================
    // 4. ANALYSE DE CORRÉLATION
    // ========================================================================
    println("\n" + "=" * 80)
    println("🔗 ANALYSE DE CORRÉLATION")
    println("=" * 80)
    
    // Préparer les données pour l'analyse de corrélation
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "trip_distance_km",
        "fare_amount",
        "tip_amount",
        "trip_duration_minutes",
        "average_speed_kmh",
        "passenger_count"
      ))
      .setOutputCol("features")
    
    val vectorDF = assembler.transform(
      taxiDF.select(
        "trip_distance_km",
        "fare_amount",
        "tip_amount",
        "trip_duration_minutes",
        "average_speed_kmh",
        "passenger_count"
      ).na.drop()
    )
    
    // Calculer la matrice de corrélation
    val correlationMatrix = Correlation.corr(vectorDF, "features", "pearson")
    
    println("\n📊 Matrice de corrélation :")
    println("Variables : distance, tarif, pourboire, durée, vitesse, passagers")
    correlationMatrix.select("pearson(features)").show(false)
    
    // ========================================================================
    // 5. PATTERNS TEMPORELS COMPLEXES
    // ========================================================================
    println("\n" + "=" * 80)
    println("🌟 PATTERNS TEMPORELS COMPLEXES")
    println("=" * 80)
    
    // Rush hours vs off-peak
    println("\n⏰ Comparaison Rush Hour vs Off-Peak :")
    taxiDF
      .withColumn("is_rush_hour",
        col("hour").isin(7, 8, 9, 17, 18, 19))
      .groupBy("is_rush_hour")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("trip_duration_minutes").alias("duree_moyenne"),
        avg("average_speed_kmh").alias("vitesse_moyenne"),
        sum("total_amount").alias("revenus_totaux")
      )
      .show(false)
    
    // Weekend vs Weekday patterns détaillés
    println("\n📅 Patterns détaillés Weekend vs Weekday :")
    taxiDF
      .groupBy("is_weekend", "time_period")
      .agg(
        count("*").alias("nombre_trajets"),
        avg("fare_amount").alias("tarif_moyen"),
        avg("average_speed_kmh").alias("vitesse_moyenne"),
        avg("tip_percentage").alias("pourboire_pct_moyen")
      )
      .orderBy("is_weekend", "time_period")
      .show(false)
    
    // ========================================================================
    // 6. EFFICACITÉ DES TRAJETS
    // ========================================================================
    println("\n" + "=" * 80)
    println("📊 ANALYSE D'EFFICACITÉ DES TRAJETS")
    println("=" * 80)
    
    val efficiency = taxiDF
      .withColumn("revenue_per_km", col("fare_amount") / col("trip_distance_km"))
      .withColumn("revenue_per_minute", col("fare_amount") / col("trip_duration_minutes"))
      .withColumn("efficiency_score",
        (col("fare_amount") / (col("trip_distance_km") + col("trip_duration_minutes") / 10)))
    
    println("\n💡 Statistiques d'efficacité :")
    efficiency.select(
      "revenue_per_km",
      "revenue_per_minute",
      "efficiency_score"
    ).describe().show()
    
    println("\n🏆 Top 10 trajets les plus rentables :")
    efficiency
      .orderBy(desc("efficiency_score"))
      .select(
        "tpep_pickup_datetime",
        "trip_distance_km",
        "trip_duration_minutes",
        "fare_amount",
        "revenue_per_km",
        "revenue_per_minute",
        "efficiency_score"
      )
      .limit(10)
      .show(false)
    
    // ========================================================================
    // 7. SAUVEGARDE DES RÉSULTATS
    // ========================================================================
    println("\n" + "=" * 80)
    println("💾 SAUVEGARDE DES ANALYSES")
    println("=" * 80)
    
    hourlyTrends.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/extension_hourly_trends")
    
    anomalies.filter(col("is_anomaly") === true).write
      .mode("overwrite")
      .option("header", "true")
      .parquet("output/extension_anomalies")
    
    categorized.write
      .mode("overwrite")
      .option("header", "true")
      .parquet("output/extension_categorized")
    
    efficiency.write
      .mode("overwrite")
      .option("header", "true")
      .parquet("output/extension_efficiency")
    
    println("✓ Tous les résultats sauvegardés")
    
    taxiDF.unpersist()
    
    println("\n" + "=" * 80)
    println("✅ EXTENSION TERMINÉE AVEC SUCCÈS")
    println("=" * 80)
    
    spark.stop()
  }
}
