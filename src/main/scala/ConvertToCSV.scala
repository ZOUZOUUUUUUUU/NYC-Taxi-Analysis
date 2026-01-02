import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ConvertToCSV {
  def main(args: Array[String]): Unit = {
    
    // Créer une session Spark
    val spark = SparkSession.builder()
      .appName("Convert to CSV for Visualization")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    import spark.implicits._

    println("================================================================================")
    println("CONVERSION DES RÉSULTATS PARQUET EN CSV")
    println("================================================================================")

    val outputBase = "output"
    val csvBase = "output/csv_for_viz"

    try {
      // 1. Distribution horaire (Phase 3)
      println("\n📊 1. Conversion : Distribution horaire...")
      val hourlyDist = spark.read.parquet(s"$outputBase/phase3_hourly_distribution")
        .orderBy("hour")
      
      hourlyDist.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$csvBase/hourly_distribution")
      println("✅ Distribution horaire → CSV")

      // 2. Top zones de départ (Phase 3)
      println("\n📊 2. Conversion : Top zones de départ...")
      val topPickup = spark.read.parquet(s"$outputBase/phase3_top_pickup_zones")
        .orderBy(desc("nombre_departs"))
        .limit(15)
      
      topPickup.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$csvBase/top_pickup_zones")
      println("✅ Top zones départ → CSV")

      // 3. Modes de paiement (Phase 4)
      println("\n📊 3. Conversion : Modes de paiement...")
      val paymentDist = spark.read.parquet(s"$outputBase/phase4_payment_distribution")
        .orderBy(desc("nombre_trajets"))
      
      paymentDist.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$csvBase/payment_distribution")
      println("✅ Modes de paiement → CSV")

      // 4. Évolution paiements par date (Phase 4)
      println("\n📊 4. Conversion : Évolution paiements...")
      val paymentByDate = spark.read.parquet(s"$outputBase/phase4_payment_by_date")
        .filter(col("trip_date") >= "2024-01-01" && col("trip_date") <= "2024-01-31")
        .orderBy("trip_date")
      
      paymentByDate.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$csvBase/payment_by_date")
      println("✅ Évolution paiements → CSV")

      // 5. Opportunités covoiturage par heure (Phase 5)
      println("\n📊 5. Conversion : Opportunités covoiturage horaire...")
      val ridesharingHourly = spark.read.parquet(s"$outputBase/phase5_hourly_opportunities")
        .orderBy("hour")
      
      ridesharingHourly.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$csvBase/ridesharing_hourly")
      println("✅ Opportunités covoiturage → CSV")

      // 6. Résumé économies (Phase 5)
      println("\n📊 6. Conversion : Résumé économies...")
      val savingsSummary = spark.read.parquet(s"$outputBase/phase5_savings_analysis")
        .orderBy(desc("economie_argent"))
        .limit(20)
      
      savingsSummary.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$csvBase/savings_summary")
      println("✅ Résumé économies → CSV")

      // 7. Échantillon de données nettoyées
      println("\n📊 7. Conversion : Échantillon données...")
      val sampleData = spark.read.parquet(s"$outputBase/cleaned_taxi_data")
        .sample(0.001) // 0.1% des données
        .select(
          "trip_distance_km", "trip_duration_minutes", "average_speed_kmh",
          "hour", "day_name", "fare_amount", "tip_percentage", "payment_type_label",
          "trip_category", "time_period"
        )
      
      sampleData.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"$csvBase/sample_data")
      println("✅ Échantillon données → CSV")

      println("\n" + "=" * 80)
      println("✅ TOUTES LES CONVERSIONS TERMINÉES AVEC SUCCÈS")
      println("=" * 80)
      println(s"📁 Fichiers CSV disponibles dans : $csvBase/")
      println("\nVous pouvez maintenant exécuter le script Python de visualisation.")

    } catch {
      case e: Exception =>
        println(s"\n❌ ERREUR lors de la conversion : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
