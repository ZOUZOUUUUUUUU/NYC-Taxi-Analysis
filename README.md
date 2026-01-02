# NYC Taxi Analysis - Analyse de Données Massives

![Spark](https://img.shields.io/badge/Apache%20Spark-4.0.1-orange)
![Scala](https://img.shields.io/badge/Scala-2.13.16-red)
![Status](https://img.shields.io/badge/Status-Complete-success)

## 📊 Vue d'ensemble

Projet d'analyse de **2,72 millions de trajets** de taxis jaunes new-yorkais utilisant Apache Spark et Scala. L'objectif principal est d'identifier des opportunités d'optimisation du service via l'analyse de patterns spatio-temporels et le potentiel du covoiturage.

## 🎯 Résultats Clés

- **326 519** opportunités de covoiturage identifiées
- **3,5 millions $** d'économies potentielles par mois
- **15,24%** de réduction possible des trajets
- **60 872 heures** économisées mensuellement
- **89,6%** des départs concentrés à Manhattan

## 🛠️ Technologies Utilisées

- **Apache Spark** 4.0.1 - Traitement distribué
- **Scala** 2.13.16 - Programmation fonctionnelle
- **SBT** 1.9.9 - Build tool
- **Python** 3.x - Visualisations (Matplotlib, Seaborn)
- **Format Parquet** - Stockage optimisé

## 📁 Structure du Projet

```
NYC-Taxi-Analysis/
├── src/main/scala/
│   ├── Phase1_IngestionExploration.scala
│   ├── Phase2_NettoyageTransformation.scala
│   ├── Phase3_AnalyseSpatioTemporelle.scala
│   ├── Phase4_AnalyseModePaiement.scala
│   ├── Phase5_RideSharing.scala
│   └── Extension_AnalyseAvancee.scala
├── data/
│   └── raw/
│       ├── yellow_tripdata_2024-01.parquet
│       └── taxi_zone_lookup.csv
├── visualizations/
│   ├── 1_hourly_distribution.png
│   ├── 2_payment_methods.png
│   ├── 3_ridesharing_opportunities.png
│   ├── 4_trip_categories.png
│   └── 5_dashboard_summary.png
├── visualize_direct.py
├── build.sbt
└── README.md
```

## 🚀 Installation et Exécution

### Prérequis

- Java 17+
- Scala 2.13.16
- SBT 1.9.9
- Apache Spark 4.0.1
- Python 3.x (pour visualisations)

### Installation

1. Cloner le repository
```bash
git clone https://github.com/VOTRE_USERNAME/NYC-Taxi-Analysis.git
cd NYC-Taxi-Analysis
```

2. Télécharger les données
```bash
# Placer yellow_tripdata_2024-01.parquet dans data/raw/
```

3. Compiler le projet
```bash
sbt compile
```

### Exécution des Phases

```bash
# Phase 1: Exploration
sbt "runMain Phase1_IngestionExploration"

# Phase 2: Nettoyage
sbt "runMain Phase2_NettoyageTransformation"

# Phase 3: Analyse Spatio-Temporelle
sbt "runMain Phase3_AnalyseSpatioTemporelle"

# Phase 4: Modes de Paiement
sbt "runMain Phase4_AnalyseModePaiement"

# Phase 5: Covoiturage (Principal)
sbt "runMain Phase5_RideSharing"
```

### Générer les Visualisations

```bash
# Installer dépendances Python
pip install pandas pyarrow matplotlib seaborn

# Générer les graphiques
python visualize_direct.py
```

## 📈 Pipeline d'Analyse

### Phase 1: Ingestion et Exploration
- Chargement de 2,96M trajets
- Détection de 240K anomalies (8,13%)
- Analyse descriptive initiale

### Phase 2: Nettoyage et Transformation
- Filtrage des valeurs aberrantes
- Création de 8 variables enrichies
- Output: 2,72M trajets valides

### Phase 3: Analyse Spatio-Temporelle
- Identification zones à forte activité
- Patterns horaires (pic à 18h: 195K trajets)
- Concentration Manhattan: 89,6%

### Phase 4: Modes de Paiement
- Dominance carte bancaire: 83,5%
- Analyse par distance
- Corrélation pourboires/distance

### Phase 5: Opportunités Covoiturage ⭐
- Algorithme de regroupement (5 min, même origine/destination)
- 326K groupes identifiés
- Calcul économies: 3,5M$/mois
- Recommandations opérationnelles

## 📊 Résultats Détaillés

### Distribution Géographique
- **Manhattan**: 89,6% des départs, 90,2% des arrivées
- **Top 3 zones**: JFK Airport, Upper East Side, Midtown

### Patterns Temporels
- **Heure de pointe**: 18h (195 742 trajets)
- **Jour le plus chargé**: Mercredi (458 379 trajets)
- **Vitesse moyenne**: 16,3 km/h (après-midi) vs 23,9 km/h (nuit)

### Impact Covoiturage
- **Scénario 1** (2 passagers): 359 691 trajets économisés
- **Scénario 2** (3 passagers): 426 621 trajets économisés
- **Réduction CO2**: ~500 tonnes/mois

## 🎓 Méthodologie

1. **Traitement distribué** avec Apache Spark pour gérer 2,7M+ enregistrements
2. **Programmation fonctionnelle** en Scala pour typage fort
3. **Format Parquet** pour compression et performances
4. **Algorithme spatio-temporel** pour identification opportunités
5. **Visualisations** Python pour communication résultats

## 📝 Rapport

Le rapport complet (PDF, 24 pages) inclut:
- Analyse détaillée de chaque phase
- Visualisations professionnelles
- Discussion et limites
- Recommandations stratégiques

## 🤝 Contribution

Les contributions sont bienvenues ! N'hésitez pas à ouvrir une issue ou soumettre une pull request.

## 📄 Licence

Ce projet est fourni à des fins éducatives et d'analyse.

## 👤 Auteur

Zeinab Nechi - Janvier 2026

## 🙏 Remerciements

- NYC Taxi & Limousine Commission pour les données
- Apache Spark community
- Scala community

---

⭐ Si ce projet vous a été utile, n'hésitez pas à lui donner une étoile !
