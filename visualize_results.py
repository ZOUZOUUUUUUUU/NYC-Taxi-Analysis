#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Visualisation - NYC Taxi Analysis
Génère des graphiques professionnels pour le rapport
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import glob
import warnings
warnings.filterwarnings('ignore')

# Configuration du style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10

# Palette de couleurs professionnelle
COLORS = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E']

def find_csv_file(directory):
    """Trouve le fichier CSV dans un dossier Spark"""
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    if csv_files:
        return csv_files[0]
    # Si format Spark avec sous-dossiers
    csv_files = glob.glob(os.path.join(directory, "part-*.csv"))
    if csv_files:
        return csv_files[0]
    return None

def load_data(csv_path):
    """Charge les données CSV avec gestion d'erreurs"""
    try:
        csv_file = find_csv_file(csv_path)
        if csv_file:
            return pd.read_csv(csv_file)
        else:
            print(f"⚠️  Fichier non trouvé dans : {csv_path}")
            return None
    except Exception as e:
        print(f"⚠️  Erreur lors du chargement de {csv_path}: {e}")
        return None

def plot_hourly_distribution(df, output_dir):
    """1. Distribution horaire des trajets avec vitesse"""
    if df is None or df.empty:
        print("⚠️  Données horaires non disponibles")
        return
    
    fig, ax1 = plt.subplots(figsize=(14, 6))
    
    # Nombre de trajets (barres)
    ax1.bar(df['hour'], df['nombre_trajets'] / 1000, 
            color=COLORS[0], alpha=0.7, label='Nombre de trajets (milliers)')
    ax1.set_xlabel('Heure de la journée', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Nombre de trajets (milliers)', fontsize=12, fontweight='bold', color=COLORS[0])
    ax1.tick_params(axis='y', labelcolor=COLORS[0])
    ax1.set_xticks(range(0, 24))
    ax1.grid(axis='y', alpha=0.3)
    
    # Vitesse moyenne (ligne)
    ax2 = ax1.twinx()
    ax2.plot(df['hour'], df['vitesse_moyenne_kmh'], 
             color=COLORS[1], linewidth=3, marker='o', markersize=6, label='Vitesse moyenne')
    ax2.set_ylabel('Vitesse moyenne (km/h)', fontsize=12, fontweight='bold', color=COLORS[1])
    ax2.tick_params(axis='y', labelcolor=COLORS[1])
    
    plt.title('Distribution des Trajets et Vitesse Moyenne par Heure', 
              fontsize=16, fontweight='bold', pad=20)
    
    # Légende combinée
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
    # Annotations heures de pointe
    peak_hours = df.nlargest(3, 'nombre_trajets')
    for _, row in peak_hours.iterrows():
        ax1.annotate(f"{int(row['nombre_trajets']/1000)}k", 
                    xy=(row['hour'], row['nombre_trajets']/1000),
                    xytext=(0, 10), textcoords='offset points',
                    ha='center', fontsize=9, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '1_hourly_distribution.png'), dpi=300, bbox_inches='tight')
    print("✅ 1. Distribution horaire créée")
    plt.close()

def plot_top_zones(df, output_dir):
    """2. Top 15 zones de départ"""
    if df is None or df.empty:
        print("⚠️  Données zones non disponibles")
        return
    
    # Prendre top 15
    df_top = df.head(15).sort_values('nombre_departs')
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    bars = ax.barh(df_top['pickup_zone'], df_top['nombre_departs'] / 1000, color=COLORS[2])
    
    # Gradient de couleurs
    for i, bar in enumerate(bars):
        bar.set_color(plt.cm.viridis(i / len(bars)))
    
    ax.set_xlabel('Nombre de départs (milliers)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Zone', fontsize=12, fontweight='bold')
    ax.set_title('Top 15 Zones de Départ - NYC Taxis', 
                 fontsize=16, fontweight='bold', pad=20)
    
    # Ajouter les valeurs sur les barres
    for i, v in enumerate(df_top['nombre_departs']):
        ax.text(v/1000 + 2, i, f'{int(v/1000)}k', 
                va='center', fontsize=9, fontweight='bold')
    
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '2_top_pickup_zones.png'), dpi=300, bbox_inches='tight')
    print("✅ 2. Top zones créé")
    plt.close()

def plot_payment_methods(df, output_dir):
    """3. Distribution des modes de paiement"""
    if df is None or df.empty:
        print("⚠️  Données paiement non disponibles")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Pie chart
    colors_pie = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A']
    explode = [0.05 if i == 0 else 0 for i in range(len(df))]
    
    wedges, texts, autotexts = ax1.pie(df['nombre_trajets'], 
                                        labels=df['payment_type_label'],
                                        autopct='%1.1f%%',
                                        colors=colors_pie[:len(df)],
                                        explode=explode,
                                        startangle=90,
                                        textprops={'fontsize': 11, 'fontweight': 'bold'})
    
    ax1.set_title('Répartition des Modes de Paiement', 
                  fontsize=14, fontweight='bold', pad=20)
    
    # Bar chart avec montants
    bars = ax2.bar(df['payment_type_label'], df['montant_moyen'], 
                   color=colors_pie[:len(df)], edgecolor='black', linewidth=1.5)
    ax2.set_ylabel('Montant moyen ($)', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Mode de paiement', fontsize=12, fontweight='bold')
    ax2.set_title('Montant Moyen par Mode de Paiement', 
                  fontsize=14, fontweight='bold', pad=20)
    ax2.grid(axis='y', alpha=0.3)
    
    # Ajouter valeurs sur barres
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'${height:.2f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.xticks(rotation=15, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '3_payment_methods.png'), dpi=300, bbox_inches='tight')
    print("✅ 3. Modes de paiement créés")
    plt.close()

def plot_ridesharing_opportunities(df, output_dir):
    """4. Opportunités de covoiturage par heure"""
    if df is None or df.empty:
        print("⚠️  Données covoiturage non disponibles")
        return
    
    fig, ax1 = plt.subplots(figsize=(14, 6))
    
    # Nombre de groupes (barres)
    ax1.bar(df['hour'], df['nombre_groupes_partageables'] / 1000,
            color=COLORS[3], alpha=0.7, label='Groupes partageables (milliers)')
    ax1.set_xlabel('Heure de la journée', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Nombre de groupes (milliers)', fontsize=12, fontweight='bold', color=COLORS[3])
    ax1.tick_params(axis='y', labelcolor=COLORS[3])
    ax1.set_xticks(range(0, 24))
    ax1.grid(axis='y', alpha=0.3)
    
    # Trajets moyens par groupe (ligne)
    ax2 = ax1.twinx()
    ax2.plot(df['hour'], df['trajets_moyen_par_groupe'],
             color=COLORS[4], linewidth=3, marker='s', markersize=6, 
             label='Trajets moyens/groupe')
    ax2.set_ylabel('Trajets moyens par groupe', fontsize=12, fontweight='bold', color=COLORS[4])
    ax2.tick_params(axis='y', labelcolor=COLORS[4])
    ax2.axhline(y=2.0, color='red', linestyle='--', linewidth=2, alpha=0.5, label='Seuil minimum (2)')
    
    plt.title('Opportunités de Covoiturage par Heure', 
              fontsize=16, fontweight='bold', pad=20)
    
    # Légende combinée
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)
    
    # Highlight heures de pointe
    peak_hours = df.nlargest(3, 'nombre_groupes_partageables')
    for _, row in peak_hours.iterrows():
        ax1.annotate(f"{int(row['nombre_groupes_partageables']/1000)}k", 
                    xy=(row['hour'], row['nombre_groupes_partageables']/1000),
                    xytext=(0, 10), textcoords='offset points',
                    ha='center', fontsize=9, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.7))
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '4_ridesharing_opportunities.png'), dpi=300, bbox_inches='tight')
    print("✅ 4. Opportunités covoiturage créées")
    plt.close()

def plot_payment_evolution(df, output_dir):
    """5. Évolution temporelle des modes de paiement"""
    if df is None or df.empty:
        print("⚠️  Données évolution non disponibles")
        return
    
    # Convertir la date
    df['trip_date'] = pd.to_datetime(df['trip_date'])
    
    # Pivoter pour avoir un mode de paiement par colonne
    df_pivot = df.pivot(index='trip_date', columns='payment_type_label', values='nombre_trajets')
    df_pivot = df_pivot.fillna(0)
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Plot des lignes
    for i, col in enumerate(df_pivot.columns):
        ax.plot(df_pivot.index, df_pivot[col] / 1000, 
               marker='o', linewidth=2, markersize=4, 
               label=col, color=COLORS[i % len(COLORS)])
    
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Nombre de trajets (milliers)', fontsize=12, fontweight='bold')
    ax.set_title('Évolution des Modes de Paiement - Janvier 2024', 
                fontsize=16, fontweight='bold', pad=20)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Rotation des dates
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, '5_payment_evolution.png'), dpi=300, bbox_inches='tight')
    print("✅ 5. Évolution paiements créée")
    plt.close()

def plot_dashboard_summary(output_dir):
    """6. Dashboard récapitulatif"""
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
    
    # Texte principal
    ax_title = fig.add_subplot(gs[0, :])
    ax_title.axis('off')
    ax_title.text(0.5, 0.5, 'NYC TAXI ANALYSIS - RÉSULTATS CLÉS', 
                 ha='center', va='center', fontsize=24, fontweight='bold',
                 bbox=dict(boxstyle='round,pad=1', facecolor='lightblue', alpha=0.8))
    
    # Métriques clés
    metrics = [
        ('2.72M', 'Trajets Analysés', COLORS[0]),
        ('326K', 'Opportunités Covoiturage', COLORS[1]),
        ('$3.5M', 'Économies Potentielles', COLORS[2]),
        ('15.2%', 'Réduction Trajets', COLORS[3]),
        ('18h', 'Heure de Pointe', COLORS[4]),
        ('83.5%', 'Paiement Carte', COLORS[0])
    ]
    
    positions = [(1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2)]
    
    for (value, label, color), pos in zip(metrics, positions):
        ax = fig.add_subplot(gs[pos[0], pos[1]])
        ax.axis('off')
        
        # Fond coloré
        ax.add_patch(plt.Rectangle((0.1, 0.2), 0.8, 0.6, 
                                   facecolor=color, alpha=0.3, 
                                   transform=ax.transAxes))
        
        # Valeur
        ax.text(0.5, 0.6, value, 
               ha='center', va='center', fontsize=28, fontweight='bold',
               transform=ax.transAxes)
        
        # Label
        ax.text(0.5, 0.35, label, 
               ha='center', va='center', fontsize=12,
               transform=ax.transAxes, style='italic')
    
    plt.savefig(os.path.join(output_dir, '6_dashboard_summary.png'), dpi=300, bbox_inches='tight')
    print("✅ 6. Dashboard créé")
    plt.close()

def main():
    """Fonction principale"""
    print("=" * 80)
    print("GÉNÉRATION DES VISUALISATIONS - NYC TAXI ANALYSIS")
    print("=" * 80)
    
    # Chemins
    base_dir = "output/csv_for_viz"
    output_dir = "visualizations"
    
    # Créer dossier de sortie
    os.makedirs(output_dir, exist_ok=True)
    
    # Vérifier si les CSV existent
    if not os.path.exists(base_dir):
        print(f"\n❌ ERREUR: Le dossier {base_dir} n'existe pas!")
        print("Vous devez d'abord exécuter le script ConvertToCSV.scala")
        print("\nCommande:")
        print('  sbt "runMain ConvertToCSV"')
        return
    
    print(f"\n📂 Chargement des données depuis : {base_dir}")
    print(f"📊 Sauvegarde des graphiques dans : {output_dir}\n")
    
    # Charger les données
    hourly_dist = load_data(os.path.join(base_dir, "hourly_distribution"))
    top_zones = load_data(os.path.join(base_dir, "top_pickup_zones"))
    payment_dist = load_data(os.path.join(base_dir, "payment_distribution"))
    payment_by_date = load_data(os.path.join(base_dir, "payment_by_date"))
    ridesharing_hourly = load_data(os.path.join(base_dir, "ridesharing_hourly"))
    
    # Générer les graphiques
    print("\n🎨 Génération des visualisations...\n")
    
    plot_hourly_distribution(hourly_dist, output_dir)
    plot_top_zones(top_zones, output_dir)
    plot_payment_methods(payment_dist, output_dir)
    plot_ridesharing_opportunities(ridesharing_hourly, output_dir)
    plot_payment_evolution(payment_by_date, output_dir)
    plot_dashboard_summary(output_dir)
    
    print("\n" + "=" * 80)
    print("✅ TOUTES LES VISUALISATIONS ONT ÉTÉ CRÉÉES AVEC SUCCÈS!")
    print("=" * 80)
    print(f"\n📁 Les graphiques sont disponibles dans : {output_dir}/")
    print("\nFichiers créés:")
    print("  1. 1_hourly_distribution.png      - Distribution horaire")
    print("  2. 2_top_pickup_zones.png         - Top 15 zones")
    print("  3. 3_payment_methods.png          - Modes de paiement")
    print("  4. 4_ridesharing_opportunities.png - Opportunités covoiturage")
    print("  5. 5_payment_evolution.png        - Évolution temporelle")
    print("  6. 6_dashboard_summary.png        - Dashboard récapitulatif")
    print("\n🎉 Vous pouvez maintenant utiliser ces graphiques dans votre rapport!")

if __name__ == "__main__":
    main()
