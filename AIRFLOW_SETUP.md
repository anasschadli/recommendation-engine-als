# ✈️ Guide Démarrage Apache Airflow

## 📋 Prérequis

### 1. Installer Apache Airflow

```powershell
# Depuis la racine du projet
pip install apache-airflow==2.7.3
pip install apache-airflow-providers-apache-spark
pip install requests
```

### 2. Vérifier l'installation

```powershell
airflow version
```

Doit afficher : `2.7.3` ou proche

---

## 🚀 Démarrage d'Airflow

### Étape 1: Initialiser la base de données Airflow

```powershell
# Une seule fois!
airflow db init
```

Cela crée les tables SQLite nécessaires.

### Étape 2: Créer un utilisateur admin

```powershell
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

À la demande du mot de passe, entrer : `admin` (ou votre choix)

### Étape 3: Démarrer le webserver

```powershell
# Terminal 1 - Webserver (interface web)
airflow webserver --port 8080
```

Accéder à : **http://localhost:8080**

### Étape 4: Démarrer le scheduler (dans un autre terminal)

```powershell
# Terminal 2 - Scheduler (orchestre les tâches)
airflow scheduler
```

---

## 📊 Accéder à l'interface Airflow

1. Ouvrir navigateur : **http://localhost:8080**
2. Se connecter :
   - **Username**: `admin`
   - **Password**: `admin` (ou ce que vous avez entré)
3. Vous verrez le DAG `recommendation_pipeline`

---

## 🎯 Utiliser le DAG

### Activer le DAG

1. Dans l'interface Airflow
2. Chercher `recommendation_pipeline`
3. Cliquer le **toggle** (bouton on/off) pour l'activer
4. Le DAG s'exécutera automatiquement chaque jour à minuit

### Déclencher manuellement

#### Option 1: Via l'interface web

1. Aller sur le DAG `recommendation_pipeline`
2. Cliquer le bouton **"Trigger DAG"** (triangulation play)
3. Cliquer **"Trigger"**

#### Option 2: Via CLI

```powershell
airflow dags trigger recommendation_pipeline
```

### Voir l'exécution

1. DAG Overview → Voir le statut
2. Graph View → Voir le pipeline et ses tâches
3. Tree View → Historique des exécutions
4. Logs → Cliquer sur une tâche pour voir les logs

---

## 📂 Structure Airflow

Le dossier Airflow contient :

```
airflow/
├── dags/
│   └── recommendation_pipeline.py  (Notre DAG)
├── logs/
│   └── (Logs des exécutions)
└── airflow.db
    └── (Base de données SQLite)
```

---

## 🔧 Configuration avancée

### Changer la fréquence d'exécution

Dans `airflow/dags/recommendation_pipeline.py`, ligne `schedule_interval`:

```python
# Tous les jours à minuit
schedule_interval='@daily'

# Toutes les heures
schedule_interval='0 * * * *'

# Chaque lundi à 9h
schedule_interval='0 9 * * 1'

# Désactiver la planification (manuel uniquement)
schedule_interval=None
```

### Variables d'environnement

Airflow utilise le fichier `.env` automatiquement grâce à la ligne `load_dotenv()` dans le DAG.

Pour vérifier :

```python
import os
from dotenv import load_dotenv
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
```

---

## ⚠️ Problèmes courants

| Problème                                         | Solution                                                                              |
| ------------------------------------------------ | ------------------------------------------------------------------------------------- |
| `ModuleNotFoundError: No module named 'airflow'` | Installer: `pip install apache-airflow==2.7.3`                                        |
| DAG n'apparaît pas                               | S'assurer que `airflow/dags/recommendation_pipeline.py` existe et sans erreurs Python |
| Tâche échoue                                     | Voir les logs: Cliquer sur la tâche → Logs                                            |
| Port 8080 déjà utilisé                           | `airflow webserver --port 8081`                                                       |
| Scheduler ne s'exécute pas                       | S'assurer qu'il tourne dans un terminal séparé                                        |

---

## 📞 Test rapide

Pour tester le DAG sans Airflow:

```powershell
python -m py_compile airflow/dags/recommendation_pipeline.py
```

Si aucune erreur, le DAG est valide!

---

## ✅ Checklist complet Airflow

- [ ] Airflow installé (`pip install apache-airflow==2.7.3`)
- [ ] Base de données initialisée (`airflow db init`)
- [ ] Utilisateur admin créé (`airflow users create ...`)
- [ ] Webserver lancé (Terminal 1: `airflow webserver --port 8080`)
- [ ] Scheduler lancé (Terminal 2: `airflow scheduler`)
- [ ] Accessible sur http://localhost:8080
- [ ] DAG `recommendation_pipeline` visible
- [ ] DAG activé (toggle on)
- [ ] Vous pouvez le déclencher manuellement

---

## 🎉 Succès !

Une fois que tout fonctionne :

1. Vous verrez le DAG dans l'interface
2. Les tâches s'exécuteront dans l'ordre
3. Les logs vous montreront chaque étape
4. Les recommandations seront importées dans MongoDB

**C'est l'étape 3 complétée!** 🚀
