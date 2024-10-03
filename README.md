# Projet : Examen de Base de Données (dit-db-exam)

## 1. Contexte et Objectif
Ce projet est développé dans le cadre d'un examen de base de données. Il met en œuvre un pipeline de traitement de données sur Kubernetes, utilisant les technologies de pointe pour démontrer la gestion, l’ingestion, le traitement et le stockage de données en environnement distribué. 

Les composants suivants sont utilisés :

- **Kafka** : Système de messagerie pour la collecte de données.
- **Spark** : Framework de calcul distribué pour le traitement en temps réel.
- **Cassandra** : Base de données NoSQL pour stocker les résultats des calculs.
- **Airflow** : Orchestrateur de flux de travail pour automatiser et planifier les jobs.

## 2. Structure du Projet
Le projet est organisé en plusieurs répertoires distincts pour assurer la modularité et la clarté :

- **`deployment/`** : Contient les manifests Kubernetes pour déployer les composants nécessaires :
  - `airflow.yaml` : Déploiement de l’orchestrateur Apache Airflow.
  - `cassandra.yaml` : Déploiement de la base de données Cassandra.
  - `kafka-zookeeper.yaml` : Déploiement du cluster Kafka et de Zookeeper.
  - `postgres.yaml` : Base de données pour stocker les métadonnées d’Airflow.
  - `redis.yaml` : Déploiement de Redis pour la file d’attente de tâches Airflow.
  - `spark.yaml` : Déploiement du cluster Apache Spark.

- **`src/`** : Scripts et jobs utilisés dans le pipeline de traitement :
  - `airflow-scripts-copy-job.yaml` : Job Kubernetes pour copier les scripts d’Airflow.
  - `cassandra-schema-setup-job.yaml` : Job de création du schéma initial dans Cassandra.
  - `spark-script-copy-job.yaml` : Job pour copier les scripts Spark dans le cluster.
  - `spark-submit-job.yaml` : Job pour soumettre les tâches Spark.
  
  - **`airflow/`** : Scripts DAG pour l’orchestration et Dockerfile d’Airflow.
    - `scripts/` : Contient les scripts Python pour générer et traiter les flux de données.
    - `airflow.Dockerfile` : Dockerfile pour créer l'image d'Airflow.
    - `requirements.txt` : Liste des dépendances nécessaires pour Airflow.
  
  - **`spark/`** : Contient les scripts Spark pour le traitement des données.
    - `spark.Dockerfile` : Dockerfile pour Spark.
    - `spark_streaming.py` : Script principal pour le traitement de flux Kafka.
    - `test_job.py` : Script de test pour vérifier l’intégration de Spark.

- **`README.md`** : Documentation principale pour comprendre et configurer le projet.
- **Fichiers YAML supplémentaires** : Définissent des rôles, des permissions et des configurations Kubernetes (ex : `pod-exec-role.yaml`, `namespace.yaml`, `rwo-pvc.yaml`).

## 3. Prérequis
- **Kubernetes** (v1.21+)
- **Docker** (v20.10+)
- **Helm** (v3.5+)
- **kubectl** pour interagir avec le cluster Kubernetes
- **Accès à un cluster Kubernetes** pour le déploiement

## 4. Instructions de Déploiement sur Kubernetes

### Étape 1 : Cloner le projet
```bash
git clone https://github.com/Mx-Bx/dit-db-exam.git
cd dit-db-exam/examen-db
```

### Étape 2 : Créer le namespace dédié
Avant de déployer les services, créez un namespace isolé :
```bash
kubectl create namespace dit-db-exam
```

### Étape 3 : Déployer les Services Composants
- **Kafka et Zookeeper** :
  ```bash
  kubectl apply -f deployment/kafka-zookeeper.yaml -n dit-db-exam
  ```

- **Cassandra** :
  ```bash
  kubectl apply -f deployment/cassandra.yaml -n dit-db-exam
  ```

- **Spark** :
  ```bash
  kubectl apply -f deployment/spark.yaml -n dit-db-exam
  ```

- **Airflow** :
  ```bash
  kubectl apply -f deployment/airflow.yaml -n dit-db-exam
  ```

### Étape 4 : Configurer les Permissions et Volumes
Assurez-vous que les permissions et volumes requis sont bien configurés :
```bash
kubectl apply -f pod-access-role.yaml -n dit-db-exam
kubectl apply -f rwo-pvc.yaml -n dit-db-exam
```

## 5. Exécution des Tâches et Workflow

### 5.1. Configuration du Schéma dans Cassandra
Lancer le job de création de schéma :
```bash
kubectl apply -f src/cassandra-schema-setup-job.yaml -n dit-db-exam
```

### 5.2. Lancer un Job Spark
Soumettre un job Spark pour analyser les flux de données en temps réel :
```bash
kubectl apply -f src/spark-submit-job.yaml -n dit-db-exam
```

### 5.3. Orchestration avec Airflow
Configurer les DAGs et vérifier que les tâches s’exécutent correctement.

## 6. Schéma et Flux de Données
- **Kafka** collecte les messages depuis les sources de données.
- **Spark** analyse et transforme les flux de données en temps réel.
- **Cassandra** stocke les résultats traités.
- **Airflow** orchestre les différentes étapes du pipeline.

## 7. Contribution
Les contributions sont les bienvenues ! Ouvrez une issue ou une pull request pour proposer des améliorations.

## 8. Auteurs
Ce projet a été développé dans le cadre d'un **examen de base de données** par **Mx-Bx**.