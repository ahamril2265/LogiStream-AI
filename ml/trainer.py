"""ML Training Module - Model Training with MLflow Integration.

Trains three models using MLflow for experiment tracking:
1. RandomForest: Delay prediction
2. XGBoost: ETA prediction
3. IsolationForest: Driver risk anomaly detection

All models:
- Snapshot feature dataset before training
- Store dataset hash with model version
- Track experiments in MLflow
- Register best models
"""

import logging
from datetime import datetime
from typing import Dict, Any, Tuple
import hashlib

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, roc_auc_score, confusion_matrix
)

try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False

import mlflow
import mlflow.sklearn
import mlflow.xgboost

logger = logging.getLogger(__name__)


class ModelTrainer:
    """Orchestrates ML model training with experiment tracking."""
    
    def __init__(self, config):
        """Initialize trainer.
        
        Args:
            config: Configuration object with MLflow settings
        """
        self.config = config
        self._init_mlflow()
    
    def _init_mlflow(self):
        """Initialize MLflow tracking."""
        try:
            mlflow.set_tracking_uri(self.config.mlflow.tracking_uri)
            mlflow.set_experiment(self.config.mlflow.experiment_name)
            logger.info(f"MLflow initialized: {self.config.mlflow.tracking_uri}")
        except Exception as e:
            logger.warning(f"MLflow initialization failed, proceeding without tracking: {e}")
    
    def train_delay_model(self, feature_df: pd.DataFrame, 
                         feature_hash: str) -> Dict[str, Any]:
        """Train RandomForest for delay prediction.
        
        Args:
            feature_df: delay_model_features_v1 dataset
            feature_hash: Hash of feature dataset
        
        Returns:
            Model metadata and performance metrics
        """
        logger.info("Starting delay model training...")
        
        if feature_df.empty:
            logger.warning("Empty feature dataset, skipping training")
            return {"status": "skipped", "reason": "no data"}
        
        with mlflow.start_run(run_name="delay_model_rf"):
            try:
                # Prepare data
                X = feature_df.drop(columns=["delay_flag", "order_id"], errors="ignore")
                y = feature_df["delay_flag"]
                
                # Handle missing values
                X = X.fillna(X.mean(numeric_only=True))
                
                # Train/test split
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=0.2, random_state=42, stratify=y
                )
                
                # Train model
                model = RandomForestClassifier(
                    n_estimators=100,
                    max_depth=10,
                    min_samples_split=5,
                    random_state=42,
                    n_jobs=-1,
                    class_weight="balanced",
                )
                
                model.fit(X_train, y_train)
                
                # Evaluate
                y_pred = model.predict(X_test)
                y_pred_proba = model.predict_proba(X_test)[:, 1]
                
                metrics = {
                    "accuracy": accuracy_score(y_test, y_pred),
                    "precision": precision_score(y_test, y_pred),
                    "recall": recall_score(y_test, y_pred),
                    "f1": f1_score(y_test, y_pred),
                    "roc_auc": roc_auc_score(y_test, y_pred_proba),
                }
                
                # Log parameters
                mlflow.log_param("feature_hash", feature_hash)
                mlflow.log_param("n_estimators", 100)
                mlflow.log_param("max_depth", 10)
                mlflow.log_param("training_rows", len(X_train))
                
                # Log metrics
                for metric_name, metric_value in metrics.items():
                    mlflow.log_metric(metric_name, metric_value)
                
                # Log dataset signature
                mlflow.log_param("dataset_hash", feature_hash)
                mlflow.log_param("training_timestamp", datetime.utcnow().isoformat())
                
                # Log model
                mlflow.sklearn.log_model(model, "model")
                
                logger.info(f"Delay model training complete: {metrics}")
                
                # Register model
                mlflow.register_model(
                    model_uri=f"runs:/{mlflow.active_run().info.run_id}/model",
                    name="delay_prediction_rf"
                )
                
                return {
                    "model": "delay_prediction_rf",
                    "algorithm": "RandomForest",
                    "metrics": metrics,
                    "feature_hash": feature_hash,
                    "training_timestamp": datetime.utcnow().isoformat(),
                }
            
            except Exception as e:
                logger.error(f"Delay model training failed: {e}")
                mlflow.log_param("status", "failed")
                mlflow.log_param("error", str(e))
                raise
    
    def train_eta_model(self, feature_df: pd.DataFrame,
                       feature_hash: str) -> Dict[str, Any]:
        """Train XGBoost regressor for ETA prediction.
        
        Args:
            feature_df: eta_model_features_v1 dataset
            feature_hash: Hash of feature dataset
        
        Returns:
            Model metadata and performance metrics
        """
        logger.info("Starting ETA model training...")
        
        if not XGBOOST_AVAILABLE:
            logger.warning("XGBoost not installed, skipping ETA model training")
            return {"status": "skipped", "reason": "xgboost not available"}
        
        if feature_df.empty:
            return {"status": "skipped", "reason": "no data"}
        
        with mlflow.start_run(run_name="eta_model_xgb"):
            try:
                # Prepare data
                X = feature_df.drop(
                    columns=["actual_eta_minutes", "order_id"], 
                    errors="ignore"
                )
                y = feature_df["actual_eta_minutes"]
                
                # Handle missing values
                X = X.fillna(X.mean(numeric_only=True))
                
                # Train/test split
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=0.2, random_state=42
                )
                
                # Train model
                model = xgb.XGBRegressor(
                    n_estimators=100,
                    max_depth=6,
                    learning_rate=0.1,
                    random_state=42,
                    n_jobs=-1,
                )
                
                model.fit(X_train, y_train)
                
                # Evaluate
                y_pred = model.predict(X_test)
                
                mae = np.mean(np.abs(y_test - y_pred))
                rmse = np.sqrt(np.mean((y_test - y_pred)**2))
                r2 = model.score(X_test, y_test)
                
                metrics = {
                    "mae": mae,
                    "rmse": rmse,
                    "r2": r2,
                }
                
                # Log parameters
                mlflow.log_param("feature_hash", feature_hash)
                mlflow.log_param("n_estimators", 100)
                mlflow.log_param("max_depth", 6)
                mlflow.log_param("learning_rate", 0.1)
                
                # Log metrics
                for metric_name, metric_value in metrics.items():
                    mlflow.log_metric(metric_name, metric_value)
                
                mlflow.xgboost.log_model(model, "model")
                
                logger.info(f"ETA model training complete: {metrics}")
                
                # Register model
                mlflow.register_model(
                    model_uri=f"runs:/{mlflow.active_run().info.run_id}/model",
                    name="eta_prediction_xgb"
                )
                
                return {
                    "model": "eta_prediction_xgb",
                    "algorithm": "XGBoost",
                    "metrics": metrics,
                    "feature_hash": feature_hash,
                    "training_timestamp": datetime.utcnow().isoformat(),
                }
            
            except Exception as e:
                logger.error(f"ETA model training failed: {e}")
                mlflow.log_param("status", "failed")
                raise
    
    def train_driver_risk_model(self, feature_df: pd.DataFrame,
                               feature_hash: str) -> Dict[str, Any]:
        """Train IsolationForest for driver risk anomaly detection.
        
        Args:
            feature_df: driver_risk_features_v1 dataset
            feature_hash: Hash of feature dataset
        
        Returns:
            Model metadata
        """
        logger.info("Starting driver risk model training...")
        
        if feature_df.empty:
            return {"status": "skipped", "reason": "no data"}
        
        with mlflow.start_run(run_name="driver_risk_iso_forest"):
            try:
                # Prepare data (all columns except driver_id)
                X = feature_df.drop(columns=["driver_id"], errors="ignore")
                X = X.fillna(X.mean(numeric_only=True))
                
                # Standardize features
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # Train model (anomaly_score = 1 means anomaly in scikit-learn)
                model = IsolationForest(
                    n_estimators=100,
                    contamination=0.05,  # Assume 5% of drivers are anomalous
                    random_state=42,
                    n_jobs=-1,
                )
                
                anomaly_labels = model.fit_predict(X_scaled)
                anomaly_scores = model.score_samples(X_scaled)
                
                # Calculate metrics
                n_anomalies = (anomaly_labels == -1).sum()
                anomaly_ratio = n_anomalies / len(X_scaled)
                
                metrics = {
                    "n_anomalies": int(n_anomalies),
                    "anomaly_ratio": float(anomaly_ratio),
                }
                
                # Log parameters and metrics
                mlflow.log_param("feature_hash", feature_hash)
                mlflow.log_param("n_estimators", 100)
                mlflow.log_param("contamination", 0.05)
                mlflow.log_metric("n_anomalies", n_anomalies)
                mlflow.log_metric("anomaly_ratio", anomaly_ratio)
                
                mlflow.sklearn.log_model(model, "model")
                
                logger.info(f"Driver risk model training complete: {metrics}")
                
                # Register model
                mlflow.register_model(
                    model_uri=f"runs:/{mlflow.active_run().info.run_id}/model",
                    name="driver_risk_iso_forest"
                )
                
                return {
                    "model": "driver_risk_iso_forest",
                    "algorithm": "IsolationForest",
                    "metrics": metrics,
                    "feature_hash": feature_hash,
                    "training_timestamp": datetime.utcnow().isoformat(),
                }
            
            except Exception as e:
                logger.error(f"Driver risk model training failed: {e}")
                mlflow.log_param("status", "failed")
                raise


def get_model_registry(mlflow_tracking_uri: str) -> Dict[str, Dict]:
    """Query MLflow model registry for latest models.
    
    TODO (Production):
    - Load models from registry
    - Track model lineage
    - Implement model governance
    - Add A/B testing support
    """
    models_info = {
        "delay_prediction_rf": {"status": "tracked in MLflow"},
        "eta_prediction_xgb": {"status": "tracked in MLflow"},
        "driver_risk_iso_forest": {"status": "tracked in MLflow"},
    }
    return models_info
