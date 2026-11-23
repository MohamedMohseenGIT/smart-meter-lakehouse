# ADR-0004: Model lifecycle: point-in-time features, out-of-time validation, alias-based promotion

**Status:** accepted

## Context
The churn model predicts the future, so leakage and optimistic validation are the main risks. Models must
be reproducible, governed and replaceable without code changes in the scoring job.

## Decision
* **Features** are computed per cutoff date from data strictly before the cutoff, and customer attributes
  come from the SCD2 version valid on the cutoff (as-of join). Labels look 30 days ahead.
* **Validation** uses the latest cutoffs as an out-of-time test set; metrics are ROC-AUC, PR-AUC and
  recall in the top 10% (what a retention team can actually call).
* **Registry**: models are logged with MLflow and registered in Unity Catalog. A gate promotes a version to
  `@champion` only if AUC ≥ minimum and not worse than the current champion; otherwise `@challenger`.
* **Scoring** always loads `models:/<name>@champion`, so promotion and rollback are alias changes.
* **Monitoring**: PSI per feature against the training reference; > 0.2 triggers retraining.

## Why scikit-learn instead of AutoML here
At Airties AutoML was used to get fast baselines. Free Edition runs on serverless compute, where AutoML
experiments are not the target workflow, so this project uses an explicit scikit-learn pipeline. This
keeps every step visible and testable, which is also what a reviewer wants to see.
