# Contributing

## Team

| Member | GitHub | Role |
|--------|--------|------|
| Emircan Kartal | @EmircanKartal (https://github.com/EmircanKartal) | Infrastructure & Integration |
| Meryem Berfin Kenar | @berfinm (https://github.com/berfinm) | Streaming & Analytics |
| Kagan Gur | @kagangur (https://github.com/kagangur) | ML & Experiment Tracking |

## Branch strategy

Each person works on their own branch and opens a PR to main when a task is done.
Never commit directly to main.

    main                  <- protected, only merged PRs
    emircan/infra-kafka   <- Docker, Kafka, Producer, repo setup
    berfin/spark-delta    <- Spark Streaming, Delta layers, EDA
    kagan/ml-mlflow       <- Feature engineering, ML models, MLflow

## Commit message format

    type(scope): short description

| Type  | When to use |
|-------|-------------|
| feat  | New working feature or component |
| fix   | Bug fix or error correction |
| wip   | Work in progress checkpoint |
| docs  | README, report, architecture notes |
| chore | Config, gitignore, dependencies, folder structure |

Examples:

    feat(kafka): add configurable rate producer
    feat(spark): bronze delta write from kafka stream
    fix(delta): null handling in silver cleaning job
    docs(readme): add architecture diagram
    chore: initial project scaffold

## What NOT to commit

- data/       CSV files, raw downloads
- delta/      Delta Lake table files
- mlruns/     MLflow experiment artifacts
- .env        environment secrets
- __pycache__/, .ipynb_checkpoints/

All of the above are in .gitignore. Never use git add -f on these.

## Per-task checklist

Before marking any task done:
- [ ] Code runs locally or inside Docker
- [ ] Screenshot taken (for Docker/Spark/MLflow output)
- [ ] File has a comment explaining what it does
- [ ] Committed on your branch with a proper commit message
- [ ] PR opened or branch pushed to remote

## Responsibility matrix

| Area | Owner | Support |
|------|-------|---------|
| docker-compose.yml + Dockerfiles | Emircan | - |
| Kafka Producer | Emircan | - |
| Data download script | Emircan | - |
| Spark Structured Streaming (job 01) | Berfin | - |
| Bronze to Silver to Gold (jobs 02-03) | Berfin | - |
| EDA notebook | Berfin | - |
| Feature engineering (job 04) | Kagan | - |
| ML models + MLflow (job 05) | Kagan | - |
| Dashboard figures (notebook 06) | Kagan | Berfin |
| Technical report | Berfin + Emircan | - |
| Presentation slides | Everyone | - |
