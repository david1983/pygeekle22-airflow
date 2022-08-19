# Robust data pipelines with airflow using Python

Sometimes creating a Data Pipeline can seem like a trivial problem can it just be solved with a small script and a CRON job. You would be surprised at how many ways this could go wrong. During this talk we will talk about creating data pipelines using Apache Airflow and what it brings to the table.

Here are the points that we will cover during this talk.

- introduction about data pipelines and problems around poor implementations
- brief introduction to airflow
- running airflow on docker
- live coding a DAG
- analysis of a real-word scenario

## Slides

The presentation is available online at https://docs.google.com/presentation/d/1F7aUyLgxcn88mB449QMWiThba-Tc2I2ADmyeuiVwCfc/edit?usp=sharing

or in the repo's slides folder.

## Simpleton data pipeline

In this folder you can find the example of a datapipeline made using a simple script and a CRON job

please create the inbound and processed folder

```
cd simple-data-pipeline &&
mkdir inbound &&
mkdir processed
```

run the gen-data.py script to generate mock data into the inbound folder

## Airflow data pipeline

In this folder you can find the example of a datapipeline made using airflow.
please create the inbound, logs, plugins and processed folders

```
cd airflow-data-pipeline &&
mkdir inbound &&
mkdir processed &&
mkdir logs &&
mkdir plugins
```

run the gen-data.py script to generate mock data into the inbound folder


