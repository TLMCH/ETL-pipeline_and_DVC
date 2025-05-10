Цель проекта — создать базовое решение для предсказания стоимости квартир в Москве, реализовать ETL-пайплайн в Airflow для сбора данных и настроить DVC-пайплайн обучения модели.

Путь до файлов с кодом всех DAG: 
- ETL-pipeline_and_DVC/part1_airflow/dags

Названия функций DAG: 
- prepare_price_dataset()
- clean_price_dataset()

Путь до файлов с Python-кодом для этапов DVC-пайплайна: 
- ETL-pipeline_and_DVC/part2_dvc/scripts

Пути до файлов с конфигурацией DVC-пайплайна:
- ETL-pipeline_and_DVC/part2_dvc/dvc.yaml
- ETL-pipeline_and_DVC/part2_dvc/params.yaml
- ETL-pipeline_and_DVC/part2_dvc/dvc.lock
