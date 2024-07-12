首先安装docker windows ，然后vscode安装focker插件，创建docker文件夹，vscode打开，打开终端
docker version   
docker-compose version

https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml   下载对应版本  2.9.1可以换成其他版本

volumes:
  - F:/application running/Im in AU/university of adelaide/AWS/ETL/airflow/dags:/opt/airflow/dags
  - F:/application running/Im in AU/university of adelaide/AWS/ETL/airflow/logs:/opt/airflow/logs
  - F:/application running/Im in AU/university of adelaide/AWS/ETL/airflow/config:/opt/airflow/config
  - F:/application running/Im in AU/university of adelaide/AWS/ETL/airflow/plugins:/opt/airflow/plugins

docker compose up airflow-init

docker compose up: 这个命令会根据 docker-compose.yml 文件启动定义的所有服务。如果加上 -d 参数，服务会在后台运行。
airflow-init: 这是 docker-compose.yml 文件中定义的一个服务名称。这个服务通常用于执行一些初始化任务，例如数据库迁移、创建用户等。

docker compose down