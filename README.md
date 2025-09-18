# Airflow Spark connection

In this repository I demonstrate two different ways of connecting a spark script to a stand alone spark cluster using Airflow.

## Using Python operator

In `python_operator_dag.py`, I use a Python operator to run the python script, in this case, I have to mention the master URL explicitly. Moreover, if a username and password is needed I have to mention it in the code, or use environment variables. So the developer, besides coding the business logic, should be worried about the connections too.

I would not recommend this approach.

## Using SparkSubmit operator

`spark_submit_dag.py` reflects a better way of connecting the Spark scripts to the Spark standalone cluster, and that is using a SparkSubmit Operator of Airflow. In this approach, an airflow connection is defined and all of the connection detail is encapsulated there. This is a better approach as the developer just gets concerned with the business logic.

If you are curious about creating the airflow connection through a script, take a look at the `init-airflow.sh`. You can see the following script:

```bash
airflow connections add spark_conn \
    --conn-type spark_connect \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    --conn-extra '{"use_ssl": false}'
```

If you like to create the connections manually, you can always use the Airflow administration. However, I strongly recommend you get familiar with Airflow CLI, and use the script to automate the connection creation. Doing so will enable you to set up CI/CD pipelines that bring up the airflow fully functional and you can version the scripts using git.

## How to use this repo

If you want to use the stack provided in this repository to check the connections, or maybe start building on top of it, please follow these steps.

1. Install docker
2. Bring up docker compose using `docker compose up --build`
3. Access Airflow UI on `localhost:8080`
4. Use `admin` for both username and password
5. Trigger Dags
6. Access Spark UI on `localhost:8081` and monitor Spark jobs.

## License

I used Apache 2.0 License to let you freely use this repo, develop on top of it, or use it for your projects.

I do not suggest you to use this repo as it is in your production environment, as several super secret details are exposed to public, and in general you should avoid putting your credentials in a shell script or docker compose script.

Feel free to use this repo even for your commercial projects, however, all of its responsibility is with you and be careful about the serious security issues that this repo has, one of them I just have mentioned above.
