Instructions to run the code

Assumptions
- Relevant softwares are installed and properly configured at machine where code will be tested. I have tested at window’s machine and it works well. 
- I have installed ‘spark-3.5.0-bin-hadoop3’ with Python 3.11. Installed & properly configured Java JDK-20 and JRE-1.8. Copied ‘winutils.exe’ for windows at one folder & set HADOOP_HOME.   
- Properly configured “Environment Variables” like HADOOP_HOME, JAVA_HOME, SPARK_HOME, PYTHON_HOME, PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON and Path. 
- Open window command prompt and check python version. It should be installed properly. Install and update the PIP command.


Run with spark-submit
- Clone the code from the repository and navigate to “<actual_path> \dummy_project_1\ src\main” folder.
- Zip the ‘jobs’ folder. You will get jobs.zip
- Move a folder up that is navigate to “<actual_path> \dummy_project_1\ src” folder.
- Copy and paste the command below and replace <actual_path> with your actual path
-----spark-submit --py-files main/jobs.zip app.py --job-name "movie_lens_pipeline" --resource-path " <actual_path> \dummy_project_1\src\main\jobs”
-----My command :
-----D:\pyspark_installed\dummy_project_1\src>spark-submit --py-files main/jobs.zip app.py --job-name "movie_lens_pipeline" --resource-path "D:\pyspark_installed\dummy_project_1\src\main\jobs"


Unit testing
- Navigate to “<actual_path> \dummy_project_1\ src” folder if you are not there in command prompt. Run the following commands.
--- set SPARK_HOME=<actual_path>\spark-3.5.0-bin-hadoop3
--- set HADOOP_HOME=%SPARK_HOME%
--- set PYTHONPATH=%SPARK_HOME%/python;%SPARK_HOME%/python/lib/py4j-0.10.9.7-src.zip;%PYTHONPATH% 
- Change the version of highlighted py4j-0… zip file as per installed at your machine. 
- Install pytest, pyspark-test using pip command if not installed.
--- pip install pytest
--- pip install pyspark-test
- To invoke the unit testing, run the following command 
---<actual_path> \dummy_project_1\src> py.test -s


via python 
set SPARK_HOME=D:\pyspark_installed\spark-3.5.0-bin-hadoop3
set HADOOP_HOME=%SPARK_HOME%
set PYTHONPATH=%SPARK_HOME%/python;%SPARK_HOME%/python/lib/py4j-0.10.9.7-src.zip;%PYTHONPATH%

D:\pyspark_installed\dummy_project_1\src>python app.py --job-name "movie_lens_pipeline" --resource-path "D:\pyspark_installed\dummy_project_1\src\main\jobs"

