import os
import sys
import argparse
import logging
import logging.config
import importlib
import json


# Activate main logging
logging.config.fileConfig('config/logging.config')

#Get config 
def get_config(path, job_name):
    file_path = path + '/' + job_name + '/resources/job_config.json'
    with open(file_path, encoding='utf-8') as job_config_json:
        config = json.loads(job_config_json.read())

    config['job_name'] = job_name
    return config



""" The main function to call multiple pipelines"""
def main():    
    logger_main = logging.getLogger(__name__)
    try:
        parser = argparse.ArgumentParser(description='Pyspark job arguments')
        parser.add_argument('--job-name', type=str, required=True, dest='job_name',
                            help='The name of the spark job you want to run')
        parser.add_argument('--resource-path', type=str, required=True, dest='res_path',
                            help='Path to the jobs resources')

        args = parser.parse_args()
        
        job_name = args.job_name
        res_path = args.res_path

        module_name = "main.jobs."+job_name+"."+job_name
        logger_main.info('Start of pipeline {} . '.format(module_name))
        
        # Call the module to run the pipeline.
        job_module = importlib.import_module(module_name)
        config = get_config(res_path, job_name)
        job_module.run(config)

        logger_main.info('End of pipelines {} .'.format(module_name))

    except Exception as exp: 
        logger_main.error(str(exp))
        logger_main.info('Pipeline failed and some error occured. Please check the application logs for details.')


if __name__ == '__main__':
    main()