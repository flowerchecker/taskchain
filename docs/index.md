# TaskChain

## What is TaskChain?

TaskChain is a tool for managing data processing pipelines.
It was created to reduce chaos in machine learning projects.


## Install 

#### From git

```bash
git clone git@gitlab.com:flowerchecker/taskchain.git
cd taskchain
python setup.py install
```


## Main concepts

- **[task](/tasks)** - one step in computation (data transformation) represented by python class.
        Every task can define two type of inputs:
    - **input tasks** - other task on which the task depends and take their outputs (data)
    - **[parameter](/tasks#parameters)** - additional values which influence computation 

- **pipeline** - *group of tasks* which are closely connected and together represent more complex computation,
        e.g., project can be split to pipeline for data preparation, 
        pipeline for feature extraction and model training and evaluation.
        Pipelines are only virtual concept and they not have a strict representation in the framework.

- **[chain](/chains)** - instance of pipeline or multiple pipelines, 
        i.e. tasks connected by their dependencies into *acyclic oriented graph* with all required parameter values  

- **[config](/configs)** - description (usually YAML file) with information needed to instantiate a chain. i.e.:
    - description of task which should be part of a chain (e.g. pipeline)
    - parameter values needed by these task
    - eventual dependencies on other configs 


## Typical project structure

Check [example project]({{ config.code_url }}/example)


```
project_name
├── configs                     pipeline configuration files
│   ├── pipeline_1                  usualy organized to dirs, one per pipeline
│   ├── pipeline_2
│   └── ...
├── data                        important data, which should be kept in repo, e.g. annotations
├── scripts                     runscripts, jupyter notebooks, etc.
├── src
│   └── project_name
│       ├── tasks               definistions of tasks
│       │   ├── pipeline_1.py
│       │   ├── pipeline_2.py
│       │   └── ...
│       ├── utils               other code
│       ├── ...
│       └── config.py           global project configuration,
├── README.md                       e.g. path to big data or presistence dir
└── setup.py
```
