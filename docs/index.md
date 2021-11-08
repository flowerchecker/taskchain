# TaskChain

## What is a TaskChain?

TaskChain is a tool for managing data processing pipelines.
It was created to **reduce chaos in machine learning projects**.

**Goals and features:**

- separate computation logic and configuration
- every result should be reproducible
- brake down computation to individual steps in [DAG structure](https://en.wikipedia.org/wiki/Directed_acyclic_graph)
- brake down whole project to smaller pipelines which can be easily configured and reused 
- never compute same thing twice - result of computation steps is saved automatically (data persistence)
- easy access to all intermediate results

## Install 

```bash
pip install taskchain
```

#### From source

```bash
git clone https://github.com/flowerchecker/taskchain
cd taskchain
python setup.py install
```


## Where to start?

- read this documentation
- check [example project]({{ config.code_url }}/example)
- go through [CheatSheet]({{config.base_url}}/cheatsheet) with the most common constructions. 


## Main concepts

- **[task]({{config.base_url}}/tasks)** - one step in computation (data transformation) represented by python class.
        Every task can define two type of inputs:
    - **[input tasks]({{config.base_url}}/tasks#input-tasks)** - other task on which the task depends and take their outputs (data)
    - **[parameter]({{config.base_url}}/tasks#parameters)** - additional values which influence computation 

- **pipeline** - *[group of tasks]({{config.base_url}}/tasks/#task-names-and-groups)* which are closely connected and together represent more complex computation,
        e.g. project can be split to pipeline for data preparation, 
        pipeline for feature extraction and pipeline for model training and evaluation.
        Pipelines are only virtual concept and they not have a strict representation in the framework.

- **[chain]({{config.base_url}}/chains)** - instance of pipeline or multiple pipelines, 
        i.e. tasks connected by their dependencies into DAG (*directed acyclic graph*) with all required parameter values  

- **[config]({{config.base_url}}/configs)** - description (usually YAML file) with information needed to instantiate a chain. i.e.:
    - description of tasks which should be part of a chain (e.g. pipeline)
    - parameter values needed by these tasks
    - eventual dependencies on other configs 


## Typical project structure

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
