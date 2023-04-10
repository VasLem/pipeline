# pipeline-factory

## Design Statements ##

The pipeline factory generates pipelines.

- A **Block** is the elementary block of the pipeline.
- A **Pipeline** can contain pipelines as steps.
- An **IterativePipeline** can run multiple **Pipeline** instances of itself, uniquely identified by the keys of an input dictionary, with inputs the values of that dictionary.
- A **Decimator** is a **Block** that generates input for an **IterativePipeline**.
- An **Aggregator** is a **Block** that accumulates the output from an **IterativePipeline**.
- A **SwitchPipeline** is an **IterativePipeline** with a variable set of steps, controlled by the current instance counter.
- Each run is controlled by a configuration object that inherits from `utils.config.Configuration`.
- For all the objects defined above, they must have a unique name set upon construction.


## Function Statements ##

- The **Block** runs a provided function.
- The output of a **Pipeline** is the output of its last **Block**, recursively.
- The input to a **Pipeline** is the input to its first **Block**, recursively.
- Each step of a **Pipeline** receives as input the output from the previous step.
- The function of each **Block** receives as input a reference to its parent and the input to the block.
- Only positional input arguments are passed to each **Block** caller.
- Each **Block** and **Pipeline** are cached upon request.
- The root **Pipeline** or **Block** always need a `RunConfiguration`.
- A MongoDB Database is used locally to report pipeline results. The results are saved in a folder.

## Nonmenclature ##

- A single input to the pipeline is assumed to correspond to a `sample`.
- An `instance` is an object instance at the current runtime.
- A `RunConfiguration` is the configuration that controls a single run.

## Example Usage ##

```python

from utils.config import Configuration

class RunConfiguration(Configuration):
    def __init__(self, field1, field2):
        self.field1 = field1
        self.field2 = field2

    @property
    def fields(self):
        return ['field1', 'field2']

    @property
    def hiddenFields(self):
        """
        Fields not to be used in __str__ or __repr__
        """
        return []
    
    @property
    def configID(self) -> str:
        """
        A unique identifier for the configuration
        """
        return str(field1) + str(field2)

from pipeline_factory.pipeline import Pipeline
from pipeline_factory.block import Block

def p1fn1(parent):
    print("This is the first block of the first pipeline")
    print("The current configuration is:", parent.runConfig)
    return 1, 'a'

def p1fn2(parent, cnt, ch):
    print("This is the second block of the first pipeline")
    return cnt + 1, ch + 1

def p2fn1(parent, cnt, ch):
    print("This is the first block of the second pipeline")
    return cnt + 1, ch + 1

def p2fn2(parent, cnt, ch):
    print("This is the second block of the second pipeline")
    return cnt + 1, ch + 1

root = Pipeline(steps=[Pipeline('Pipeline1',steps=[Block('Block11', p1fn1), Block('Block12', p1fn2)]),
                       Pipeline('Pipeline2',steps=[Block('Block21', p2fn1), Block('Block22', p2fn2)])],
                       runConfig=RunConfiguration())
print(root())
print("Here the cache is loaded")
print(root()) # here the cache is loaded
root.clearCache() # the cache is cleared
print("The cache is cleared")
print(root()) # the steps are run again
```

Output:

```
INFO - 2023-04-10 20:25:32,396 - pipeline::_run():368: Running Pipeline Root..
INFO - 2023-04-10 20:25:32,402 - pipeline::_run():368: Running Pipeline Root.Pipeline1..
INFO - 2023-04-10 20:25:32,406 - block::_run():329: Running: Root.Pipeline1.Block11..
This is the first block of the first pipeline
The current configuration is: RunConfiguration
field1: Field1
field2: Field2
INFO - 2023-04-10 20:25:32,635 - cacher::updateCache():64: Updating cache of Block11..
INFO - 2023-04-10 20:25:32,908 - block::_run():329: Running: Root.Pipeline1.Block12..
This is the second block of the first pipeline
INFO - 2023-04-10 20:25:32,999 - cacher::updateCache():64: Updating cache of Block12..
INFO - 2023-04-10 20:25:33,183 - cacher::updateCache():64: Updating cache of Pipeline1..
INFO - 2023-04-10 20:25:33,242 - pipeline::_run():368: Running Pipeline Root.Pipeline2..
INFO - 2023-04-10 20:25:33,245 - block::_run():329: Running: Root.Pipeline2.Block21..
This is the first block of the second pipeline
INFO - 2023-04-10 20:25:33,335 - cacher::updateCache():64: Updating cache of Block21..
INFO - 2023-04-10 20:25:33,517 - block::_run():329: Running: Root.Pipeline2.Block22..
This is the second block of the second pipeline
INFO - 2023-04-10 20:25:33,609 - cacher::updateCache():64: Updating cache of Block22..
INFO - 2023-04-10 20:25:33,790 - cacher::updateCache():64: Updating cache of Pipeline2..
INFO - 2023-04-10 20:25:33,845 - cacher::updateCache():64: Updating cache of Root..
(4, 'd')
Here the cache is loaded
INFO - 2023-04-10 20:25:33,909 - block::onSuccessfulCachingLoad():356: Step Root.Pipeline1.Block11 is cached, skipping
INFO - 2023-04-10 20:25:33,909 - block::onSuccessfulCachingLoad():356: Step Root.Pipeline1.Block12 is cached, skipping
INFO - 2023-04-10 20:25:33,910 - block::onSuccessfulCachingLoad():356: Step Root.Pipeline2.Block21 is cached, skipping
INFO - 2023-04-10 20:25:33,910 - block::onSuccessfulCachingLoad():356: Step Root.Pipeline2.Block22 is cached, skipping
(4, 'd')
The cache is cleared
INFO - 2023-04-10 20:25:34,179 - pipeline::_run():368: Running Pipeline Root..
INFO - 2023-04-10 20:25:34,185 - pipeline::_run():368: Running Pipeline Root.Pipeline1..
INFO - 2023-04-10 20:25:34,188 - block::_run():329: Running: Root.Pipeline1.Block11..
This is the first block of the first pipeline
The current configuration is: RunConfiguration
field1: Field1
field2: Field2
INFO - 2023-04-10 20:25:34,278 - cacher::updateCache():64: Updating cache of Block11..
INFO - 2023-04-10 20:25:34,458 - block::_run():329: Running: Root.Pipeline1.Block12..
This is the second block of the first pipeline
INFO - 2023-04-10 20:25:34,549 - cacher::updateCache():64: Updating cache of Block12..
INFO - 2023-04-10 20:25:34,740 - cacher::updateCache():64: Updating cache of Pipeline1..
INFO - 2023-04-10 20:25:34,795 - pipeline::_run():368: Running Pipeline Root.Pipeline2..
INFO - 2023-04-10 20:25:34,798 - block::_run():329: Running: Root.Pipeline2.Block21..
This is the first block of the second pipeline
INFO - 2023-04-10 20:25:34,889 - cacher::updateCache():64: Updating cache of Block21..
INFO - 2023-04-10 20:25:35,068 - block::_run():329: Running: Root.Pipeline2.Block22..
This is the second block of the second pipeline
INFO - 2023-04-10 20:25:35,159 - cacher::updateCache():64: Updating cache of Block22..
INFO - 2023-04-10 20:25:35,345 - cacher::updateCache():64: Updating cache of Pipeline2..
INFO - 2023-04-10 20:25:35,393 - cacher::updateCache():64: Updating cache of Root..
(4, 'd')
```
