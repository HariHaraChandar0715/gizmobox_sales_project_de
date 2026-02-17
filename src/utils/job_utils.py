from abc import ABC, abstractmethod


class Job(ABC):
    def __init__(self, sparksession,**kwargs):
        self.spark = sparksession

    @abstractmethod
    def execute(self):
        pass

