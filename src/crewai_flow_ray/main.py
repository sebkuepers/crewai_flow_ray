#!/usr/bin/env python
import ray
from random import randint
from typing import List
from pydantic import BaseModel

from crewai.flow import Flow, listen, start
from crewai_flow_ray.crews.poem_crew.poem_crew import PoemCrew

# Initialize Ray (minimal configuration; adjust as needed for your cluster)
ray.init(ignore_reinit_error=True)

class PoemState(BaseModel):
    sentence_count: int = 1
    num_poems: int = 1           # New parameter: number of poems to generate
    poems: List[str] = []        # Store multiple poems

# Define a minimal remote task to run PoemCrew on the cluster
@ray.remote
def run_poem_task(sentence_count: int):
    result = PoemCrew().crew().kickoff(inputs={"sentence_count": sentence_count})
    return result.raw

class PoemFlow(Flow[PoemState]):

    @start()
    def generate_sentence_count(self):
        print("Generating sentence count")
        self.state.sentence_count = randint(1, 5)
        # Set the number of poems to generate (this could come from an external parameter)
        self.state.num_poems = 3

    @listen(generate_sentence_count)
    def generate_poem(self):
        print("Generating poem(s)")
        # Launch one task per poem using Ray
        tasks = [
            run_poem_task.remote(self.state.sentence_count)
            for _ in range(self.state.num_poems)
        ]
        # Collect results concurrently
        results = ray.get(tasks)
        self.state.poems = results
        print("Poems generated:", results)

    @listen(generate_poem)
    def save_poem(self):
        print("Saving poems")
        with open("poems.txt", "w") as f:
            for poem in self.state.poems:
                f.write(poem + "\n---\n")

def kickoff():
    poem_flow = PoemFlow()
    poem_flow.kickoff()
    return poem_flow.state.poems

def plot():
    poem_flow = PoemFlow()
    poem_flow.plot()

if __name__ == "__main__":
    poems = kickoff()
    print("Generated poems:", poems)