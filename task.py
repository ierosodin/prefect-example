import datetime
import numpy as np
import os

os.environ["PREFECT__LOGGING__LEVEL"] = "ERROR"

import prefect
from prefect import task
from prefect.engine.signals import SKIP
from prefect.tasks.shell import ShellTask
import xarray as xr
from PIL import Image
from prefect import Parameter, Flow
from prefect.schedules.schedules import Schedule
from prefect.schedules.schedules import IntervalSchedule

from dask.distributed import Client, LocalCluster
from prefect.engine.executors import DaskExecutor, LocalDaskExecutor
from dask.distributed import as_completed
import time


class DataLoader():
    def __init__(self):
        self.preprocess_section = {
            "4": {
                "input_list": {
                    "source": ["start", "image"],
                },
                "task_fn": "resize",
                "param": {
                },
            },
            "5": {
                "input_list": {
                    "source": ["4", "image"],
                },
                "task_fn": "transpose",
                "param": {
                },
            },
            "6": {
                "input_list": {
                    "source": ["4", "image"],
                },
                "task_fn": "transpose",
                "param": {
                },
            },
            "7": {
                "input_list": {
                    "source1": ["5", "image"],
                    "source2": ["6", "image"],
                    "source3": ["4", "image"],
                },
                "task_fn": "stop",
                "param": {
                },
            },
        }
        # cluster = LocalCluster(n_workers=2, threads_per_worker=1, dashboard_address=':8788')
        # client = Client(n_workers=1, threads_per_worker=4)#cluster)
        # self.executor = DaskExecutor(address=client.scheduler.address, local_processes=True)
        # self.executor = DaskExecutor(n_workers=1, threads_per_worker=4, dashboard_address="0", scheduler_port=0)
        self.count = 0

    def set_graph(self, client2):
        global flow, output_cache
        global executor
        # self.data_list = np.arange(1, 800000)
        # cluster = LocalCluster(n_workers=1, threads_per_worker=8, dashboard_address="0", scheduler_port=0, processes=False)
        # client = Client(cluster)
        executor = LocalDaskExecutor(address=client2.scheduler.address)

        schedule = IntervalSchedule(interval=datetime.timedelta(seconds=1))
        # executor = DaskExecutor(n_workers=1, threads_per_worker=4, dashboard_address="0", scheduler_port=0, processes=False)
        with Flow("Image ETL") as flow:
            # we use the `upstream_tasks` keyword to specify non-data dependencies
            output_cache = {}
            output_cache["start"] = self.start(upstream_tasks=[])
            for node_id in self.preprocess_section:
                input_list = self.preprocess_section[node_id]["input_list"]
                task_fn = self.preprocess_section[node_id]["task_fn"]
                param = self.preprocess_section[node_id]["param"]
                kwargs = {}
                for key in input_list:
                    kwargs[key] = output_cache[input_list[key][0]][input_list[key][1]]
                output_cache[node_id] = getattr(self, task_fn)(**kwargs)

    @task(skip_on_upstream_skip=True)
    def start():
        rs = np.random.RandomState(os.getpid() + int.from_bytes(os.urandom(4), byteorder='little') >> 1)
        image = np.ones((640, 480, 3)) * rs.randint(1, 100)
        return {"image": image}

    @task(name="resize", max_retries=3, retry_delay=datetime.timedelta(seconds=3))
    def resize(source):
        img = Image.fromarray(np.uint8(source))
        img = img.resize((4, 4))
        return {"image": np.array(img)}

    @task(name="transpose", max_retries=3, retry_delay=datetime.timedelta(seconds=3))
    def transpose(source):
        img = Image.fromarray(np.uint8(source))
        img = img.transpose(2)
        return {"image": np.array(img)}

    @task(name="stop", max_retries=3, retry_delay=datetime.timedelta(seconds=3))
    def stop(source1, source2, source3):
        img1 = Image.fromarray(np.uint8(source1))
        img2 = Image.fromarray(np.uint8(source2))
        img3 = Image.fromarray(np.uint8(source3))
        img1.save("{}.png".format(j))
        return j
        return {"source1": np.array(img1),
                "source2": np.array(img2),
                "source3": np.array(img3),
               }

    def run(self, i):
        # client = Client(n_workers=1, threads_per_worker=4)#cluster)
        global j
        j = i
        state = flow.run(executor=executor)
        return j#state.result[output_cache["7"]].result

# DATA_FILE = Parameter("DATA_FILE", default="image-data.img")


if __name__ == "__main__":
    data_loader = DataLoader()
    # data_loader.flow.visualize()
    """
    print(time.time())
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, initializer=data_loader.set_graph) as executor:
        for ret in executor.map(data_loader.run, range(100)):
            print(ret)
            pass
    """

    client2 = Client(host='10.20.10.201:8789', n_workers=4, threads_per_worker=1)
    # client2 = Client('tcp://10.20.10.201:8789')
    data_loader.set_graph(client2)
    """
    print(time.time())
    for i in range(100):
        print(data_loader.run(i))
    print(time.time())
    """
    # cluster = LocalCluster(n_workers=8, dashboard_address="0", scheduler_port=0)
    # client = Client(cluster)
    futures = client2.map(data_loader.run, range(1000))
    print(time.time())
    for future in as_completed(futures):
        print(future.result())
        del future
    # data_loader.run()
    print(time.time())
    client2.shutdown()
