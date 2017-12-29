#!/usr/bin/env python
import os
import sys
import argparse
import subprocess
import re
import time
import logging
import  socket
from dask.distributed import Client
from pangeo import PBSCluster

global logger
logger = logging.getLogger(__name__)

def parse_command_line(args, description):

    parser = argparse.ArgumentParser(description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--project", default=os.environ.get("PROJECT"),
                        help="Specify a project id for the case (optional)."
                        "Used for accounting when on a batch system."
                        "The default is user-specified environment variable PROJECT")

    parser.add_argument("--workers", default=0, type=int,
                        help="Number of client nodes to launch")

    parser.add_argument("--walltime", default="01:00:00",
                        help="Set the wallclock limit for all nodes. ")

    parser.add_argument("--queue", default="economy",
                        help="Set the queue to use for submitted jobs. ")

    parser.add_argument("--workdir",
                        default=os.path.join("/glade","scratch",os.environ.get("USER")),
                        help="Set the working diirectory")

    parser.add_argument("--notebookdir",
                        default=os.path.join("/glade","p","work",os.environ.get("USER")),
                        help="Set the notebook diirectory")

    parser.add_argument("--notebook-port",type=int,
                        default=8877,
                        help="Set the notebook tcp/ip port")

    parser.add_argument("--dashboard-port",type=int,
                        default=8878,
                        help="Set the dask tcp/ip port")

    args = parser.parse_args(args)

    return args.project, args.workers, args.walltime, args.notebookdir, args.workdir,args.notebook_port, args.dashboard_port, args.queue


def start_jlab(dask_scheduler, host=None, port='8888', notebook_dir=''):
    cmd = ['jupyter', 'lab', '--ip', host,
           '--no-browser', '--port', port,
           '--notebook-dir', notebook_dir]

    proc = subprocess.Popen(cmd)
    dask_scheduler.jlab_proc = proc


def get_logger(log_level):
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(' - '.join(
        ["%(asctime)s", "%(name)s", "%(levelname)s", "%(message)s"]))
    ch.setFormatter(formatter)
    logger = logging.getLogger(__file__)
    logger.setLevel(log_level)
    ch.setLevel(log_level)
    logger.addHandler(ch)
    return logger


def setup_jlab(client, scheduler_file, jlab_port, dash_port, notebook_dir,
               hostname):

    logger.debug('Getting hostname where scheduler is running')
    host = client.run_on_scheduler(socket.gethostname)
    logger.info('host is %s' % host)

    logger.info('Starting jupyter lab on host')
    client.run_on_scheduler(start_jlab, host=host, port=jlab_port,
                            notebook_dir=notebook_dir)
    logger.debug('Done.')

    user = os.environ['USER']
    print('Run the following command from your local machine:')
    print(f'ssh -N -L {jlab_port}:{host}:{jlab_port} '
          f'-L {dash_port}:{host}:8787 {user}@{hostname}')
    print('Then open the following URLs:')
    print(f'\tJupyter lab: http://localhost:{jlab_port}')
    print(f'\tDask dashboard: http://localhost:{dash_port}', flush=True)


def _main_func(args, description):
    project, workers, walltime, notebookdir, workdir, notebook_port, dashboard_port, job_queue = parse_command_line(args, description)
    scheduler_file = os.path.join(workdir,"scheduler.json")
    with PBSCluster(queue=job_queue, walltime=walltime, project=project, interface='ib0',\
                    extra="--scheduler-file {} --local-directory {}".
                    format(scheduler_file, workdir)) as cluster:
        logger.debug("cluster config is {}".format(cluster.config))
        with Client(cluster) as client:
            workers = cluster.start_workers(workers)
            info = client.scheduler_info()
            logger.debug("client scheduler info {}".format(info))
            if os.path.isfile(scheduler_file):
                setup_jlab(client, jlab_port=str(notebook_port), dash_port=str(dashboard_port),
                           notebook_dir=notebookdir,
                           hostname="cheyenne.ucar.edu",
                           scheduler_file=scheduler_file)
            else:
                logger.warning("No scheduler_file found")

if __name__ == "__main__":
    logger = get_logger("DEBUG")
    _main_func(sys.argv[1:], __doc__)
