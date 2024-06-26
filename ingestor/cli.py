from enum import Enum
import os
import click
import datetime
from pathlib import Path

from deltalake import DeltaTable, write_deltalake
import typer
import ray

from .utils import any2date
from .tq_utils import get_tq_api, _get_cont_tick_of_range

    
DATA_HOME = os.getenv('DATA_HOME', '~/ingestor_data')

    # delta_name = 'xxx'
    # write_deltalake(
    #     delta_name, arrow_table,
    #     schema=Schema.from_pyarrow(schema),
    #     mode='append',
    #     # schema_mode='overwrite',
    # )

class Mode(str, Enum):
    create = 'create'
    append = 'append'

class IfExists(str, Enum):
    skip = 'skip'
    overwrite = 'overwrite'

app = typer.Typer()

@app.command()
def download_cont_tick(
    symbol: str = 'all',
    start: datetime.date = typer.Option("2020-01-01", parser=any2date),
    end: datetime.date = typer.Option("today", parser=any2date),
    delta_path: Path = typer.Option('/Users/wangxin/SynologyDrive/DeltaLake', exists=False),
    mode: Mode = typer.Option(Mode.create, help='create or append mode'),
    if_exists: IfExists = typer.Option(IfExists.skip, help='skip or overwrite if exists'),
):
    if symbol == 'all':
        with get_tq_api() as api:
            symbols = api.query_quotes(ins_class='CONT')
    else:
        symbols = [symbol]

    title = f"Total tasks: {len(symbols)}"
    click.echo(f"{title:=^50}")
    
    @ray.remote(max_retries=2, num_cpus=1)
    def _run_single(symbol):
        tag = symbol.split('@')[1] + '.tick'
        uri = os.path.join(delta_path, tag)
        partition_cols = ['symbol']
        click.echo(f"==> symbol: {tag}, start: {start}, end: {end}, mode: {mode}, uri: {uri}")
        if mode == 'create':
            if if_exists == 'skip' and os.path.exists(uri):
                click.echo(f"==> {uri} exists, skip.")
                return
            df = _get_cont_tick_of_range(symbol, start, end)
            write_deltalake(uri, df.to_arrow(), partition_by=partition_cols, mode='error')
        elif mode == 'append':
            # table = DeltaTable(uri)
            # table.write(df.to_arrow(), mode='append')
            ...
            
    ray.init(num_cpus=5, num_gpus=1, ignore_reinit_error=True)
    MAX_QUEUE_SIZE = 5
    task_ids = []
    queue_results = []
    for _symbol in symbols:
        task_ids.append(_run_single.remote(_symbol))
        if len(task_ids) >= MAX_QUEUE_SIZE:
            done_ids, task_ids = ray.wait(task_ids, num_returns=1)
            queue_results.extend(ray.get(done_ids))
    queue_results.extend(ray.get(task_ids))
    

@app.command()
def download_cont_tick_csv(
    symbol: str = 'all',
    start: datetime.date = typer.Option("2020-01-01", parser=any2date),
    end: datetime.date = typer.Option("today", parser=any2date),
    data_path: Path = typer.Option('/Users/wangxin/SynologyDrive/DataLake', exists=False),
    # mode: Mode = typer.Option(Mode.create, help='create or append mode'),
    # if_exists: IfExists = typer.Option(IfExists.skip, help='skip or overwrite if exists'),
):
    if symbol == 'all':
        with get_tq_api() as api:
            symbols = api.query_quotes(ins_class='CONT')
    else:
        symbols = [symbol]

    title = f"Total tasks: {len(symbols)}"
    click.echo(f"{title:=^50}")
    
    @ray.remote(max_retries=2, num_cpus=1)
    def _run_single(symbol):
        tag = symbol.split('@')[1] + '.tick'
        uri = os.path.join(data_path, tag)
        # partition_cols = ['symbol']
        # click.echo(f"==> symbol: {tag}, start: {start}, end: {end}, mode: {mode}, uri: {uri}")
        _get_cont_tick_of_range(symbol, start, end, dir=uri)
        # if mode == 'create':
        #     if if_exists == 'skip' and os.path.exists(uri):
        #         click.echo(f"==> {uri} exists, skip.")
        #         return
        #     df = _get_cont_tick_of_range(symbol, start, end)
        #     write_deltalake(uri, df.to_arrow(), partition_by=partition_cols, mode='error')
        # elif mode == 'append':
        #     # table = DeltaTable(uri)
        #     # table.write(df.to_arrow(), mode='append')
        #     ...
            
    ray.init(num_cpus=1, num_gpus=0, ignore_reinit_error=True)
    MAX_QUEUE_SIZE = 1
    task_ids = []
    queue_results = []
    for _symbol in symbols:
        task_ids.append(_run_single.remote(_symbol))
        if len(task_ids) >= MAX_QUEUE_SIZE:
            done_ids, task_ids = ray.wait(task_ids, num_returns=1)
            queue_results.extend(ray.get(done_ids))
    queue_results.extend(ray.get(task_ids))
    
@app.command()
def list_all_conts():
    "List all continuous contracts."
    with get_tq_api() as api:
        conts = api.query_quotes(ins_class='CONT')
    print(conts)

if __name__ == '__main__':
    app()