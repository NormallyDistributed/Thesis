import argparse
from itertools import repeat
import json
from multiprocessing import set_start_method, Lock, Pool
from typing import List, Sequence, Tuple

import torch

from args import train_argparser, eval_argparser
from config_reader import process_configs
from create_run_configs import split_gs_config
from spert import input_reader
from spert.spert_trainer import SpERTTrainer


def init_pool(l):
    global lock
    lock = l


def batch_sequence(seq: Sequence, batch_size: int) -> Tuple:
    return tuple(seq[pos: pos + batch_size] for pos in range(0, len(seq), batch_size))


def get_balanced_devices(count: int,
                         no_cuda: bool = False) -> List[str]:
    if not no_cuda and torch.cuda.is_available():
        devices = [f'cuda:{id_}' for id_ in range(torch.cuda.device_count())]
    else:
        devices = ['cpu']
    factor = int(count / len(devices))
    remainder = count % len(devices)
    devices = devices * factor + devices[:remainder]
    return devices


def __train(run_cfg, device):

    device_id = int(device.split(':')[-1])

    arg_parser = train_argparser()
    args, _ = arg_parser.parse_known_args()
    old_args = args.__dict__
    run_args = argparse.Namespace(**{**old_args, **run_cfg, **{'device': device_id}})
    trainer = SpERTTrainer(run_args)
    trainer.train(train_path=run_args.train_path, valid_path=run_args.valid_path,
                  types_path=run_args.types_path, input_reader_cls=input_reader.JsonInputReader)


def _train():
    gs_config = json.load(open('configs/train_cfg_new.json', 'r'))
    run_configs = split_gs_config(gs_config)
    num_workers = 4

    set_start_method('spawn', force=True)
    for batch_run_configs in batch_sequence(run_configs, batch_size=num_workers):
        devices = get_balanced_devices(count=len(batch_run_configs),
                                       no_cuda=False)

        with Pool(initializer=init_pool, initargs=(Lock(),), processes=len(batch_run_configs)) as pool:
            pool.starmap(__train, zip(batch_run_configs,
                                      devices))

    # process_configs(target=__train, arg_parser=arg_parser)


def __eval(run_args):
    trainer = SpERTTrainer(run_args)
    trainer.eval(dataset_path=run_args.dataset_path, types_path=run_args.types_path,
                 input_reader_cls=input_reader.JsonInputReader)


def _eval():
    arg_parser = eval_argparser()
    process_configs(target=__eval, arg_parser=arg_parser)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(add_help=False)
    arg_parser.add_argument('mode', type=str, help="Mode: 'train' or 'eval'")
    args, _ = arg_parser.parse_known_args()

    # single_run_configs = split_gs_config(config)
    #
    # set_start_method('spawn', force=True)
    # for batch_run_configs in tqdm(batch_sequence(single_run_configs, batch_size=args.num_processes)):
    #     devices = get_balanced_devices(count=len(batch_run_configs),
    #                                    no_cuda=args.no_cuda)
    #
    #     with Pool(initializer=init_pool, initargs=(Lock(),), processes=len(batch_run_configs)) as pool:
    #         pool.starmap(run_task, zip(batch_run_configs,
    #                                    devices,
    #                                    repeat(args.task),
    #                                    repeat(args.force),
    #                                    repeat(args.warm_start)))

    if args.mode == 'train':
        _train()
    elif args.mode == 'eval':
        _eval()
    else:
        raise Exception("Mode not in ['train', 'eval'], e.g. 'python spert.py train ...'")
