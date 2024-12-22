import asyncio
import importlib.util
import logging
import os
import sys

from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager

worker = None
module_name = ""


@contextmanager
def setup_logger(document_id, file_path, log_path, level=logging.ERROR):
    """Context manager to set up a logger for the given document and file path."""
    logger = logging.getLogger(f"{document_id} {file_path}")
    logger.setLevel(logging.INFO)
    logger.propagate = True
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        handlers = [console_handler, file_handler]
    else:
        handlers = logger.handlers
    try:
        yield logger
    finally:
        for handler in handlers:
            handler.close()
            logger.removeHandler(handler)


def module_initializer(module):
    global worker, module_name

    module_name = module.NAME

    if not os.path.exists(module.PATH):
        raise FileNotFoundError(f"Module file not found: {module.PATH}")

    # Get the directory of the module file
    module_dir = os.path.dirname(os.path.abspath(module.PATH))

    # Temporarily add the module directory to sys.path
    original_sys_path = sys.path[:]
    sys.path.insert(0, module_dir)

    try:
        spec = importlib.util.spec_from_file_location("worker_module", module.PATH)
        worker = importlib.util.module_from_spec(spec)
        sys.modules["worker_module"] = worker
        spec.loader.exec_module(worker)
    finally:
        # Restore the original sys.path
        sys.path = original_sys_path


def process_task(args):
    fp, doc, spath, mtime = args
    try:
        logging.info(f'{module_name} "{fp}" run task on process from pool')
        with setup_logger(doc["id"], fp, spath / "log.txt") as _logger:
            result = worker.main(fp, doc, spath, mtime, _logger)
        logging.debug(f'{module_name} "{fp}" done')
        return doc, result, {"success": 1}, fp
    except Exception as e:
        logging.exception(f'{module_name} "{fp}" failed')
        return doc, None, {"failure": 1}, fp


async def process_tasks(module, arg_list, cancel_event):
    executor = ProcessPoolExecutor(
        max_workers=module.MAX_WORKERS,
        initializer=module_initializer,
        initargs=(module,),
    )

    try:
        loop = asyncio.get_running_loop()

        tasks = []
        for args in arg_list:
            fp, doc, spath, mtime = args
            logging.debug(f'{module.NAME} "{fp}" start')
            tasks.append(loop.run_in_executor(executor, process_task, args))
        stats = {"success": 0, "failure": 0, "cancelled": 0}
        is_cancelled = False
        for future in asyncio.as_completed(tasks):
            try:
                pdoc, cdoc, stat, fp = await future
                logging.info(f'{module.NAME} "{fp}" process completed task {stat}')
                for key in stats:
                    stats[key] += stat.get(key, 0)
                if cdoc:
                    yield pdoc, cdoc, fp
                if cancel_event.is_set():
                    logging.info(
                        f"{module.NAME} finish pending tasks, then shutdown..."
                    )
                    is_cancelled = True
                    break
            except Exception:
                stats["failure"] += 1
        if is_cancelled:
            for task in tasks:
                task.cancel()
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                stats["cancelled"] += 1
    finally:
        executor.shutdown()
    logging.info("------------------------------------------")
    logging.info(f"{module.NAME} shutdown summary:")
    logging.info(f"  {stats['success']} succeeded")
    logging.info(f"  {stats['failure']} failed")
    logging.info(f"  {stats['cancelled']} postponed ")
    logging.info("------------------------------------------")
