import atexit
import os

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"
os.environ[
    "PYTORCH_ENABLE_MPS_FALLBACK"
] = "1"  # Transformers uses .isin for a simple op, which is not supported on MPS
os.environ["IN_STREAMLIT"] = "true"  # Avoid multiprocessing inside surya


import argparse
import gc
import glob
import hashlib
import math
import traceback

import magic
import torch.multiprocessing as mp
from marker.config.parser import ConfigParser
from marker.config.printer import CustomClickPrinter
from marker.logger import configure_logging
from marker.models import create_model_dict
from marker.output import output_exists, save_output
from marker.settings import settings
from tqdm import tqdm

configure_logging()


def is_pdf(filepath):
    mime = magic.Magic(mime=True)
    return mime.from_file(filepath) == "application/pdf"


def worker_init(model_dict):
    if model_dict is None:
        model_dict = create_model_dict()

    global model_refs
    model_refs = model_dict

    # Ensure we clean up the model references on exit
    atexit.register(worker_exit)


def worker_exit():
    global model_refs
    try:
        del model_refs
    except Exception:
        pass


def process_single_pdf(fpath):
    config = {
        "output_format": "markdown",
        "disable_image_extraction": True,
        "disable_links": True,
        "disable_multiprocessing": True,
    }
    config_parser = ConfigParser(config)

    converter_cls = config_parser.get_converter_cls()
    config_dict = config_parser.generate_config_dict()
    config_dict["disable_tqdm"] = True

    try:
        converter = converter_cls(
            config=config_dict,
            artifact_dict=model_refs,
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer(),
            llm_service=config_parser.get_llm_service(),
        )
        rendered = converter(fpath)
        markdown = rendered.markdown

        del rendered
        del converter
    except Exception as e:
        print(f"Error converting {fpath}: {e}")
        print(traceback.format_exc())
        markdown = None
    finally:
        gc.collect()

    return markdown, fpath


def get_output_path(fpath, output_dir):
    base_name = os.path.basename(fpath)
    id = os.path.splitext(base_name)[0]
    return os.path.join(output_dir, id[:2], f"{id}.md")


def output_exists(fpath, output_dir):
    return os.path.exists(get_output_path(fpath, output_dir))


def get_slice(f, num_slices):
    return int(hashlib.sha256(f.encode("utf-8")).hexdigest(), 16) % num_slices


def main(
    input_glob,
    output_dir,
    slice_idx,
    num_slices,
    device="cuda:0",
    num_workers=5,
    **kwargs,
):
    os.makedirs(output_dir, exist_ok=True)
    files_to_convert = filter(
        lambda f: get_slice(os.path.basename(f), num_slices) == slice_idx
        and is_pdf(f)
        and not output_exists(f, output_dir),
        glob.glob(input_glob),
    )

    try:
        mp.set_start_method("spawn")  # Required for CUDA, forkserver doesn't work
    except RuntimeError:
        raise RuntimeError(
            "Set start method to spawn twice. This may be a temporary issue with the script. Please try running it again."
        )

    if settings.TORCH_DEVICE == "mps" or settings.TORCH_DEVICE_MODEL == "mps":
        model_dict = None
    else:
        model_dict = create_model_dict(device=device)
        for k, v in model_dict.items():
            v.model.share_memory()

    print(f"Converting with {num_workers} processes and saving to {output_dir}")
    total_bytes_written = 0
    num_errors = 0
    with mp.Pool(
        processes=num_workers,
        initializer=worker_init,
        initargs=(model_dict,),
        maxtasksperchild=10,
    ) as pool:
        pbar = tqdm(desc="Processing PDFs", unit=" files")
        for markdown, fpath in pool.imap_unordered(
            process_single_pdf, files_to_convert
        ):
            if markdown is None:
                pbar.update(1)
                num_errors += 1
                continue

            total_bytes_written += len(markdown)
            output_path = get_output_path(fpath, output_dir)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(markdown)
            pbar.update(1)

            pbar.set_postfix(
                {"Errors": num_errors, "MB Written": f"{total_bytes_written/1e6:.3E}"}
            )

        pbar.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-glob", required=True, help="Directory containing PDF files"
    )
    parser.add_argument(
        "--output-directory", required=True, help="Directory to save markdown files"
    )
    parser.add_argument(
        "--device",
        default="cuda:0",
        help="Device to run the marker model on (e.g., 'cuda:0' or 'cpu')",
    )
    parser.add_argument("--num-workers", type=int, default=5)
    parser.add_argument("--slice-idx", type=int, default=0)
    parser.add_argument("--num-slices", type=int, default=5)
    args = parser.parse_args()

    main(
        args.input_glob,
        args.output_directory,
        args.slice_idx,
        args.num_slices,
        device=args.device,
        num_workers=args.num_workers,
    )
