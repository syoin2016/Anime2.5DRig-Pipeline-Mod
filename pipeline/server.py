from __future__ import annotations

import datetime as dt
import html
import json
import mimetypes
import os
import random
import shutil
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "pipeline"
CONFIG_PATH = PIPELINE / "config.json"
JOBS_DIR = PIPELINE / "jobs"
LATEST_DIR = PIPELINE / "latest"
LOGS_DIR = PIPELINE / "logs"
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}


def now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_config() -> dict[str, Any]:
    cfg = read_json(CONFIG_PATH, {})
    cfg.setdefault("host", "127.0.0.1")
    cfg.setdefault("port", 8765)
    cfg.setdefault("comfy_url", "http://127.0.0.1:8189")
    cfg.setdefault("job_timeout_minutes", 90)
    return cfg


CONFIG = load_config()
JOBS: dict[str, dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()
COMFY_PROCESS: subprocess.Popen[Any] | None = None
COMFY_STARTED_BY_PIPELINE = False


def job_path(job_id: str) -> Path:
    return JOBS_DIR / job_id / "job.json"


def save_job(job: dict[str, Any]) -> None:
    write_json(job_path(job["id"]), job)
    with JOBS_LOCK:
        JOBS[job["id"]] = job


def load_jobs() -> None:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    for path in JOBS_DIR.glob("*/job.json"):
        try:
            job = read_json(path)
            if isinstance(job, dict) and "id" in job:
                JOBS[job["id"]] = job
        except Exception:
            continue


def append_log(job: dict[str, Any], message: str) -> None:
    line = f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}"
    job.setdefault("log", []).append(line)
    job["updated_at"] = now_iso()
    save_job(job)


def http_get_json(url: str, timeout: int = 10) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_post_json(url: str, payload: dict[str, Any], timeout: int = 30) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        return json.loads(body.decode("utf-8")) if body else {}


def comfy_online() -> bool:
    try:
        http_get_json(CONFIG["comfy_url"].rstrip("/") + "/system_stats", timeout=3)
        return True
    except Exception:
        return False


def ensure_comfy(job: dict[str, Any]) -> None:
    global COMFY_PROCESS, COMFY_STARTED_BY_PIPELINE
    if comfy_online():
        append_log(job, "ComfyUI API は起動済みです")
        COMFY_STARTED_BY_PIPELINE = False
        return

    comfy_root = Path(CONFIG["comfy_root"])
    comfy_python = Path(CONFIG["comfy_python"])
    if not comfy_root.exists():
        raise RuntimeError(f"ComfyUI root が見つかりません: {comfy_root}")
    if not comfy_python.exists():
        raise RuntimeError(f"ComfyUI Python が見つかりません: {comfy_python}")

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = (LOGS_DIR / f"comfyui_{stamp}.out.log").open("ab")
    err = (LOGS_DIR / f"comfyui_{stamp}.err.log").open("ab")
    cmd = [
        str(comfy_python),
        "main.py",
        "--listen",
        "127.0.0.1",
        "--port",
        "8189",
    ]
    flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    append_log(job, "ComfyUI を自動起動しています")
    COMFY_PROCESS = subprocess.Popen(cmd, cwd=str(comfy_root), stdout=out, stderr=err, creationflags=flags)
    COMFY_STARTED_BY_PIPELINE = True

    deadline = time.time() + 180
    while time.time() < deadline:
        if comfy_online():
            append_log(job, "ComfyUI API の準備ができました")
            return
        if COMFY_PROCESS.poll() is not None:
            raise RuntimeError(f"ComfyUI が終了しました。ログ: {out.name}, {err.name}")
        time.sleep(3)
    raise TimeoutError("ComfyUI の起動待ちがタイムアウトしました")


def release_comfy_for_see_through(job: dict[str, Any]) -> None:
    global COMFY_PROCESS, COMFY_STARTED_BY_PIPELINE
    try:
        http_post_json(
            CONFIG["comfy_url"].rstrip() + "/free",
            {"unload_models": True, "free_memory": True},
            timeout=10,
        )
        append_log(job, "ComfyUI のモデル解放を要求しました")
    except Exception as exc:
        append_log(job, "ComfyUI のモデル解放要求をスキップしました: " + str(exc))

    if not CONFIG.get("stop_owned_comfy_before_see_through", True):
        return
    if not COMFY_STARTED_BY_PIPELINE or COMFY_PROCESS is None:
        if comfy_online():
            append_log(job, "ComfyUIは外部起動のため停止しません")
        return
    if COMFY_PROCESS.poll() is not None:
        COMFY_PROCESS = None
        COMFY_STARTED_BY_PIPELINE = False
        return

    append_log(job, "GPUメモリ確保のため、自動起動したComfyUIを停止します")
    COMFY_PROCESS.terminate()
    try:
        COMFY_PROCESS.wait(timeout=20)
    except subprocess.TimeoutExpired:
        COMFY_PROCESS.kill()
        COMFY_PROCESS.wait(timeout=10)
    COMFY_PROCESS = None
    COMFY_STARTED_BY_PIPELINE = False
    time.sleep(4)


def build_txt2img_workflow(params: dict[str, Any]) -> tuple[dict[str, Any], int]:
    seed = int(params.get("seed") or 0)
    if seed <= 0:
        seed = random.randint(1, 2**32 - 1)
    model = params.get("model") or CONFIG.get("default_model")
    width = int(params.get("width") or CONFIG.get("default_width", 768))
    height = int(params.get("height") or CONFIG.get("default_height", 768))
    steps = int(params.get("steps") or CONFIG.get("default_steps", 10))
    cfg = float(params.get("cfg") or CONFIG.get("default_cfg", 7.0))
    sampler = params.get("sampler") or CONFIG.get("default_sampler", "euler_ancestral")
    scheduler = params.get("scheduler") or CONFIG.get("default_scheduler", "normal")
    prompt = params.get("prompt") or "anime character, full body, clean front view, layered character design, white background"
    negative = params.get("negative_prompt") or "lowres, blurry, cropped, multiple people, bad hands, text, watermark"
    prefix = params.get("filename_prefix") or f"a25d_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if str(model).lower() == str((CONFIG.get("anima_base") or {}).get("unet", "anima-base-v1.0.safetensors")).lower():
        return build_anima_base_workflow(params, seed, prefix)
    workflow = {
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "seed": seed,
                "steps": steps,
                "cfg": cfg,
                "sampler_name": sampler,
                "scheduler": scheduler,
                "denoise": 1.0,
                "model": ["4", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["5", 0],
            },
        },
        "4": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": model}},
        "5": {"class_type": "EmptyLatentImage", "inputs": {"width": width, "height": height, "batch_size": 1}},
        "6": {"class_type": "CLIPTextEncode", "inputs": {"text": prompt, "clip": ["4", 1]}},
        "7": {"class_type": "CLIPTextEncode", "inputs": {"text": negative, "clip": ["4", 1]}},
        "8": {"class_type": "VAEDecode", "inputs": {"samples": ["3", 0], "vae": ["4", 2]}},
        "9": {"class_type": "SaveImage", "inputs": {"filename_prefix": prefix, "images": ["8", 0]}},
    }
    return workflow, seed


def build_anima_base_workflow(params: dict[str, Any], seed: int, prefix: str) -> tuple[dict[str, Any], int]:
    cfg = CONFIG.get("anima_base") or {}
    width = int(params.get("width") or cfg.get("width", 1024))
    height = int(params.get("height") or cfg.get("height", 1024))
    steps = int(params.get("steps") or cfg.get("steps", 30))
    cfg_scale = float(params.get("cfg") or cfg.get("cfg", 4.0))
    sampler = params.get("sampler") or cfg.get("sampler_name", "er_sde")
    scheduler = params.get("scheduler") or cfg.get("scheduler", "simple")
    prompt = params.get("prompt") or (
        "anime illustration of one young adult woman, solo, full body, front view, standing pose, "
        "arms slightly away from body, clean separated hair shapes, clear eyes, visible mouth, "
        "simple white background, centered character, clean line art, best quality"
    )
    prefix_text = cfg.get("prompt_prefix", "")
    if prefix_text and not str(prompt).startswith(prefix_text):
        prompt = prefix_text + str(prompt)
    negative = params.get("negative_prompt") or (
        "worst quality, low quality, blurry, cropped, multiple people, text, watermark, logo, "
        "signature, bad anatomy, extra limbs, hidden hands, occluded face, complex background"
    )
    workflow = {
        "10": {
            "class_type": "UNETLoader",
            "inputs": {
                "unet_name": cfg.get("unet", "anima-base-v1.0.safetensors"),
                "weight_dtype": "default",
            },
        },
        "20": {
            "class_type": "CLIPLoader",
            "inputs": {
                "clip_name": cfg.get("clip", "qwen_3_06b_base.safetensors"),
                "type": "stable_diffusion",
            },
        },
        "30": {"class_type": "CLIPTextEncode", "inputs": {"clip": ["20", 0], "text": prompt}},
        "31": {"class_type": "CLIPTextEncode", "inputs": {"clip": ["20", 0], "text": negative}},
        "40": {"class_type": "EmptyLatentImage", "inputs": {"width": width, "height": height, "batch_size": 1}},
        "60": {"class_type": "VAELoader", "inputs": {"vae_name": cfg.get("vae", "qwen_image_vae.safetensors")}},
        "18": {
            "class_type": "ModelSamplingAuraFlow",
            "inputs": {"model": ["10", 0], "shift": float(cfg.get("shift", 3.0))},
        },
        "50": {
            "class_type": "KSampler",
            "inputs": {
                "model": ["18", 0],
                "positive": ["30", 0],
                "negative": ["31", 0],
                "latent_image": ["40", 0],
                "seed": seed,
                "steps": steps,
                "cfg": cfg_scale,
                "sampler_name": sampler,
                "scheduler": scheduler,
                "denoise": 1.0,
            },
        },
        "70": {"class_type": "VAEDecode", "inputs": {"samples": ["50", 0], "vae": ["60", 0]}},
        "80": {"class_type": "SaveImage", "inputs": {"filename_prefix": prefix, "images": ["70", 0]}},
    }
    return workflow, seed


def wait_for_history(prompt_id: str, timeout_seconds: int) -> dict[str, Any]:
    base = CONFIG["comfy_url"].rstrip("/")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        history = http_get_json(f"{base}/history/{prompt_id}", timeout=30)
        if prompt_id in history:
            item = history[prompt_id]
            status = item.get("status") or {}
            if status.get("status_str") == "error":
                raise RuntimeError(json.dumps(status, ensure_ascii=False))
            return item
        time.sleep(3)
    raise TimeoutError(f"ComfyUI 生成待ちがタイムアウトしました: {prompt_id}")


def collect_first_image(history: dict[str, Any]) -> dict[str, Any]:
    outputs = history.get("outputs") or {}
    for output in outputs.values():
        if not isinstance(output, dict):
            continue
        for item in output.get("images") or []:
            if isinstance(item, dict) and item.get("filename"):
                return item
    raise RuntimeError("ComfyUI の履歴に画像出力がありません")


def copy_comfy_image(item: dict[str, Any], dest: Path) -> Path:
    output_root = Path(CONFIG["comfy_root"]) / "output"
    subfolder = item.get("subfolder") or ""
    src = (output_root / subfolder / item["filename"]).resolve()
    if not src.exists():
        # Fallback through /view when the output root is virtual or symlinked oddly.
        query = urllib.parse.urlencode(
            {
                "filename": item["filename"],
                "subfolder": subfolder,
                "type": item.get("type") or "output",
            }
        )
        with urllib.request.urlopen(CONFIG["comfy_url"].rstrip("/") + "/view?" + query, timeout=60) as resp:
            dest.write_bytes(resp.read())
        return dest
    shutil.copy2(src, dest)
    return dest


def eagle_library_root() -> Path:
    return Path(CONFIG.get("eagle_library", "")).resolve()


def find_eagle_source_file(info_dir: Path, meta: dict[str, Any]) -> Path | None:
    ext = "." + str(meta.get("ext") or "").lower().lstrip(".")
    name = str(meta.get("name") or "")
    preferred = info_dir / (name + ext)
    if ext in IMAGE_EXTS and preferred.exists() and "_thumbnail" not in preferred.name.lower():
        return preferred
    files = []
    for path in info_dir.iterdir():
        if path.is_file() and path.suffix.lower() in IMAGE_EXTS and "_thumbnail" not in path.name.lower():
            files.append(path)
    if not files:
        return None
    files.sort(key=lambda p: p.stat().st_size, reverse=True)
    return files[0]


def read_eagle_record(info_dir: Path) -> dict[str, Any] | None:
    meta_path = info_dir / "metadata.json"
    if not meta_path.exists():
        return None
    try:
        meta = read_json(meta_path, {})
        if not isinstance(meta, dict) or meta.get("isDeleted"):
            return None
        source = find_eagle_source_file(info_dir, meta)
        if not source:
            return None
        record_id = str(meta.get("id") or info_dir.name.replace(".info", ""))
        tags = [str(t) for t in (meta.get("tags") or [])]
        title = str(meta.get("name") or source.stem)
        text = " ".join([title, str(meta.get("annotation") or ""), *tags]).lower()
        score = 0
        for term, points in (
            ("full-body", 8),
            ("full body", 8),
            ("全身", 8),
            ("standing", 5),
            ("立ち", 5),
            ("front", 4),
            ("正面", 4),
            ("1girl", 4),
            ("anime", 3),
            ("anima", 3),
            ("white background", 3),
            ("simple", 2),
        ):
            if term in text:
                score += points
        for term, points in (
            ("medium shot", -5),
            ("ミディアム", -5),
            ("waist", -5),
            ("ウエスト", -5),
            ("cropped", -8),
            ("複数", -8),
        ):
            if term in text:
                score += points
        return {
            "id": record_id,
            "title": title,
            "width": int(meta.get("width") or 0),
            "height": int(meta.get("height") or 0),
            "tags": tags[:24],
            "score": score,
            "mtime": int(meta.get("mtime") or meta.get("modificationTime") or source.stat().st_mtime),
            "size": source.stat().st_size,
            "url": f"/api/eagle/file?id={urllib.parse.quote(record_id)}",
            "_path": str(source),
        }
    except Exception:
        return None


def list_eagle_images(limit: int = 80) -> list[dict[str, Any]]:
    root = eagle_library_root()
    images_dir = root / "images"
    if not images_dir.exists():
        return []
    records = []
    for info_dir in images_dir.glob("*.info"):
        if not info_dir.is_dir():
            continue
        rec = read_eagle_record(info_dir)
        if rec:
            records.append(rec)
    records.sort(key=lambda r: (r.get("score", 0), r.get("mtime", 0), r.get("size", 0)), reverse=True)
    clean = []
    for rec in records[:limit]:
        item = dict(rec)
        item.pop("_path", None)
        clean.append(item)
    return clean


def get_eagle_record(record_id: str) -> dict[str, Any]:
    root = eagle_library_root()
    images_dir = root / "images"
    safe_id = "".join(ch for ch in record_id if ch.isalnum() or ch in "_-")
    candidates = [images_dir / f"{safe_id}.info"]
    for path in images_dir.glob("*.info"):
        if path.name == f"{safe_id}.info":
            continue
        candidates.append(path)
    for info_dir in candidates:
        if not info_dir.exists() or not info_dir.is_dir():
            continue
        rec = read_eagle_record(info_dir)
        if rec and rec["id"] == record_id:
            src = Path(rec["_path"]).resolve()
            if not str(src).lower().startswith(str(root).lower()):
                raise RuntimeError("Eagleライブラリ外の画像は使用できません")
            return rec
    raise FileNotFoundError(f"Eagle画像が見つかりません: {record_id}")


def run_see_through(job: dict[str, Any], source: Path) -> Path:
    see_root = Path(CONFIG["see_through_root"])
    see_python = Path(CONFIG["see_through_python"])
    if not see_root.exists():
        raise RuntimeError(f"see-through root が見つかりません: {see_root}")
    if not see_python.exists():
        raise RuntimeError(f"see-through Python が見つかりません: {see_python}")

    job_dir = JOBS_DIR / job["id"]
    save_dir = job_dir / "see_through"
    save_dir.mkdir(parents=True, exist_ok=True)
    res = int(job["params"].get("resolution") or CONFIG.get("default_resolution", 1024))
    depth_res = int(job["params"].get("resolution_depth") or CONFIG.get("default_resolution_depth", 720))
    cmd = [
        str(see_python),
        "inference\\scripts\\inference_psd.py",
        "--srcp",
        str(source),
        "--save_to_psd",
        "--resolution",
        str(res),
        "--resolution_depth",
        str(depth_res),
        "--save_dir",
        str(save_dir),
    ]
    append_log(job, "see-through でPSD分解を開始します")
    proc = subprocess.Popen(
        cmd,
        cwd=str(see_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    output_log = job_dir / "see_through.log"
    with output_log.open("w", encoding="utf-8") as handle:
        for line in proc.stdout:
            handle.write(line)
            handle.flush()
            text = line.strip()
            if text and ("psd saved" in text.lower() or text.startswith("running ")):
                append_log(job, text)
    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"see-through が失敗しました。ログ: {output_log}")

    psds = sorted(save_dir.glob("*.psd"), key=lambda p: p.stat().st_mtime, reverse=True)
    psds = [p for p in psds if not p.name.endswith("_depth.psd")]
    if not psds:
        raise RuntimeError(f"PSD出力が見つかりません: {save_dir}")
    model_psd = job_dir / "model.psd"
    shutil.copy2(psds[0], model_psd)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(model_psd, LATEST_DIR / "latest.psd")
    return model_psd


def finish_psd_job(job: dict[str, Any], source: Path) -> None:
    model_psd = run_see_through(job, source)
    job["model_psd"] = rel_url(model_psd)
    job["latest_psd"] = "/pipeline/latest/latest.psd"
    job["anime_url"] = "/index.html?psd=/pipeline/latest/latest.psd"
    job["status"] = "done"
    job["finished_at"] = now_iso()
    append_log(job, "完了しました。Anime2.5DRigで調整できます")
    save_job(job)


def run_job(job_id: str) -> None:
    with JOBS_LOCK:
        job = JOBS[job_id]
    try:
        job["status"] = "running"
        job["started_at"] = now_iso()
        save_job(job)
        append_log(job, "ジョブを開始しました")

        ensure_comfy(job)
        job_dir = JOBS_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        workflow, seed = build_txt2img_workflow(job["params"])
        job["params"]["seed"] = seed
        write_json(job_dir / "workflow.json", workflow)
        append_log(job, f"ComfyUI に画像生成を投入します seed={seed}")
        queued = http_post_json(CONFIG["comfy_url"].rstrip("/") + "/prompt", {"prompt": workflow, "client_id": job_id})
        prompt_id = queued.get("prompt_id")
        if not prompt_id:
            raise RuntimeError(f"ComfyUI が prompt_id を返しませんでした: {queued}")
        job["comfy_prompt_id"] = prompt_id
        save_job(job)

        timeout = int(CONFIG.get("job_timeout_minutes", 90)) * 60
        history = wait_for_history(prompt_id, timeout)
        write_json(job_dir / "comfy_history.json", history)
        image_item = collect_first_image(history)
        source = copy_comfy_image(image_item, job_dir / "source.png")
        job["source_image"] = rel_url(source)
        append_log(job, f"生成画像を保存しました: {source.name}")

        release_comfy_for_see_through(job)
        finish_psd_job(job, source)
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["traceback"] = traceback.format_exc()
        append_log(job, "エラー: " + str(exc))
        save_job(job)


def rel_url(path: Path) -> str:
    try:
        rel = path.resolve().relative_to(ROOT.resolve())
    except ValueError:
        return str(path)
    return "/" + rel.as_posix()


def create_job(params: dict[str, Any]) -> dict[str, Any]:
    job_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    job = {
        "id": job_id,
        "status": "queued",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "params": params,
        "log": [],
    }
    save_job(job)
    thread = threading.Thread(target=run_job, args=(job_id,), daemon=True)
    thread.start()
    return job


def run_eagle_job(job_id: str) -> None:
    with JOBS_LOCK:
        job = JOBS[job_id]
    try:
        job["status"] = "running"
        job["started_at"] = now_iso()
        save_job(job)
        append_log(job, "Eagle画像ジョブを開始しました")
        rec = get_eagle_record(str(job["params"].get("image_id") or ""))
        src = Path(rec["_path"])
        job_dir = JOBS_DIR / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        dest = job_dir / ("source" + src.suffix.lower())
        shutil.copy2(src, dest)
        job["source_image"] = rel_url(dest)
        job["eagle"] = {k: v for k, v in rec.items() if not k.startswith("_")}
        append_log(job, "Eagleから画像をコピーしました: " + rec["title"])
        release_comfy_for_see_through(job)
        finish_psd_job(job, dest)
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        job["traceback"] = traceback.format_exc()
        append_log(job, "エラー: " + str(exc))
        save_job(job)


def create_eagle_job(params: dict[str, Any]) -> dict[str, Any]:
    job_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    job = {
        "id": job_id,
        "status": "queued",
        "kind": "eagle",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "params": params,
        "log": [],
    }
    save_job(job)
    thread = threading.Thread(target=run_eagle_job, args=(job_id,), daemon=True)
    thread.start()
    return job


def latest_jobs(limit: int = 20) -> list[dict[str, Any]]:
    with JOBS_LOCK:
        jobs = list(JOBS.values())
    jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
    return jobs[:limit]


def page_html() -> bytes:
    models = CONFIG.get("model_options") or [CONFIG.get("default_model", "")]
    model_options = "\n".join(
        f'<option value="{html.escape(str(m))}" {"selected" if m == CONFIG.get("default_model") else ""}>{html.escape(str(m))}</option>'
        for m in models
    )
    body = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Anime2.5DRig Pipeline</title>
<style>
:root{{--ink:#181714;--paper:#f7f3ea;--panel:#fffaf0;--line:#d9cdb9;--accent:#b03226;--blue:#2f5f73;--muted:#756f66;--ok:#2c7a4b;--bad:#a33131}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--paper);color:var(--ink);font-family:"Yu Gothic UI","Hiragino Sans",system-ui,sans-serif;min-height:100vh}}
header{{padding:18px 28px;border-bottom:1px solid var(--line);display:flex;align-items:flex-end;gap:18px;flex-wrap:wrap;background:#fbf8f0}}
h1{{font-family:Georgia,"Times New Roman",serif;font-size:25px;line-height:1;margin:0;color:var(--accent);font-weight:700}}
header p{{margin:0;color:var(--muted);font-size:13px}} main{{display:grid;grid-template-columns:minmax(320px,520px) 1fr;gap:20px;padding:22px;max-width:1280px;margin:0 auto}}
.stack{{display:flex;flex-direction:column;gap:20px}}
section{{border:1px solid var(--line);background:var(--panel);border-radius:8px;padding:16px}} h2{{font-size:13px;margin:0 0 12px;color:var(--blue);letter-spacing:.04em}}
label{{display:block;font-size:12px;color:var(--muted);margin:10px 0 5px}} textarea,input,select{{width:100%;border:1px solid var(--line);border-radius:6px;background:#fff;color:var(--ink);padding:9px;font:inherit}}
textarea{{min-height:118px;resize:vertical}} .grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px}} .actions{{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}}
button,a.button{{border:1px solid var(--accent);background:var(--accent);color:white;border-radius:6px;padding:9px 13px;font-weight:700;cursor:pointer;text-decoration:none;font-size:13px}}
button.secondary,a.secondary{{background:transparent;color:var(--accent)}} button:disabled{{opacity:.55;cursor:wait}} .status{{font-size:13px;color:var(--muted);line-height:1.7;white-space:pre-wrap}}
.job{{border-top:1px solid var(--line);padding:12px 0}} .job:first-child{{border-top:0}} .pill{{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:1px 8px;font-size:11px;margin-left:6px}}
.done{{color:var(--ok)}} .failed{{color:var(--bad)}} .running,.queued{{color:var(--blue)}} img{{max-width:220px;border:1px solid var(--line);border-radius:6px;background:#fff;margin-top:8px}}
code{{background:#efe6d6;border-radius:4px;padding:1px 5px}} footer{{padding:0 22px 24px;max-width:1280px;margin:0 auto;color:var(--muted);font-size:12px}}
@media(max-width:850px){{main{{grid-template-columns:1fr;padding:14px}} header{{padding:16px}}}}
</style>
</head>
<body>
<header><h1>Layer Rig Bench</h1><p>ComfyUIで一枚絵を作り、see-throughでPSDに分け、Anime2.5DRigで調整します。</p></header>
<main>
<div class="stack">
<section>
  <h2>生成</h2>
  <form id="jobForm">
    <label>プロンプト</label>
    <textarea name="prompt">anime character, solo, full body, clean front view, simple white background, standing pose, separated hair shapes, clear eyes and mouth, high quality illustration</textarea>
    <label>ネガティブ</label>
    <textarea name="negative_prompt">lowres, blurry, cropped, multiple people, text, watermark, bad hands, bad anatomy, extra limbs</textarea>
    <label>モデル</label>
    <select name="model">{model_options}</select>
    <div class="grid">
      <div><label>幅</label><input name="width" type="number" min="512" max="1536" step="64" value="{CONFIG.get("default_width", 768)}"></div>
      <div><label>高さ</label><input name="height" type="number" min="512" max="1536" step="64" value="{CONFIG.get("default_height", 768)}"></div>
      <div><label>steps</label><input name="steps" type="number" min="1" max="60" value="{CONFIG.get("default_steps", 10)}"></div>
      <div><label>cfg</label><input name="cfg" type="number" min="1" max="15" step="0.1" value="{CONFIG.get("default_cfg", 7.0)}"></div>
      <div><label>seed（0でランダム）</label><input name="seed" type="number" value="0"></div>
      <div><label>PSD解像度</label><input name="resolution" type="number" min="768" max="1536" step="64" value="{CONFIG.get("default_resolution", 1024)}"></div>
    </div>
    <div class="actions">
      <button id="startBtn" type="submit">生成してPSD化</button>
      <a class="button secondary" href="/index.html">Anime2.5DRigを開く</a>
      <a class="button secondary" id="latestBtn" href="/index.html?psd=/pipeline/latest/latest.psd">最新PSDを調整</a>
    </div>
  </form>
  <p class="status" id="status">待機中です。</p>
</section>
<section>
  <h2>Eagle画像からPSD</h2>
  <label>Eagleライブラリ画像</label>
  <select id="eagleSelect"></select>
  <div id="eaglePreview" class="status">Eagle画像を読み込み中です。</div>
  <div class="grid">
    <div><label>PSD解像度</label><input id="eagleResolution" type="number" min="768" max="1536" step="64" value="{CONFIG.get("default_resolution", 1024)}"></div>
    <div><label>Depth解像度</label><input id="eagleDepthResolution" type="number" min="512" max="1536" step="64" value="{CONFIG.get("default_resolution_depth", 720)}"></div>
  </div>
  <div class="actions">
    <button id="eagleBtn" type="button">このEagle画像をPSD化</button>
    <button id="refreshEagleBtn" class="secondary" type="button">Eagle一覧を更新</button>
  </div>
</section>
</div>
<section>
  <h2>ジョブ</h2>
  <div id="jobs"></div>
</section>
</main>
<footer>ComfyUI: <code>{html.escape(CONFIG.get("comfy_url", ""))}</code> / see-through: <code>{html.escape(CONFIG.get("see_through_root", ""))}</code></footer>
<script>
const form = document.getElementById('jobForm');
const startBtn = document.getElementById('startBtn');
const eagleSelect = document.getElementById('eagleSelect');
const eagleBtn = document.getElementById('eagleBtn');
const refreshEagleBtn = document.getElementById('refreshEagleBtn');
const eaglePreview = document.getElementById('eaglePreview');
const statusEl = document.getElementById('status');
const jobsEl = document.getElementById('jobs');
let activeJob = null;
let eagleImages = [];
function dataFromForm() {{
  const fd = new FormData(form);
  const obj = Object.fromEntries(fd.entries());
  for (const k of ['width','height','steps','seed','resolution']) obj[k] = parseInt(obj[k] || '0', 10);
  obj.cfg = parseFloat(obj.cfg || '7');
  obj.resolution_depth = 720;
  return obj;
}}
form.addEventListener('submit', async (ev) => {{
  ev.preventDefault();
  startBtn.disabled = true;
  statusEl.textContent = 'ジョブを作成しています...';
  const res = await fetch('/api/jobs', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify(dataFromForm())}});
  const job = await res.json();
  activeJob = job.id;
  await refresh();
}});
async function loadEagleImages() {{
  eaglePreview.textContent = 'Eagle画像を読み込み中です。';
  const res = await fetch('/api/eagle/images?limit=80');
  const data = await res.json();
  eagleImages = data.images || [];
  eagleSelect.innerHTML = eagleImages.map((img, i) => `<option value="${{esc(img.id)}}">${{esc(img.title)}} (${{img.width}}x${{img.height}})</option>`).join('');
  if (!eagleImages.length) {{
    eaglePreview.textContent = 'Eagle画像が見つかりません。';
    eagleBtn.disabled = true;
    return;
  }}
  eagleBtn.disabled = false;
  renderEaglePreview();
}}
function renderEaglePreview() {{
  const img = eagleImages.find(x => x.id === eagleSelect.value);
  if (!img) return;
  const tags = (img.tags || []).slice(0, 10).map(esc).join(' / ');
  eaglePreview.innerHTML = `<img src="${{img.url}}" alt="">`+
    `<div><b>${{esc(img.title)}}</b><br>${{img.width}}x${{img.height}}<br>${{tags}}</div>`;
}}
eagleSelect.addEventListener('change', renderEaglePreview);
refreshEagleBtn.addEventListener('click', loadEagleImages);
eagleBtn.addEventListener('click', async () => {{
  const imageId = eagleSelect.value;
  if (!imageId) return;
  eagleBtn.disabled = true;
  statusEl.textContent = 'Eagle画像のPSD化ジョブを作成しています...';
  const res = await fetch('/api/eagle/jobs', {{
    method:'POST',
    headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{
      image_id: imageId,
      resolution: parseInt(document.getElementById('eagleResolution').value || '1024', 10),
      resolution_depth: parseInt(document.getElementById('eagleDepthResolution').value || '720', 10)
    }})
  }});
  const job = await res.json();
  activeJob = job.id;
  await refresh();
  eagleBtn.disabled = false;
}});
function esc(s) {{ return String(s ?? '').replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c])); }}
function jobHtml(j) {{
  const cls = esc(j.status);
  const links = j.status === 'done'
    ? `<div class="actions"><a class="button" href="${{j.anime_url}}">Anime2.5DRigで開く</a><a class="button secondary" href="${{j.model_psd}}">PSD</a></div>`
    : '';
  const img = j.source_image ? `<div><img src="${{j.source_image}}" alt="生成画像"></div>` : '';
  const log = (j.log || []).slice(-8).map(esc).join('\\n');
  return `<div class="job"><b>${{esc(j.id)}}</b><span class="pill ${{cls}}">${{esc(j.status)}}</span>${{img}}<pre class="status">${{log}}</pre>${{links}}</div>`;
}}
async function refresh() {{
  const res = await fetch('/api/jobs');
  const data = await res.json();
  jobsEl.innerHTML = data.jobs.map(jobHtml).join('') || '<p class="status">まだジョブはありません。</p>';
  const current = activeJob && data.jobs.find(j => j.id === activeJob);
  if (current) {{
    statusEl.textContent = (current.log || []).slice(-5).join('\\n') || current.status;
    if (current.status === 'done' || current.status === 'failed') startBtn.disabled = false;
  }} else {{
    startBtn.disabled = false;
  }}
}}
setInterval(refresh, 3000);
loadEagleImages();
refresh();
</script>
</body>
</html>"""
    return body.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    server_version = "Anime25DPipeline/1.0"

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        try:
            if path in ("/pipeline", "/pipeline/"):
                self.send_bytes(page_html(), "text/html; charset=utf-8")
                return
            if path == "/api/config":
                self.send_json(CONFIG)
                return
            if path == "/api/jobs":
                self.send_json({"jobs": latest_jobs()})
                return
            if path == "/api/eagle/images":
                query = urllib.parse.parse_qs(parsed.query)
                limit = int((query.get("limit") or ["80"])[0])
                self.send_json({"images": list_eagle_images(limit=limit)})
                return
            if path == "/api/eagle/file":
                query = urllib.parse.parse_qs(parsed.query)
                image_id = (query.get("id") or [""])[0]
                rec = get_eagle_record(image_id)
                self.send_local_file(Path(rec["_path"]))
                return
            self.serve_file(path)
        except Exception as exc:
            self.send_error_json(500, str(exc))

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        try:
            if parsed.path == "/api/jobs":
                params = self.read_json_body()
                job = create_job(params)
                self.send_json(job, status=201)
                return
            if parsed.path == "/api/eagle/jobs":
                params = self.read_json_body()
                job = create_eagle_job(params)
                self.send_json(job, status=201)
                return
            self.send_error_json(404, "not found")
        except Exception as exc:
            self.send_error_json(500, str(exc))

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        data = self.rfile.read(length) if length else b"{}"
        return json.loads(data.decode("utf-8") or "{}")

    def serve_file(self, url_path: str) -> None:
        rel = url_path.lstrip("/") or "index.html"
        target = (ROOT / rel).resolve()
        root = ROOT.resolve()
        if not str(target).lower().startswith(str(root).lower()):
            self.send_error_json(403, "forbidden")
            return
        if target.is_dir():
            target = target / "index.html"
        if not target.exists() or not target.is_file():
            self.send_error_json(404, "not found")
            return
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self.send_local_file(target, ctype)

    def send_local_file(self, target: Path, content_type: str | None = None) -> None:
        ctype = content_type or mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(target.stat().st_size))
        self.end_headers()
        with target.open("rb") as handle:
            shutil.copyfileobj(handle, self.wfile)

    def send_json(self, data: Any, status: int = 200) -> None:
        self.send_bytes(json.dumps(data, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8", status)

    def send_error_json(self, status: int, message: str) -> None:
        self.send_json({"error": message}, status=status)

    def send_bytes(self, data: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stdout.write("%s - %s\n" % (self.address_string(), fmt % args))


def main() -> int:
    for path in (JOBS_DIR, LATEST_DIR, LOGS_DIR):
        path.mkdir(parents=True, exist_ok=True)
    load_jobs()
    host = CONFIG.get("host", "127.0.0.1")
    port = int(CONFIG.get("port", 8765))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Pipeline UI: http://{host}:{port}/pipeline")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping pipeline server.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
