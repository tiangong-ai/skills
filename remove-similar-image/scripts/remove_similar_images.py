#!/usr/bin/env python3
"""Find similar or blurry local images and optionally remove them."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import cv2  # type: ignore
except ImportError:
    cv2 = None

try:
    import imagehash  # type: ignore
except ImportError:
    imagehash = None

try:
    import numpy as np  # type: ignore
except ImportError:
    np = None

try:
    from PIL import Image, ImageOps, UnidentifiedImageError  # type: ignore
except ImportError:
    Image = None
    ImageOps = None

    class UnidentifiedImageError(Exception):
        """Fallback error used when Pillow is unavailable."""


INSTALL_COMMAND = "python3 -m pip install Pillow ImageHash numpy opencv-python-headless"
DEFAULT_SIMILAR_THRESHOLD = 5
DEFAULT_BLUR_THRESHOLD = 100.0
DEFAULT_HASH_SIZE = 8
DEFAULT_EXTENSIONS = {
    ".avif",
    ".bmp",
    ".gif",
    ".heic",
    ".heif",
    ".jpeg",
    ".jpg",
    ".png",
    ".tif",
    ".tiff",
    ".webp",
}
HASH_METHODS = {"phash", "dhash", "ahash", "whash"}


@dataclass(frozen=True)
class ImageRecord:
    path: Path
    rel_path: str
    width: int
    height: int
    pixels: int
    file_size: int
    modified_at: float
    modified_at_iso: str
    hash_hex: str
    hash_int: int
    blur_score: float
    is_blurry: bool


@dataclass(frozen=True)
class ReadFailure:
    path: Path
    rel_path: str
    error: str


@dataclass
class BKNode:
    value: int
    indices: list[int] = field(default_factory=list)
    children: dict[int, "BKNode"] = field(default_factory=dict)


class BKTree:
    def __init__(self) -> None:
        self.root: BKNode | None = None

    def add(self, value: int, index: int) -> None:
        if self.root is None:
            self.root = BKNode(value=value, indices=[index])
            return

        node = self.root
        while True:
            distance = hamming_distance(value, node.value)
            if distance == 0:
                node.indices.append(index)
                return
            child = node.children.get(distance)
            if child is None:
                node.children[distance] = BKNode(value=value, indices=[index])
                return
            node = child

    def search(self, value: int, threshold: int) -> list[tuple[int, int]]:
        if self.root is None:
            return []

        results: list[tuple[int, int]] = []
        stack = [self.root]
        while stack:
            node = stack.pop()
            distance = hamming_distance(value, node.value)
            if distance <= threshold:
                results.extend((index, distance) for index in node.indices)

            lower = max(0, distance - threshold)
            upper = distance + threshold
            for child_distance, child in node.children.items():
                if lower <= child_distance <= upper:
                    stack.append(child)
        return results


class DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, item: int) -> int:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: int, right: int) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if self.rank[left_root] < self.rank[right_root]:
            self.parent[left_root] = right_root
        elif self.rank[left_root] > self.rank[right_root]:
            self.parent[right_root] = left_root
        else:
            self.parent[right_root] = left_root
            self.rank[left_root] += 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze local images with ImageHash and OpenCV to find similar or blurry "
            "photos and optionally delete or move them."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Print dependency status for Pillow, ImageHash, numpy, and OpenCV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    doctor_parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print dependency status as JSON instead of human-readable text.",
    )

    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Scan images, group similar files, score blur, and optionally remove files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    analyze_parser.add_argument(
        "--input-path",
        type=Path,
        required=True,
        help="Local file or directory to analyze.",
    )
    analyze_parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Do not recurse into subdirectories when input is a directory.",
    )
    analyze_parser.add_argument(
        "--extra-extension",
        action="append",
        default=[],
        help="Additional extension to include, for example .jfif or .raw.",
    )
    analyze_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of files scanned after sorting by path.",
    )
    analyze_parser.add_argument(
        "--hash-method",
        choices=sorted(HASH_METHODS),
        default="phash",
        help="ImageHash algorithm used to compare images.",
    )
    analyze_parser.add_argument(
        "--hash-size",
        type=int,
        default=DEFAULT_HASH_SIZE,
        help="ImageHash size. Larger values are slower but more discriminative.",
    )
    analyze_parser.add_argument(
        "--similar-threshold",
        type=int,
        default=DEFAULT_SIMILAR_THRESHOLD,
        help="Maximum Hamming distance treated as similar.",
    )
    analyze_parser.add_argument(
        "--blur-threshold",
        type=float,
        default=DEFAULT_BLUR_THRESHOLD,
        help="Variance-of-Laplacian score below which an image is considered blurry.",
    )
    analyze_parser.add_argument(
        "--keep-policy",
        choices=("best", "largest", "newest", "oldest"),
        default="best",
        help="How to choose the keeper inside each similar-image group.",
    )
    analyze_parser.add_argument(
        "--delete-similar",
        action="store_true",
        help="Mark non-keeper members of similar groups for deletion or moving.",
    )
    analyze_parser.add_argument(
        "--delete-blurry",
        action="store_true",
        help="Mark blurry images for deletion or moving, even if unique.",
    )
    analyze_parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletion or move actions. Without this flag the script only previews.",
    )
    analyze_parser.add_argument(
        "--trash-dir",
        type=Path,
        default=None,
        help="Move files into this directory instead of permanently deleting them.",
    )
    analyze_parser.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="Write the full analysis report to a JSON file.",
    )
    analyze_parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print the full analysis report as JSON to stdout.",
    )
    analyze_parser.add_argument(
        "--top-groups",
        type=int,
        default=10,
        help="Maximum number of similar groups to print in text mode.",
    )
    analyze_parser.add_argument(
        "--top-blurry",
        type=int,
        default=10,
        help="Maximum number of blurry images to print in text mode.",
    )

    return parser


def dependency_status() -> list[dict[str, Any]]:
    return [
        {
            "module": "Pillow",
            "import_name": "PIL",
            "available": Image is not None,
            "version": getattr(Image, "__version__", None) if Image is not None else None,
        },
        {
            "module": "ImageHash",
            "import_name": "imagehash",
            "available": imagehash is not None,
            "version": getattr(imagehash, "__version__", None) if imagehash is not None else None,
        },
        {
            "module": "numpy",
            "import_name": "numpy",
            "available": np is not None,
            "version": getattr(np, "__version__", None) if np is not None else None,
        },
        {
            "module": "OpenCV",
            "import_name": "cv2",
            "available": cv2 is not None,
            "version": getattr(cv2, "__version__", None) if cv2 is not None else None,
        },
    ]


def require_runtime_dependencies() -> None:
    missing = [row["import_name"] for row in dependency_status() if not row["available"]]
    if not missing:
        return
    print(
        "Missing dependency modules: "
        + ", ".join(missing)
        + f". Install with `{INSTALL_COMMAND}`.",
        file=sys.stderr,
    )
    raise SystemExit(2)


def normalize_extension(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        raise ValueError("empty extension")
    if not normalized.startswith("."):
        normalized = f".{normalized}"
    return normalized


def collect_extensions(extra_extensions: list[str]) -> set[str]:
    extensions = set(DEFAULT_EXTENSIONS)
    for value in extra_extensions:
        extensions.add(normalize_extension(value))
    return extensions


def validate_analyze_args(args: argparse.Namespace) -> None:
    if args.hash_size <= 0:
        raise ValueError("--hash-size must be greater than 0")
    max_bits = args.hash_size * args.hash_size
    if args.similar_threshold < 0 or args.similar_threshold > max_bits:
        raise ValueError(f"--similar-threshold must be between 0 and {max_bits}")
    if args.blur_threshold < 0:
        raise ValueError("--blur-threshold must be 0 or greater")
    if args.limit is not None and args.limit < 0:
        raise ValueError("--limit must be 0 or greater")
    if args.top_groups < 0:
        raise ValueError("--top-groups must be 0 or greater")
    if args.top_blurry < 0:
        raise ValueError("--top-blurry must be 0 or greater")
    if args.apply and not (args.delete_similar or args.delete_blurry):
        raise ValueError("--apply requires --delete-similar or --delete-blurry")
    if not args.input_path.exists():
        raise ValueError(f"Input path does not exist: {args.input_path}")
    if args.trash_dir is not None and args.trash_dir.exists() and not args.trash_dir.is_dir():
        raise ValueError("--trash-dir must be a directory path")


def resolve_root_dir(input_path: Path) -> Path:
    if input_path.is_dir():
        return input_path
    return input_path.parent


def collect_image_paths(
    input_path: Path,
    recursive: bool,
    extensions: set[str],
    limit: int | None,
) -> list[Path]:
    if input_path.is_file():
        if limit == 0:
            return []
        return [input_path]

    iterator = input_path.rglob("*") if recursive else input_path.glob("*")
    paths = sorted(
        path
        for path in iterator
        if path.is_file() and path.suffix.lower() in extensions
    )
    if limit is not None:
        return paths[:limit]
    return paths


def path_relative_to_root(path: Path, root_dir: Path) -> str:
    try:
        return str(path.relative_to(root_dir))
    except ValueError:
        return path.name


def isoformat_timestamp(timestamp: float) -> str:
    return (
        datetime.fromtimestamp(timestamp, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_normalized_image(path: Path) -> Any:
    assert Image is not None
    assert ImageOps is not None
    with Image.open(path) as raw_image:
        raw_image.load()
        image = ImageOps.exif_transpose(raw_image)
        if image.mode not in {"L", "RGB", "RGBA"}:
            image = image.convert("RGB")
        else:
            image = image.copy()
    return image


def compute_hash_hex(image: Any, hash_method: str, hash_size: int) -> str:
    assert imagehash is not None
    if hash_method == "phash":
        hash_value = imagehash.phash(image, hash_size=hash_size)
    elif hash_method == "dhash":
        hash_value = imagehash.dhash(image, hash_size=hash_size)
    elif hash_method == "ahash":
        hash_value = imagehash.average_hash(image, hash_size=hash_size)
    elif hash_method == "whash":
        hash_value = imagehash.whash(image, hash_size=hash_size)
    else:
        raise ValueError(f"Unsupported hash method: {hash_method}")
    return str(hash_value)


def compute_blur_score(image: Any) -> float:
    assert cv2 is not None
    assert np is not None

    pixel_array = np.array(image)
    if pixel_array.ndim == 2:
        grayscale = pixel_array
    elif pixel_array.ndim == 3 and pixel_array.shape[2] == 4:
        grayscale = cv2.cvtColor(pixel_array, cv2.COLOR_RGBA2GRAY)
    elif pixel_array.ndim == 3 and pixel_array.shape[2] == 3:
        grayscale = cv2.cvtColor(pixel_array, cv2.COLOR_RGB2GRAY)
    else:
        raise ValueError(f"Unsupported image array shape: {pixel_array.shape}")
    return float(cv2.Laplacian(grayscale, cv2.CV_64F).var())


def analyze_images(
    image_paths: list[Path],
    root_dir: Path,
    hash_method: str,
    hash_size: int,
    blur_threshold: float,
) -> tuple[list[ImageRecord], list[ReadFailure]]:
    records: list[ImageRecord] = []
    failures: list[ReadFailure] = []

    for path in image_paths:
        rel_path = path_relative_to_root(path, root_dir)
        try:
            image = load_normalized_image(path)
            width, height = image.size
            hash_hex = compute_hash_hex(image, hash_method=hash_method, hash_size=hash_size)
            blur_score = compute_blur_score(image)
            modified_at = path.stat().st_mtime
            records.append(
                ImageRecord(
                    path=path,
                    rel_path=rel_path,
                    width=width,
                    height=height,
                    pixels=width * height,
                    file_size=path.stat().st_size,
                    modified_at=modified_at,
                    modified_at_iso=isoformat_timestamp(modified_at),
                    hash_hex=hash_hex,
                    hash_int=int(hash_hex, 16),
                    blur_score=blur_score,
                    is_blurry=blur_score < blur_threshold,
                )
            )
        except (OSError, UnidentifiedImageError, ValueError) as exc:
            failures.append(ReadFailure(path=path, rel_path=rel_path, error=str(exc)))
    return records, failures


def hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def build_similarity_components(records: list[ImageRecord], threshold: int) -> list[list[int]]:
    if len(records) < 2:
        return []

    tree = BKTree()
    groups = DisjointSet(len(records))

    for index, record in enumerate(records):
        for neighbor_index, _distance in tree.search(record.hash_int, threshold):
            groups.union(index, neighbor_index)
        tree.add(record.hash_int, index)

    by_root: dict[int, list[int]] = defaultdict(list)
    for index in range(len(records)):
        by_root[groups.find(index)].append(index)
    return [member_indices for member_indices in by_root.values() if len(member_indices) > 1]


def keeper_sort_key(record: ImageRecord, keep_policy: str) -> tuple[Any, ...]:
    if keep_policy == "best":
        return (
            0 if record.is_blurry else 1,
            record.blur_score,
            record.pixels,
            record.file_size,
            record.modified_at,
            record.rel_path,
        )
    if keep_policy == "largest":
        return (
            record.pixels,
            record.file_size,
            0 if record.is_blurry else 1,
            record.blur_score,
            record.modified_at,
            record.rel_path,
        )
    if keep_policy == "newest":
        return (
            record.modified_at,
            0 if record.is_blurry else 1,
            record.blur_score,
            record.pixels,
            record.file_size,
            record.rel_path,
        )
    if keep_policy == "oldest":
        return (
            -record.modified_at,
            0 if record.is_blurry else 1,
            record.blur_score,
            record.pixels,
            record.file_size,
            record.rel_path,
        )
    raise ValueError(f"Unsupported keep policy: {keep_policy}")


def closest_group_distance(index: int, member_indices: list[int], records: list[ImageRecord]) -> int:
    distances = [
        hamming_distance(records[index].hash_int, records[other_index].hash_int)
        for other_index in member_indices
        if other_index != index
    ]
    if not distances:
        return 0
    return min(distances)


def build_group_reports(
    records: list[ImageRecord],
    similar_components: list[list[int]],
    keep_policy: str,
) -> tuple[list[dict[str, Any]], set[int], set[int]]:
    reports: list[dict[str, Any]] = []
    keeper_indices: set[int] = set()
    non_keeper_indices: set[int] = set()

    for member_indices in similar_components:
        ordered_indices = sorted(
            member_indices,
            key=lambda index: keeper_sort_key(records[index], keep_policy),
            reverse=True,
        )
        keeper_index = ordered_indices[0]
        keeper_indices.add(keeper_index)
        for index in ordered_indices[1:]:
            non_keeper_indices.add(index)

        members_payload = []
        for index in ordered_indices:
            record = records[index]
            members_payload.append(
                {
                    "path": str(record.path),
                    "rel_path": record.rel_path,
                    "keep": index == keeper_index,
                    "width": record.width,
                    "height": record.height,
                    "pixels": record.pixels,
                    "file_size": record.file_size,
                    "blur_score": round(record.blur_score, 4),
                    "is_blurry": record.is_blurry,
                    "hash_hex": record.hash_hex,
                    "closest_distance": closest_group_distance(index, ordered_indices, records),
                }
            )

        reports.append(
            {
                "keeper_path": str(records[keeper_index].path),
                "keeper_rel_path": records[keeper_index].rel_path,
                "member_count": len(ordered_indices),
                "members": members_payload,
            }
        )

    reports.sort(
        key=lambda report: (
            report["member_count"],
            report["keeper_rel_path"],
        ),
        reverse=True,
    )
    return reports, keeper_indices, non_keeper_indices


def build_action_reasons(
    records: list[ImageRecord],
    non_keeper_indices: set[int],
    delete_similar: bool,
    delete_blurry: bool,
) -> dict[int, list[str]]:
    actions: dict[int, list[str]] = {}

    if delete_similar:
        for index in sorted(non_keeper_indices):
            actions.setdefault(index, []).append("similar_non_keeper")

    if delete_blurry:
        for index, record in enumerate(records):
            if record.is_blurry:
                actions.setdefault(index, []).append("blurry")

    return actions


def build_trash_destination(source_path: Path, trash_dir: Path, root_dir: Path) -> Path:
    try:
        relative_path = source_path.relative_to(root_dir)
    except ValueError:
        relative_path = Path(source_path.name)

    destination = trash_dir / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        return destination

    counter = 1
    while True:
        candidate = destination.with_name(
            f"{destination.stem}__{counter}{destination.suffix}"
        )
        if not candidate.exists():
            return candidate
        counter += 1


def apply_actions(
    records: list[ImageRecord],
    action_reasons: dict[int, list[str]],
    root_dir: Path,
    trash_dir: Path | None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    if trash_dir is not None:
        trash_dir.mkdir(parents=True, exist_ok=True)

    for index in sorted(action_reasons, key=lambda item: records[item].rel_path):
        record = records[index]
        payload: dict[str, Any] = {
            "path": str(record.path),
            "rel_path": record.rel_path,
            "reasons": action_reasons[index],
        }
        if not record.path.exists():
            payload["status"] = "missing"
            results.append(payload)
            continue

        try:
            if trash_dir is None:
                record.path.unlink()
                payload["status"] = "deleted"
            else:
                destination = build_trash_destination(record.path, trash_dir, root_dir)
                shutil.move(str(record.path), str(destination))
                payload["status"] = "moved"
                payload["destination"] = str(destination)
        except OSError as exc:
            payload["status"] = "error"
            payload["error"] = str(exc)
        results.append(payload)

    return results


def build_report(
    args: argparse.Namespace,
    records: list[ImageRecord],
    failures: list[ReadFailure],
    group_reports: list[dict[str, Any]],
    non_keeper_indices: set[int],
    action_reasons: dict[int, list[str]],
    applied_actions: list[dict[str, Any]],
) -> dict[str, Any]:
    blurry_indices = [index for index, record in enumerate(records) if record.is_blurry]

    report = {
        "config": {
            "input_path": str(args.input_path),
            "recursive": not args.no_recursive,
            "hash_method": args.hash_method,
            "hash_size": args.hash_size,
            "similar_threshold": args.similar_threshold,
            "blur_threshold": args.blur_threshold,
            "keep_policy": args.keep_policy,
            "delete_similar": args.delete_similar,
            "delete_blurry": args.delete_blurry,
            "apply": args.apply,
            "trash_dir": str(args.trash_dir) if args.trash_dir is not None else None,
            "extra_extensions": [normalize_extension(value) for value in args.extra_extension],
            "limit": args.limit,
        },
        "summary": {
            "scanned_images": len(records),
            "unreadable_images": len(failures),
            "similar_groups": len(group_reports),
            "similar_non_keepers": len(non_keeper_indices),
            "blurry_images": len(blurry_indices),
            "planned_actions": len(action_reasons),
            "applied_actions": len(applied_actions),
            "applied_failures": sum(1 for item in applied_actions if item["status"] == "error"),
        },
        "images": [
            {
                "path": str(record.path),
                "rel_path": record.rel_path,
                "width": record.width,
                "height": record.height,
                "pixels": record.pixels,
                "file_size": record.file_size,
                "modified_at": record.modified_at_iso,
                "hash_hex": record.hash_hex,
                "blur_score": round(record.blur_score, 4),
                "is_blurry": record.is_blurry,
            }
            for record in records
        ],
        "unreadable": [
            {
                "path": str(failure.path),
                "rel_path": failure.rel_path,
                "error": failure.error,
            }
            for failure in failures
        ],
        "similar_groups": group_reports,
        "planned_actions": [
            {
                "path": str(records[index].path),
                "rel_path": records[index].rel_path,
                "reasons": reasons,
            }
            for index, reasons in sorted(
                action_reasons.items(),
                key=lambda item: records[item[0]].rel_path,
            )
        ],
        "applied_actions": applied_actions,
    }
    return report


def write_report_json(output_path: Path, report: dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def print_doctor_report(print_json: bool) -> None:
    status = {
        "dependencies": dependency_status(),
        "install_command": INSTALL_COMMAND,
    }
    if print_json:
        print(json.dumps(status, indent=2, ensure_ascii=False))
        return

    for row in status["dependencies"]:
        label = "OK" if row["available"] else "MISSING"
        version = f" version={row['version']}" if row["version"] else ""
        print(f"{label:<7} {row['module']} ({row['import_name']}){version}")
    print(f"Install command: {INSTALL_COMMAND}")


def print_text_report(
    report: dict[str, Any],
    top_groups: int,
    top_blurry: int,
) -> None:
    summary = report["summary"]
    config = report["config"]

    print(
        f"Scanned {summary['scanned_images']} image(s); unreadable {summary['unreadable_images']}."
    )
    print(
        f"Found {summary['similar_groups']} similar group(s) with "
        f"{summary['similar_non_keepers']} non-keeper candidate(s)."
    )
    print(
        f"Found {summary['blurry_images']} blurry image(s) using blur threshold "
        f"{config['blur_threshold']}."
    )

    if report["unreadable"]:
        print("")
        print("Unreadable images:")
        for item in report["unreadable"][:10]:
            print(f"  - {item['rel_path']}: {item['error']}")
        remaining = len(report["unreadable"]) - min(len(report["unreadable"]), 10)
        if remaining > 0:
            print(f"  ... {remaining} more")

    if report["similar_groups"] and top_groups > 0:
        print("")
        visible_groups = report["similar_groups"][:top_groups]
        print(f"Similar groups ({len(visible_groups)} of {len(report['similar_groups'])}):")
        for group_number, group in enumerate(visible_groups, start=1):
            print(
                f"  [{group_number}] keep {group['keeper_rel_path']} "
                f"({group['member_count']} images)"
            )
            for member in group["members"]:
                status = "keep" if member["keep"] else "candidate"
                print(
                    "      "
                    f"{status:<9} {member['rel_path']} "
                    f"dist={member['closest_distance']} "
                    f"blur={member['blur_score']:.2f} "
                    f"size={member['width']}x{member['height']}"
                )

    blurry_images = sorted(
        (item for item in report["images"] if item["is_blurry"]),
        key=lambda item: (item["blur_score"], item["rel_path"]),
    )
    if blurry_images and top_blurry > 0:
        print("")
        visible_blurry = blurry_images[:top_blurry]
        print(f"Blurriest images ({len(visible_blurry)} of {len(blurry_images)}):")
        for item in visible_blurry:
            print(
                f"  - {item['rel_path']} blur={item['blur_score']:.2f} "
                f"size={item['width']}x{item['height']}"
            )

    if report["planned_actions"]:
        print("")
        label = "Applied actions" if report["applied_actions"] else "Planned actions"
        print(f"{label} ({len(report['planned_actions'])}):")
        for item in report["planned_actions"][:20]:
            reason_text = ",".join(item["reasons"])
            print(f"  - {item['rel_path']} reasons={reason_text}")
        remaining = len(report["planned_actions"]) - min(len(report["planned_actions"]), 20)
        if remaining > 0:
            print(f"  ... {remaining} more")

    if report["applied_actions"]:
        print("")
        print(f"Action results ({len(report['applied_actions'])}):")
        for item in report["applied_actions"][:20]:
            extra = ""
            if item.get("destination"):
                extra = f" -> {item['destination']}"
            if item.get("error"):
                extra = f" error={item['error']}"
            print(
                f"  - {item['status']:<7} {item['rel_path']} "
                f"reasons={','.join(item['reasons'])}{extra}"
            )
        remaining = len(report["applied_actions"]) - min(len(report["applied_actions"]), 20)
        if remaining > 0:
            print(f"  ... {remaining} more")


def run_analyze(args: argparse.Namespace) -> int:
    validate_analyze_args(args)
    require_runtime_dependencies()

    extensions = collect_extensions(args.extra_extension)
    root_dir = resolve_root_dir(args.input_path)
    image_paths = collect_image_paths(
        input_path=args.input_path,
        recursive=not args.no_recursive,
        extensions=extensions,
        limit=args.limit,
    )
    records, failures = analyze_images(
        image_paths=image_paths,
        root_dir=root_dir,
        hash_method=args.hash_method,
        hash_size=args.hash_size,
        blur_threshold=args.blur_threshold,
    )

    similar_components = build_similarity_components(records, args.similar_threshold)
    group_reports, _keeper_indices, non_keeper_indices = build_group_reports(
        records=records,
        similar_components=similar_components,
        keep_policy=args.keep_policy,
    )
    action_reasons = build_action_reasons(
        records=records,
        non_keeper_indices=non_keeper_indices,
        delete_similar=args.delete_similar,
        delete_blurry=args.delete_blurry,
    )

    applied_actions: list[dict[str, Any]] = []
    if args.apply and action_reasons:
        applied_actions = apply_actions(
            records=records,
            action_reasons=action_reasons,
            root_dir=root_dir,
            trash_dir=args.trash_dir,
        )

    report = build_report(
        args=args,
        records=records,
        failures=failures,
        group_reports=group_reports,
        non_keeper_indices=non_keeper_indices,
        action_reasons=action_reasons,
        applied_actions=applied_actions,
    )

    if args.report_json is not None:
        write_report_json(args.report_json, report)
    if args.print_json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print_text_report(report, top_groups=args.top_groups, top_blurry=args.top_blurry)

    if any(item.get("status") == "error" for item in applied_actions):
        return 1
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "doctor":
            print_doctor_report(print_json=args.print_json)
            return 0
        if args.command == "analyze":
            return run_analyze(args)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
