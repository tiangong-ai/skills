---
name: remove-similar-image
description: Analyze local image files with ImageHash and OpenCV to detect near-duplicate photos and blurry shots, then preview or remove them. Use when Codex needs to clean a photo folder, deduplicate similar images, identify blur, or safely delete/move bad images after reviewing a report.
---

# Remove Similar Image

## Core Goal
- Scan one local image or a directory tree of local images.
- Use ImageHash to cluster exact duplicates and near-duplicates.
- Use OpenCV variance-of-Laplacian blur scoring to flag blurry shots.
- Preview actions first, then permanently delete or move candidates to a trash folder when requested.

## Required Script
- Use `scripts/remove_similar_images.py`.
- Start with `doctor` if dependency availability is unknown.
- Treat `analyze` without `--apply` as the safe default.
- Prefer `--trash-dir` before permanent deletion so the user can review results.

## Dependency
- This skill requires Pillow, ImageHash, numpy, and OpenCV:

```bash
python3 -m pip install Pillow ImageHash numpy opencv-python-headless
```

- If `doctor` reports missing dependencies, stop and surface the install command instead of pretending the scan ran.

## Workflow
1. Check dependencies:

```bash
python3 scripts/remove_similar_images.py doctor
```

2. Preview similar groups and blurry images for a folder:

```bash
python3 scripts/remove_similar_images.py analyze \
  --input-path /path/to/photos
```

3. Preview safe cleanup by moving similar non-keepers and blurry files into a trash folder:

```bash
python3 scripts/remove_similar_images.py analyze \
  --input-path /path/to/photos \
  --delete-similar \
  --delete-blurry \
  --trash-dir /path/to/review-trash
```

4. Apply the move after the preview looks correct:

```bash
python3 scripts/remove_similar_images.py analyze \
  --input-path /path/to/photos \
  --delete-similar \
  --delete-blurry \
  --trash-dir /path/to/review-trash \
  --apply
```

5. Permanently delete only similar non-keepers:

```bash
python3 scripts/remove_similar_images.py analyze \
  --input-path /path/to/photos \
  --delete-similar \
  --apply
```

## Main Arguments
- `--input-path`: source image file or directory.
- `--no-recursive`: scan only the top-level directory.
- `--extra-extension`: include additional suffixes not covered by default.
- `--limit`: cap the number of scanned files for quick tests.
- `--hash-method`: `phash`, `dhash`, `ahash`, or `whash`.
- `--hash-size`: larger hashes are stricter and slower.
- `--similar-threshold`: maximum Hamming distance considered similar.
- `--blur-threshold`: Laplacian-variance cutoff for blurry images.
- `--keep-policy`: choose the keeper in each similar group with `best`, `largest`, `newest`, or `oldest`.
- `--delete-similar`: mark non-keeper files in similar groups as removal candidates.
- `--delete-blurry`: mark blurry files as removal candidates even when they are unique.
- `--trash-dir`: move files into a review directory instead of permanently deleting.
- `--apply`: execute removals or moves. Without this flag the script only reports.
- `--report-json`: save a machine-readable report for later review.
- `--print-json`: print the full report as JSON to stdout.

## Default Heuristics
- Default similarity detection uses `phash` with `hash_size=8` and `similar-threshold=5`.
- Default blur detection uses a variance-of-Laplacian cutoff of `100.0`.
- Default `keep-policy=best` prefers non-blurry images, then sharper images, then larger images.
- Similar groups are connected components: if `A` is close to `B`, and `B` is close to `C`, they are treated as one group even if `A` and `C` are slightly farther apart.

## Usage Notes
- Review the preview before adding `--apply`.
- Use `--trash-dir` for the first pass on any valuable photo collection.
- Lower `--similar-threshold` to be stricter. Raise it when near-duplicates are being missed.
- Lower `--blur-threshold` if too many acceptable images are marked blurry. Raise it when obvious blur is missed.
- Expect format support to follow Pillow and OpenCV availability in the local environment.

## Output
- Text mode prints scan counts, similar groups, blurry images, and planned or applied actions.
- JSON mode includes per-image metadata, unreadable files, similar groups, planned actions, and action results.

## Script
- `scripts/remove_similar_images.py`
