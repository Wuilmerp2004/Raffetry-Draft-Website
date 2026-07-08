# AI Video Metadata Tagger — RaffertyWeiss Media

Link: https://rafferty-media-website.vercel.app 

A full-stack application that automatically generates rich, searchable metadata for video files, built for a real media client to replace a slow manual cataloging workflow with an automated pipeline powered by Google's Gemini API.

## Overview

RaffertyWeiss Media needed a faster way to catalog and search their growing video library. Manually reviewing and tagging a single video took roughly 30 minutes per file. This tool cuts that down to under a minute per video by combining an adaptive video compression pipeline with Gemini's multimodal understanding to auto-generate structured metadata at scale.

## Key Features

- **Automated metadata generation** — produces 28 distinct metadata fields per video (topics, objects, tone, setting, notable moments, and more) without manual review
- **Adaptive FFmpeg compression pipeline** — shrinks source files (up to 400 MB) down to under 10 MB before sending them to the Gemini API, cutting processing time by 10x and reducing API costs by 90%
- **Parallel batch processing** — processes 10+ videos concurrently with live streaming results via NDJSON, so results appear as they complete rather than after the whole batch finishes
- **Searchable archive browser** — a frontend interface for browsing tagged videos with 9 filterable dimensions (topic, tone, setting, objects, and more)
- **CSV export** — one-click export of metadata for reporting or downstream tooling

## Tech Stack

- **Backend:** Node.js, Express
- **AI/ML:** Google Gemini API (multimodal video understanding)
- **Media processing:** FFmpeg (adaptive compression pipeline)
- **Data streaming:** NDJSON over HTTP for real-time batch progress
- **Frontend:** Searchable/filterable archive browser with CSV export

## How It Works

1. **Ingest** — a video file is uploaded or queued for processing.
2. **Compress** — an adaptive FFmpeg pipeline analyzes the source file and compresses it down to under 10 MB, tuning compression settings based on the original file size and length to preserve enough visual fidelity for accurate tagging.
3. **Tag** — the compressed video is sent to the Gemini API, which returns structured metadata across 28 fields.
4. **Stream results** — as each video in a batch finishes processing, results are streamed back to the client via NDJSON so the archive updates in real time instead of waiting on the full batch.
5. **Browse & export** — tagged videos land in a searchable archive with multi-dimension filtering and CSV export for reporting.

## Impact

| Metric | Before | After |
|---|---|---|
| Time per video | ~30 minutes (manual) | Under 1 minute (automated) |
| Processing speed | Baseline | 10x faster |
| API cost | Baseline | 90% reduction |
| Batch handling | One at a time | 10+ videos in parallel |

## Status

Built and delivered for a live client (RaffertyWeiss Media). Deployed and tested across Vercel, Railway, and Render during the deployment process.

## Future Improvements

- Expand configurable metadata schemas per client vertical
- Add authentication and multi-tenant support for additional clients
- Introduce automated regression tests for the compression pipeline
