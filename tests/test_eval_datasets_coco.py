"""A boxes dataset as COCO (``client.evals.datasets.coco``) and written to a
folder for an experiment (``download``): annotations.json, items.jsonl and
the images, with safe file names (sync and async)."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from typing import Any

import httpx

from lbb import AsyncLbbClient, LbbClient, models

ID = "ds_1791967648726a1b2c3d4e5f6"
SHA_A = "a" * 64
SHA_B = "b" * 64

COCO: dict[str, Any] = {
    "info": {"dataset": ID, "name": "plans", "created": "2026-11-27T10:00:00.000000Z"},
    "images": [
        {"id": 1, "file_name": "../plan 1", "width": 100, "height": 80, "lbb_file": SHA_A, "lbb_status": "done", "lbb_labels": ["door"]},
        {"id": 2, "file_name": "plan-2", "width": 100, "height": 80, "lbb_file": SHA_B, "lbb_status": "todo"},
    ],
    "annotations": [
        {"id": 1, "image_id": 1, "category_id": 2, "bbox": [20.0, 0.0, 20.0, 4.0], "area": 80.0, "iscrowd": 0, "lbb_uncertain": True},
        {"id": 2, "image_id": 1, "category_id": 1, "bbox": [70.0, 70.0, 10.0, 10.0], "area": 100.0, "iscrowd": 1},
        {"id": 3, "image_id": 2, "category_id": 1, "bbox": [1.0, 1.0, 4.0, 4.0], "area": 16.0, "iscrowd": 0},
    ],
    "categories": [{"id": 1, "name": "door"}, {"id": 2, "name": "window"}],
}
BYTES = {SHA_A: b"\x89PNG-a", SHA_B: b"\x89PNG-b"}


def handler(seen: list[tuple[str, str, dict[str, str]]]):
    def handle(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen.append((request.method, request.url.path, params))
        if request.url.path == "/v1/evals/datasets/coco":
            return httpx.Response(200, json=COCO)
        if request.url.path == "/v1/files/content":
            return httpx.Response(200, content=BYTES[params["sha256"]], headers={"content-type": "image/png"})
        return httpx.Response(404, json={"error": {"type": "not_found", "code": "not_found", "message": "no"}})

    return handle


class CocoAndDownload(unittest.TestCase):
    def test_coco_reads_the_route_and_types_the_answer(self) -> None:
        seen: list[tuple[str, str, dict[str, str]]] = []
        client = LbbClient("http://lbb.test", transport=httpx.MockTransport(handler(seen)))
        coco = client.evals.datasets.coco(ID)
        self.assertIsInstance(coco, models.EvalCocoExport)
        self.assertEqual(seen[0][:2], ("GET", "/v1/evals/datasets/coco"))
        self.assertEqual(seen[0][2]["id"], ID)
        self.assertEqual(coco.annotations[0].lbb_uncertain, True)

    def test_download_writes_annotations_items_and_safe_image_names(self) -> None:
        seen: list[tuple[str, str, dict[str, str]]] = []
        client = LbbClient("http://lbb.test", transport=httpx.MockTransport(handler(seen)))
        with tempfile.TemporaryDirectory() as tmp:
            root = client.evals.datasets.download(ID, os.path.join(tmp, "plans"))
            images = sorted(os.listdir(os.path.join(root, "images")))
            # A name that would leave the folder stays inside it.
            self.assertEqual(images, ["_plan_1", "plan-2"])
            with open(os.path.join(root, "images", "_plan_1"), "rb") as f:
                self.assertEqual(f.read(), BYTES[SHA_A])
            with open(os.path.join(root, "annotations.json"), encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual([i["file_name"] for i in data["images"]], ["_plan_1", "plan-2"])
            with open(os.path.join(root, "items.jsonl"), encoding="utf-8") as f:
                rows = [json.loads(line) for line in f]
            self.assertEqual(
                rows[0],
                {
                    "file": "images/_plan_1",
                    "width": 100,
                    "height": 80,
                    "status": "done",
                    "labels": ["door"],
                    "boxes": [{"label": "window", "box": [20.0, 0.0, 40.0, 4.0], "uncertain": True}],
                    "ignore": [{"label": "door", "box": [70.0, 70.0, 80.0, 80.0]}],
                },
            )
            self.assertEqual(rows[1]["status"], "todo")

    def test_download_keeps_only_done_images_when_asked(self) -> None:
        seen: list[tuple[str, str, dict[str, str]]] = []
        client = LbbClient("http://lbb.test", transport=httpx.MockTransport(handler(seen)))
        with tempfile.TemporaryDirectory() as tmp:
            root = client.evals.datasets.download(ID, tmp, only_done=True)
            self.assertEqual(os.listdir(os.path.join(root, "images")), ["_plan_1"])
            with open(os.path.join(root, "annotations.json"), encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual([a["id"] for a in data["annotations"]], [1, 2])
        self.assertEqual([p for _, p, _ in seen].count("/v1/files/content"), 1)

    def test_async_download_writes_the_same_folder(self) -> None:
        seen: list[tuple[str, str, dict[str, str]]] = []

        async def run(tmp: str) -> list[str]:
            async with AsyncLbbClient("http://lbb.test", transport=httpx.MockTransport(handler(seen))) as client:
                root = await client.evals.datasets.download(ID, tmp)
                return sorted(os.listdir(os.path.join(root, "images")))

        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(asyncio.run(run(tmp)), ["_plan_1", "plan-2"])


if __name__ == "__main__":
    unittest.main()
