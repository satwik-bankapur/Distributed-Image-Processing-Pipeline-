"""Worker-side image transforms: shape/dtype invariants and the base64 round-trip."""
import base64

import numpy as np
import pytest

from worker.services.image_processor import ImageProcessor


@pytest.fixture
def rgb_tile():
    # 64x64 RGB gradient — small but exercises every code path.
    tile = np.zeros((64, 64, 3), dtype=np.uint8)
    tile[:, :, 0] = np.arange(64, dtype=np.uint8)
    tile[:, :, 1] = np.arange(64, dtype=np.uint8)[:, None]
    return tile


def test_base64_roundtrip(rgb_tile):
    encoded = ImageProcessor.image_to_base64(rgb_tile)
    assert isinstance(encoded, str)
    decoded = ImageProcessor.base64_to_image(encoded)
    assert decoded.shape == rgb_tile.shape


def test_grayscale_collapses_channels(rgb_tile):
    out = ImageProcessor.grayscale(rgb_tile)
    assert out.ndim == 2
    assert out.shape == rgb_tile.shape[:2]


def test_edge_detection_is_2d_uint8(rgb_tile):
    out = ImageProcessor.edge_detection(rgb_tile)
    assert out.ndim == 2
    assert out.dtype == np.uint8


@pytest.mark.parametrize("op", ["blur", "sharpen", "brightness_increase"])
def test_shape_preserving_ops(rgb_tile, op):
    out = getattr(ImageProcessor, op)(rgb_tile)
    assert out.shape == rgb_tile.shape
    assert out.dtype == np.uint8


def test_process_tile_end_to_end(rgb_tile):
    tile_b64 = ImageProcessor.image_to_base64(rgb_tile)
    result_b64 = ImageProcessor.process_tile(tile_b64, "grayscale")
    # Result must be valid base64 that decodes to a single-channel tile.
    result = ImageProcessor.base64_to_image(result_b64)
    base64.b64decode(result_b64)  # raises if not valid base64
    assert result.shape == rgb_tile.shape[:2]


def test_unknown_transformation_is_passthrough(rgb_tile):
    tile_b64 = ImageProcessor.image_to_base64(rgb_tile)
    result_b64 = ImageProcessor.process_tile(tile_b64, "does_not_exist")
    result = ImageProcessor.base64_to_image(result_b64)
    assert result.shape == rgb_tile.shape
