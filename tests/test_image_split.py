"""Master-side tiling: splitting then reconstructing must return the original image."""
import pytest
from PIL import Image, ImageChops

from app.services.image_processor import ImageSplitter
from app.config import TILE_SIZE, MIN_IMAGE_SIZE


def _make_image(tmp_path, width, height):
    """Write a deterministic non-uniform RGB image so tile positions actually matter."""
    img = Image.new("RGB", (width, height))
    px = img.load()
    for y in range(height):
        for x in range(width):
            px[x, y] = (x % 256, y % 256, (x + y) % 256)
    path = tmp_path / "input.png"
    img.save(path)
    return path


def _reconstruct(tiles, width, height):
    tiles_dict = {t["tile_id"]: {"image": t["image"], "x": t["x"], "y": t["y"]} for t in tiles}
    return ImageSplitter.reconstruct_image(tiles_dict, width, height, "job")


@pytest.mark.parametrize("width,height", [
    (1024, 1024),      # exact multiple of TILE_SIZE
    (1024 + 200, 1024 + 100),  # ragged edges — last row/column tiles are smaller
])
def test_split_reconstruct_roundtrip(tmp_path, width, height):
    path = _make_image(tmp_path, width, height)
    tiles, w, h = ImageSplitter.split_image(str(path), "job")

    assert (w, h) == (width, height)
    # Tile count matches a ceil-division grid.
    expected = ((width + TILE_SIZE - 1) // TILE_SIZE) * ((height + TILE_SIZE - 1) // TILE_SIZE)
    assert len(tiles) == expected

    result = _reconstruct(tiles, w, h)
    original = Image.open(path).convert("RGB")
    assert ImageChops.difference(result, original).getbbox() is None


def test_validate_rejects_small_image(tmp_path):
    path = _make_image(tmp_path, MIN_IMAGE_SIZE - 1, MIN_IMAGE_SIZE - 1)
    with pytest.raises(ValueError):
        ImageSplitter.split_image(str(path), "job")
