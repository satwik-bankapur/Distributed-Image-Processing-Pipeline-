# app/services/image_processor.py

import logging

from PIL import Image

from app.config import TILE_SIZE, MIN_IMAGE_SIZE

logger = logging.getLogger("master")


class ImageSplitter:
    """Splits images into tiles for distributed processing and reassembles results."""

    @staticmethod
    def validate_image(image_path):
        """Open an image and enforce the minimum-size requirement."""
        img = Image.open(image_path)
        width, height = img.size
        if width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE:
            raise ValueError(
                f"Image must be at least {MIN_IMAGE_SIZE}x{MIN_IMAGE_SIZE}. Got {width}x{height}"
            )
        return img

    @staticmethod
    def split_image(image_path, job_id):
        """Split an image into TILE_SIZE tiles.

        Returns (tiles, width, height), where each tile carries its grid position
        so the master can reconstruct the result without tracking order.
        """
        img_rgb = ImageSplitter.validate_image(image_path).convert("RGB")

        tiles = []
        tile_id = 0
        for y in range(0, img_rgb.height, TILE_SIZE):
            for x in range(0, img_rgb.width, TILE_SIZE):
                x_end = min(x + TILE_SIZE, img_rgb.width)
                y_end = min(y + TILE_SIZE, img_rgb.height)
                tile = img_rgb.crop((x, y, x_end, y_end))
                tiles.append({
                    "tile_id": tile_id,
                    "x": x,
                    "y": y,
                    "width": tile.width,
                    "height": tile.height,
                    "image": tile,
                    "job_id": job_id,
                })
                tile_id += 1

        logger.info("Split into %d tiles (%dx%d), image %dx%d",
                    len(tiles), TILE_SIZE, TILE_SIZE, img_rgb.width, img_rgb.height)
        return tiles, img_rgb.width, img_rgb.height

    @staticmethod
    def reconstruct_image(tiles_dict, image_width, image_height, job_id):
        """Paste processed tiles back onto a blank canvas at their original positions."""
        result_img = Image.new("RGB", (image_width, image_height))
        for tile_data in tiles_dict.values():
            result_img.paste(tile_data["image"], (tile_data["x"], tile_data["y"]))
        logger.info("Reconstructed job %s: %d tiles assembled", job_id, len(tiles_dict))
        return result_img
