# app/services/image_processor.py

from PIL import Image
import os
from app.config import TILE_SIZE, MIN_IMAGE_SIZE, RESULTS_DIR

class ImageSplitter:
    """Splits images into tiles for distributed processing"""
    
    @staticmethod
    def validate_image(image_path):
        """Validate image meets size requirements"""
        img = Image.open(image_path)
        width, height = img.size
        
        if width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE:
            raise ValueError(
                f"Image must be at least {MIN_IMAGE_SIZE}x{MIN_IMAGE_SIZE}. "
                f"Got {width}x{height}"
            )
        
        return img
    
    @staticmethod
    def split_image(image_path, job_id):
        """
        Split image into tiles
        
        Args:
            image_path: Path to image file
            job_id: Job identifier
            
        Returns:
            List of tile dictionaries with metadata
        """
        img = ImageSplitter.validate_image(image_path)
        img_rgb = img.convert('RGB')  # Ensure RGB format
        
        tiles = []
        tile_id = 0
        
        # Split image into tiles
        for y in range(0, img_rgb.height, TILE_SIZE):
            for x in range(0, img_rgb.width, TILE_SIZE):
                # Calculate tile boundaries
                x_end = min(x + TILE_SIZE, img_rgb.width)
                y_end = min(y + TILE_SIZE, img_rgb.height)
                
                # Extract tile
                tile = img_rgb.crop((x, y, x_end, y_end))
                
                tiles.append({
                    'tile_id': tile_id,
                    'x': x,
                    'y': y,
                    'x_end': x_end,
                    'y_end': y_end,
                    'width': tile.width,
                    'height': tile.height,
                    'image': tile,
                    'job_id': job_id
                })
                
                tile_id += 1
        
        print(f'Image split into {len(tiles)} tiles ({TILE_SIZE}x{TILE_SIZE})')
        print(f'Image dimensions: {img_rgb.width}x{img_rgb.height}')
        
        return tiles, img_rgb.width, img_rgb.height
    
    @staticmethod
    def get_tile_grid_info(image_width, image_height):
        """Get information about tile grid"""
        tiles_x = (image_width + TILE_SIZE - 1) // TILE_SIZE
        tiles_y = (image_height + TILE_SIZE - 1) // TILE_SIZE
        
        return {
            'tiles_x': tiles_x,
            'tiles_y': tiles_y,
            'total_tiles': tiles_x * tiles_y,
            'image_width': image_width,
            'image_height': image_height
        }

    @staticmethod
    def reconstruct_image(tiles_dict, image_width, image_height, job_id):
        """
        Reconstruct image from processed tiles
        
        Args:
            tiles_dict: Dictionary mapping tile_id to tile data
            image_width: Original image width
            image_height: Original image height
            job_id: Job identifier
            
        Returns:
            PIL Image object
        """
        print(f'Reconstructing image {job_id} ({image_width}x{image_height})...')
        
        # Create blank image
        result_img = Image.new('RGB', (image_width, image_height))
        
        # Place each tile in correct position
        for tile_id, tile_data in tiles_dict.items():
            tile_image = tile_data['image']
            x = tile_data['x']
            y = tile_data['y']
            
            result_img.paste(tile_image, (x, y))
        
        print(f'Image reconstruction complete: {len(tiles_dict)} tiles assembled')
        return result_img
    
    @staticmethod
    def save_result_image(image, job_id, format_name="PNG"):
        """Save reconstructed image to disk"""
        output_path = RESULTS_DIR / f"{job_id}_result.png"
        image.save(output_path, format=format_name)
        print(f'Result image saved: {output_path}')
        return output_path

