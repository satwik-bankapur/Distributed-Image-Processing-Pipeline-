# worker/services/image_processor.py

import cv2
import numpy as np
from PIL import Image
import io
import base64

class ImageProcessor:
    """Process image tiles with various transformations"""
    
    @staticmethod
    def base64_to_image(img_base64):
        """Convert base64 string to PIL Image"""
        try:
            img_bytes = base64.b64decode(img_base64)
            img = Image.open(io.BytesIO(img_bytes))
            return np.array(img)
        except Exception as e:
            print(f"Error decoding image: {str(e)}")
            raise
    
    @staticmethod
    def image_to_base64(img_array):
        """Convert image array to base64 string"""
        try:
            # Convert numpy array to PIL Image
            if len(img_array.shape) == 2:  # Grayscale
                img = Image.fromarray(img_array, mode='L')
            else:  # RGB or RGBA
                img = Image.fromarray(img_array)
            
            # Save to bytes
            buffered = io.BytesIO()
            img.save(buffered, format="PNG")
            img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return img_base64
        except Exception as e:
            print(f"Error encoding image: {str(e)}")
            raise
    
    @staticmethod
    def grayscale(img_array):
        """Convert image to grayscale"""
        try:
            if len(img_array.shape) == 2:
                return img_array  # Already grayscale
            
            # Convert RGB to Grayscale
            if img_array.shape[2] == 3:  # RGB
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            elif img_array.shape[2] == 4:  # RGBA
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGBA2GRAY)
            else:
                gray = cv2.cvtColor(img_array, cv2.COLOR_BGR2GRAY)
            
            return gray
        except Exception as e:
            print(f"Grayscale processing error: {str(e)}")
            raise
    
    @staticmethod
    def blur(img_array):
        """Apply Gaussian blur"""
        try:
            # Apply blur with kernel size 5x5
            blurred = cv2.GaussianBlur(img_array, (5, 5), 0)
            return blurred
        except Exception as e:
            print(f"Blur processing error: {str(e)}")
            raise
    
    @staticmethod
    def edge_detection(img_array):
        """Canny edge detection"""
        try:
            # Convert to grayscale if color
            if len(img_array.shape) == 3:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            else:
                gray = img_array
            
            # Apply Canny edge detection
            edges = cv2.Canny(gray, 100, 200)
            return edges
        except Exception as e:
            print(f"Edge detection error: {str(e)}")
            raise
    
    @staticmethod
    def sharpen(img_array):
        """Sharpen image"""
        try:
            # Define sharpen kernel
            kernel = np.array([[-1, -1, -1],
                              [-1,  9, -1],
                              [-1, -1, -1]])
            
            # Apply kernel
            sharpened = cv2.filter2D(img_array, -1, kernel)
            return sharpened
        except Exception as e:
            print(f"Sharpen processing error: {str(e)}")
            raise
    
    @staticmethod
    def brightness_increase(img_array):
        """Increase brightness by 20%"""
        try:
            # Increase brightness (scale by 1.2, max 255)
            brightened = cv2.convertScaleAbs(img_array, alpha=1.2, beta=0)
            return np.clip(brightened, 0, 255).astype(np.uint8)
        except Exception as e:
            print(f"Brightness increase error: {str(e)}")
            raise
    
    @staticmethod
    def process_tile(tile_data_base64, transformation):
        """
        Process a single tile with specified transformation
        
        Args:
            tile_data_base64: Base64 encoded image
            transformation: Type of transformation to apply
            
        Returns:
            Processed image as base64 string
        """
        try:
            # Decode image
            img_array = ImageProcessor.base64_to_image(tile_data_base64)
            
            print(f"Processing tile with {transformation}...")
            print(f"Input shape: {img_array.shape}")
            
            # Apply transformation
            if transformation == 'grayscale':
                processed = ImageProcessor.grayscale(img_array)
            elif transformation == 'blur':
                processed = ImageProcessor.blur(img_array)
            elif transformation == 'edge_detection':
                processed = ImageProcessor.edge_detection(img_array)
            elif transformation == 'sharpen':
                processed = ImageProcessor.sharpen(img_array)
            elif transformation == 'brightness_increase':
                processed = ImageProcessor.brightness_increase(img_array)
            else:
                print(f"Unknown transformation: {transformation}")
                processed = img_array
            
            print(f"Output shape: {processed.shape}")
            
            # Encode result
            result_base64 = ImageProcessor.image_to_base64(processed)
            
            return result_base64
            
        except Exception as e:
            print(f"Error processing tile: {str(e)}")
            raise

