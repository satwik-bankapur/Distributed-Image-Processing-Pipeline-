import cv2
import numpy as np
from PIL import Image
import io
import base64

class ImageProcessor:
    @staticmethod
    def base64_to_image(img_base64):
        try:
            img_bytes = base64.b64decode(img_base64)
            img = Image.open(io.BytesIO(img_bytes))
            return np.array(img)
        except Exception as e:
            print(f"Error decoding image: {str(e)}")
            raise
    
    @staticmethod
    def image_to_base64(img_array):
        try:
            if len(img_array.shape) == 2:
                img = Image.fromarray(img_array, mode='L')
            else:
                img = Image.fromarray(img_array)
            
            buffered = io.BytesIO()
            img.save(buffered, format="PNG")
            img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
            return img_base64
        except Exception as e:
            print(f"Error encoding image: {str(e)}")
            raise
    
    @staticmethod
    def grayscale(img_array):
        try:
            if len(img_array.shape) == 2:
                return img_array
            
            if img_array.shape[2] == 3:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            elif img_array.shape[2] == 4:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGBA2GRAY)
            else:
                gray = cv2.cvtColor(img_array, cv2.COLOR_BGR2GRAY)
            
            return gray
        except Exception as e:
            print(f"Grayscale processing error: {str(e)}")
            raise
    
    @staticmethod
    def blur(img_array):
        try:
            blurred = cv2.GaussianBlur(img_array, (5, 5), 0)
            return blurred
        except Exception as e:
            print(f"Blur processing error: {str(e)}")
            raise
    
    @staticmethod
    def edge_detection(img_array):
        try:
            if len(img_array.shape) == 3:
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            else:
                gray = img_array
            
            edges = cv2.Canny(gray, 100, 200)
            return edges
        except Exception as e:
            print(f"Edge detection error: {str(e)}")
            raise
    
    @staticmethod
    def sharpen(img_array):
        try:
            kernel = np.array([[-1, -1, -1],
                              [-1,  9, -1],
                              [-1, -1, -1]])
            
            sharpened = cv2.filter2D(img_array, -1, kernel)
            return sharpened
        except Exception as e:
            print(f"Sharpen processing error: {str(e)}")
            raise
    
    @staticmethod
    def brightness_increase(img_array):
        try:
            brightened = cv2.convertScaleAbs(img_array, alpha=1.2, beta=0)
            return np.clip(brightened, 0, 255).astype(np.uint8)
        except Exception as e:
            print(f"Brightness increase error: {str(e)}")
            raise
    
    @staticmethod
    def process_tile(tile_data_base64, transformation):
        try:
            img_array = ImageProcessor.base64_to_image(tile_data_base64)
            
            print(f"Processing tile with {transformation}...")
            print(f"Input shape: {img_array.shape}")
            
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
            
            result_base64 = ImageProcessor.image_to_base64(processed)
            
            return result_base64
            
        except Exception as e:
            print(f"Error processing tile: {str(e)}")
            raise

