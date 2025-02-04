from ultralytics import YOLO

def get_model(model_name):

    return YOLO(model_name)

def get_detections(model, image_paths):

    # Process each image
    detections = []
    for img_path in image_paths:
        results = model(img_path)
        
        detected_objects = []
        for result in results:
            for box in result.boxes:
                detected_objects.append({
                    "class": result.names[int(box.cls)],  # Object class
                    "confidence": float(box.conf),  # Confidence score
                    "bbox": box.xyxy.tolist()[0]  # Bounding box [x1, y1, x2, y2]
                })
        
        detections.append({
            "image_path": img_path,
            "detections": detected_objects
        })

    return detections