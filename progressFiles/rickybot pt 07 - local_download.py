from transformers import ViTImageProcessor, ViTForImageClassification

feature_extractor = ViTImageProcessor.from_pretrained('google/vit-base-patch16-224')
model = ViTForImageClassification.from_pretrained('google/vit-base-patch16-224')

model.save_pretrained('./vit')
feature_extractor.save_pretrained('./vit')
