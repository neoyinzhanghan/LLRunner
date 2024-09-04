import torch
import torch.nn as nn
import pytorch_lightning as pl
import os
from LLRunner.config import BMA_speciment_clf_ckpt_path
from torchvision import transforms, models
from PIL import Image


# Define the model class (must match the training script)
class ResNeXtModel(pl.LightningModule):
    def __init__(self, num_classes=2, class_weights=None):
        super(ResNeXtModel, self).__init__()
        self.model = models.resnext50_32x4d(pretrained=True)
        num_ftrs = self.model.fc.in_features
        self.model.fc = nn.Linear(num_ftrs, num_classes)

    def forward(self, x):
        return self.model(x)


# Define test image transformations (should match those used in validation)
test_transforms = transforms.Compose(
    [
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
    ]
)


def predict_image(model, pil_image, device):
    image = test_transforms(pil_image).unsqueeze(0)
    model.eval()
    model.to(device)
    image = image.to(device)
    output = model(image)
    positive_score = output[0][1].item()

    return positive_score


def get_topview_bma_score(pil_image):
    model = ResNeXtModel.load_from_checkpoint(BMA_speciment_clf_ckpt_path, strict=False)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return predict_image(model, pil_image, device)


if __name__ == "__main__":

    # Minimal Usage Example
    # Load the model
    model = ResNeXtModel.load_from_checkpoint(BMA_speciment_clf_ckpt_path)

    test_image_path = "H19-3075;S10;MSKI - 2023-08-31 14.23.48.jpg"  # NOTE THAT THIS TEST IMAGE PATH ONLY APPLIES TO GLV3

    # Load the test image
    test_image = Image.open(test_image_path)

    test_image_score = get_topview_bma_score(test_image)

    print(f"Test image score: {test_image_score}")
