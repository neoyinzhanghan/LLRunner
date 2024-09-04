import torch
import torch.nn as nn
import pytorch_lightning as pl
from LLRunner.config import BMA_speciment_clf_ckpt_path
from torchvision import transforms, models
from PIL import Image
from torchvision.models import ResNeXt50_32X4D_Weights


# Define the model class (must match the training script)
class ResNeXtModel(pl.LightningModule):
    def __init__(self, num_classes=2, class_weights=None):
        super(ResNeXtModel, self).__init__()
        self.model = models.resnext50_32x4d(weights=ResNeXt50_32X4D_Weights.DEFAULT)
        num_ftrs = self.model.fc.in_features
        self.model.fc = nn.Linear(num_ftrs, num_classes)
        self.criterion = nn.CrossEntropyLoss(
            weight=class_weights
        )  # Class weights (if any)

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


def load_bma_specimen_clf_model():
    model = ResNeXtModel(num_classes=2)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    checkpoint = torch.load(BMA_speciment_clf_ckpt_path, map_location=device)

    state_dict = checkpoint["state_dict"]
    filtered_state_dict = {
        k: v for k, v in state_dict.items() if k in model.state_dict()
    }

    # Load the filtered state_dict into the model
    model.load_state_dict(filtered_state_dict, strict=False)
    model.eval()

    return model


def predict_image(model, pil_image, device):
    image = test_transforms(pil_image).unsqueeze(0)
    model.eval()
    model.to(device)
    image = image.to(device)
    output = model(image)

    # apply softmax to get the positive score
    softmax_fn = nn.Softmax(dim=1)

    softmax_output = softmax_fn(output)

    positive_score = softmax_output[0][0].item()

    return positive_score


def get_topview_bma_score(pil_image):
    model = ResNeXtModel.load_from_checkpoint(BMA_speciment_clf_ckpt_path, strict=False)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return predict_image(model, pil_image, device)


if __name__ == "__main__":

    # Minimal Usage Example
    # Load the model
    model = load_bma_specimen_clf_model()

    test_image_path = "/media/hdd3/neo/tmp_slide_dir/topview/H19-3075;S10;MSKI - 2023-08-31 14.23.48.jpg"  # NOTE THAT THIS TEST IMAGE PATH ONLY APPLIES TO GLV3

    # Load the test image
    test_image = Image.open(test_image_path)

    test_image_score = get_topview_bma_score(test_image)

    print(f"Test image score: {test_image_score}")