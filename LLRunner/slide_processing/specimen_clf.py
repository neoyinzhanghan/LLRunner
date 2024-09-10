import torch
import torch.nn as nn
import pytorch_lightning as pl
import pandas as pd
from pathlib import Path
from LLRunner.config import (
    BMA_specimen_clf_ckpt_path,
    PBS_specimen_clf_ckpt_path,
    slide_metadata_path,
    tmp_slide_dir,
)
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
    checkpoint = torch.load(BMA_specimen_clf_ckpt_path, map_location=device)

    state_dict = checkpoint["state_dict"]
    filtered_state_dict = {
        k: v for k, v in state_dict.items() if k in model.state_dict()
    }

    # Load the filtered state_dict into the model
    model.load_state_dict(filtered_state_dict, strict=False)
    model.eval()

    return model

def load_pbs_specimen_clf_model():
    model = ResNeXtModel(num_classes=2)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    checkpoint = torch.load(PBS_specimen_clf_ckpt_path, map_location=device)

    state_dict = checkpoint["state_dict"]
    filtered_state_dict = {
        k: v for k, v in state_dict.items() if k in model.state_dict()
    }

    # Load the filtered state_dict into the model
    model.load_state_dict(filtered_state_dict, strict=False)
    model.eval()

    return model


def predict_image(model, pil_image, device):
    """
    Predict the class of an image using the given model. Return the positive score.
    """
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
    model = ResNeXtModel.load_from_checkpoint(BMA_specimen_clf_ckpt_path, strict=False)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return predict_image(model, pil_image, device)


def update_bma_specimen_clf_result(wsi_name):
    """First check that the topview for the wsi_name exists, if not raise an error.
    Second check that the wsi_name is already in the slide_metadata_path, if not then raise an error.
    Also check that the slide_metadata_path exists and has the column, if not then create them.
    """

    # first make sure that the csv file at slide_metadata_path has columns named BMA_specimen_clf_score and BMA_specimen_clf_error
    # if not then create that column with None as the value for all the rows, if the column already exists then do nothing
    slide_metadata_df = pd.read_csv(slide_metadata_path)

    # use pathlib to replace the extension of wsi_name with .jpg and check if the file exists in the topview subfolder of the tmp_slide_dir
    wsi_name_path = Path(wsi_name)
    topview_path = Path(tmp_slide_dir) / "topview" / (wsi_name_path.stem + ".jpg")

    # assert topview_path.exists(), f"Error: Topview for {wsi_name} does not exist." # DEPRECATED

    # now check if the wsi_name is in the slide_metadata_df
    assert (
        wsi_name in slide_metadata_df["wsi_name"].values
    ), f"Error: {wsi_name} not found in slide_metadata_df."

    if "BMA_specimen_clf_score" not in slide_metadata_df.columns:
        slide_metadata_df["BMA_specimen_clf_score"] = None
    if "BMA_specimen_clf_error" not in slide_metadata_df.columns:
        slide_metadata_df["BMA_specimen_clf_error"] = None

    # now open the topview image and get the score
    error = None
    try:
        topview_image = Image.open(topview_path)
        score = get_topview_bma_score(topview_image)
    except Exception as e:
        error = str(e)
        score = None

    # now update the slide_metadata_df with the score and error
    slide_metadata_df.loc[
        slide_metadata_df["wsi_name"] == wsi_name, "BMA_specimen_clf_score"
    ] = score

    slide_metadata_df.loc[
        slide_metadata_df["wsi_name"] == wsi_name, "BMA_specimen_clf_error"
    ] = error

    # now save the slide_metadata_df back to the slide_metadata_path
    slide_metadata_df.to_csv(slide_metadata_path, index=False)

    return score, error


if __name__ == "__main__":

    # Minimal Usage Example
    # Load the model
    model = load_bma_specimen_clf_model()

    test_image_path = "/media/hdd3/neo/tmp_slide_dir/topview/H19-3075;S10;MSKI - 2023-08-31 14.23.48.jpg"  # NOTE THAT THIS TEST IMAGE PATH ONLY APPLIES TO GLV3

    # Load the test image
    test_image = Image.open(test_image_path)

    test_image_score = get_topview_bma_score(test_image)

    print(f"Test image score: {test_image_score}")
