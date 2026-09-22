import os
import glob
from PIL import Image
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
import numpy as np

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

CHARS = "0123456789"
char_to_idx = {c: i + 1 for i, c in enumerate(CHARS)}  # 0 is reserved for CTC blank
idx_to_char = {i + 1: c for i, c in enumerate(CHARS)}
NUM_CLASSES = len(CHARS) + 1

class TimerDataset(Dataset):
    def __init__(self, img_dir):
        self.paths = glob.glob(os.path.join(img_dir, "*.png"))

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        path = self.paths[idx]
        img = Image.open(path).convert("RGB").resize((70, 25))
        img = torch.tensor(np.array(img), dtype=torch.float32).permute(2, 0, 1) / 255.0

        # filename = label
        label_str = os.path.splitext(os.path.basename(path))[0]
        label, leftover = label_str.split("_")
        label = torch.tensor([char_to_idx[c] for c in label], dtype=torch.long)

        return img, label, len(label)

def collate_fn(batch):
    images, labels, lengths = zip(*batch)

    images = torch.stack(images)

    labels_concat = torch.cat(labels)
    label_lengths = torch.tensor(lengths, dtype=torch.long)

    return images, labels_concat, label_lengths

class TimerOCR(nn.Module):
    def __init__(self):
        super().__init__()

        self.cnn = nn.Sequential(
            nn.Conv2d(3, 32, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2, 2),  # 25x70 → 12x35

            nn.Conv2d(32, 64, 3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d((2,1)),  # 12x35 → 6x35

            nn.Conv2d(64, 128, 3, padding=1),
            nn.ReLU(),
        )

        self.rnn = nn.LSTM(
            input_size=128,
            hidden_size=128,
            num_layers=2,
            bidirectional=True
        )

        self.fc = nn.Linear(256, NUM_CLASSES)

    def forward(self, x):
        x = self.cnn(x)  # (B, C, H, W)
        x = x.mean(dim=2)  # collapse height → (B, C, W)
        x = x.permute(2, 0, 1)  # (T, B, C)
        x, _ = self.rnn(x)
        x = self.fc(x)  # (T, B, C)
        return F.log_softmax(x, dim=2)


def train(model, dataloader, epochs=10):
    model.to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=5e-4)
    ctc_loss = nn.CTCLoss(blank=0, zero_infinity=True)

    for epoch in range(epochs):
        model.train()
        total_loss = 0
        i = 0
        for images, labels, label_lengths in dataloader:
            images = images.to(device)
            labels = labels.to(device)
            label_lengths = label_lengths.to(device)

            optimizer.zero_grad()

            outputs = model(images)  # (T, B, C)
            T, B, _ = outputs.shape

            input_lengths = torch.full(size=(B,), fill_value=T, dtype=torch.long).to(device)

            loss = ctc_loss(outputs, labels, input_lengths, label_lengths)
            loss.backward()
            optimizer.step()
            i += 1

            total_loss += loss.item()
            print(f"Batch {i} / 313, Loss: {loss}")

        print(f"Epoch {epoch+1}, Loss: {total_loss:.4f}")

    torch.save(model.state_dict(), "../Experimental/ReadTimeModel.pth")

def greedy_decode(output):
    # output: (T, B, C)
    output = output.argmax(2)  # (T, B)

    results = []
    for b in range(output.shape[1]):
        seq = output[:, b].cpu().numpy()

        prev = -1
        decoded = []
        for s in seq:
            if s != prev and s != 0:
                decoded.append(idx_to_char[s])
            prev = s

        results.append("".join(decoded))

    return results


def predict(model, image_path):
    model.eval()
    image = Image.fromarray(image_path)
    img = image.convert("RGB").resize((70, 25))
    img = torch.tensor(np.array(img), dtype=torch.float32).permute(2, 0, 1) / 255.0
    img = img.unsqueeze(0).to(device)

    with torch.no_grad():
        output = model(img)
        pred = greedy_decode(output)[0]

    return pred


"""dataset = TimerDataset("Images/Training")
loader = DataLoader(dataset, batch_size=64, shuffle=True, collate_fn=collate_fn)
model = TimerOCR()
model.load_state_dict(torch.load("ReadTimeModel.pth"))
model.to(device).eval()
train(model, loader, epochs=50)"""

