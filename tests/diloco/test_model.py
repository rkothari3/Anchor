import torch

from dssd.diloco.data import CharTokenizer, make_batch, synthetic_corpus
from dssd.diloco.model import ModelConfig, TinyGPT


def test_inner_loop_reduces_loss():
    torch.manual_seed(0)

    text = synthetic_corpus(length=2000)
    tokenizer = CharTokenizer(text)
    data = torch.tensor(tokenizer.encode(text), dtype=torch.long)

    cfg = ModelConfig(vocab_size=tokenizer.vocab_size, block_size=16, n_embd=32, n_head=2, n_layer=2)
    model = TinyGPT(cfg)
    optimizer = torch.optim.AdamW(model.parameters(), lr=3e-3)

    def step() -> float:
        x, y = make_batch(data, cfg.block_size, batch_size=16)
        optimizer.zero_grad()
        _, loss = model(x, y)
        loss.backward()
        optimizer.step()
        return loss.item()

    first_loss = step()
    for _ in range(49):
        last_loss = step()

    assert last_loss < first_loss * 0.7
