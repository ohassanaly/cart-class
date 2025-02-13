#This framework to train a BERT NER model with annotated clinical notes is taken from https://aphp.github.io/edsnlp/latest/tutorials/make-a-training-script/
#To try it, one could use QUAERO dataset https://quaerofrenchmed.limsi.fr/#download 

from copy import deepcopy
from typing import Iterator

import torch
from confit import Cli
from tqdm import tqdm

import edsnlp
import edsnlp.pipes as eds
from edsnlp.metrics.ner import NerExactMetric
from edsnlp.utils.batching import stat_batchify

app = Cli(pretty_exceptions_show_locals=False) # False to avoid exposing sensitive data in production


@app.command(name="train", registry=edsnlp.registry)  
def train_model(
    nlp: edsnlp.Pipeline,
    train_data_path: str,
    val_data_path: str,
    batch_size: int = 32 * 128,
    lr: float = 1e-4,
    max_steps: int = 400,
    num_preprocessing_workers: int = 1,
    evaluation_interval: int = 100,
):
    device = "cuda" if torch.cuda.is_available() else "cpu"

    # Define function to skip empty docs
    def skip_empty_docs(batch: Iterator) -> Iterator:
        for doc in batch:
            if len(doc.ents) > 0:
                yield doc

    # Load and process training data
    training_data = (
        edsnlp.data.read_standoff(
            train_data_path,
            span_setter=["ents", "gold-ner"],
            tokenizer=nlp.tokenizer,
        )
        .map(eds.split(regex="\n\n"))
        .map_batches(skip_empty_docs)
    )

    # Load validation data
    val_data = edsnlp.data.read_standoff(
        val_data_path,
        span_setter=["ents", "gold-ner"],
        tokenizer=nlp.tokenizer,
    )
    val_docs = list(val_data)

    # Initialize components
    nlp.post_init(training_data)

    # Prepare the stream of batches
    batches = (
        training_data.loop()
        .shuffle("dataset")
        .map(nlp.preprocess, kwargs={"supervision": True})
        .batchify(batch_size=batch_size, batch_by=stat_batchify("tokens"))
        .map(nlp.collate, kwargs={"device": device})
        .set_processing(num_cpu_workers=1, process_start_method="spawn")
    )

    # Move the model to the GPU if available
    nlp.to(device)

    # Initialize optimizer
    optimizer = torch.optim.AdamW(params=nlp.parameters(), lr=lr)

    metric = NerExactMetric(span_getter=nlp.pipes.ner.target_span_getter)

    # Training loop
    iterator = iter(batches)
    for step in tqdm(range(max_steps), "Training model", leave=True):
        batch = next(iterator)
        optimizer.zero_grad()

        with nlp.cache():
            loss = torch.zeros((), device=device)
            for name, component in nlp.torch_components():
                output = component(batch[name])
                if "loss" in output:
                    loss += output["loss"]

        loss.backward()
        optimizer.step()

        # Evaluation and model saving
        if ((step + 1) % evaluation_interval) == 0:
            with nlp.select_pipes(enable=["ner"]):
                # Clean the documents that our model will annotate
                preds = deepcopy(val_docs)
                for doc in preds:
                    doc.ents = doc.spans["gold-ner"] = []
                preds = nlp.pipe(preds)
                print(metric(val_docs, preds))

            nlp.to_disk("model")


if __name__ == "__main__":
    nlp = edsnlp.blank("eds")
    nlp.add_pipe(
        eds.ner_crf(
            mode="joint",
            target_span_getter="gold-ner",
            window=20,
            embedding=eds.text_cnn(
                kernel_sizes=[3],
                embedding=eds.transformer(
                    model="prajjwal1/bert-tiny", #tiny model for POC ; use instead camembert-base
                    window=128,
                    stride=96,
                ),
            ),
        ),
        name="ner",
    )
    train_model(
        nlp,
        train_data_path="brat_data/train", #fill here the path for brat annotated training data
        val_data_path="brat_data/test", #fill here the path for brat annotated validating data
        batch_size=32 * 128,
        lr=1e-4,
        max_steps=1000,
        num_preprocessing_workers=1,
        evaluation_interval=100,
    )