Running the container(s)
```bash
podman run -it --rm --name spark-delta-container -p 4040:4040 -p 8080:8080 quality_gates:latest
```

Running the producer in a valid scenario:
```bash
uv run /home/marti/GIT/PRIV/quality_gates/producer/src/producer/main.py /home/marti/GIT/PRIV/quality_gates/data_store/transactions/src/valid.csv /home/marti/GIT/PRIV/quality_gates/data_store/transactions/gold
```