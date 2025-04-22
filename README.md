Running the container(s)
```bash
podman run -it --rm --name spark-delta-container -p 4040:4040 -p 8080:8080 quality_gates:latest
```