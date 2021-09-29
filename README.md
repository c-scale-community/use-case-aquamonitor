# use-case-aquamonitor

## Architecture

[architecture](./img/C_Scale_Aquamonitor.svg)

## Build docker image

```
docker build -t aquamonitor .
```

## Run docker container
```
docker run -p 8888:8888 -v $(pwd):/home/jovyan/work aquamonitor
```

## Run Notebook

Copy the output from the docker run command in your browser:
http://127.0.0.1:8888/?token=xxx
