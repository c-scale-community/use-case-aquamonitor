# use-case-aquamonitor

## Architecture

[architecture](./img/C_Scale_Aquamonitor.svg)

## Build docker image

```
cd notebooks
docker build -t openeo .
```

## Run docker container
```
docker run -p 8889:8889 -v <notebooks path>:/data openeo
```

## Run Notebook

Copy the output from the docker run command in your browser:
http://127.0.0.1:8889/?token=xxx
