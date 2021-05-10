FROM continuumio/miniconda3

COPY environment.yml /tmp/environment.yml
RUN conda env create -f /tmp/environment.yml

RUN echo "source activate openeo_platform" > /root/.bashrc
ENV PATH=/opt/conda/envs/openeo_platform/bin:$PATH 

RUN mkdir /data
WORKDIR /data

CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]
