FROM jupyter/minimal-notebook:lab-3.2.5

COPY ./environment.yaml ${HOME}/environment.yaml

RUN conda env update --name base --file environment.yaml --prune
RUN jupyter labextension install \
    jupyterlab-system-monitor \
    jupyterlab-topbar-extension \
    jupyter-matplotlib \
 && rm environment.yaml

RUN echo "c.NotebookApp.iopub_data_rate_limit = 10000000" >> /home/jovyan/.jupyter/jupyter_notebook_config.py
RUN echo "c.NotebookApp.iopub_msg_rate_limit = 100000" >> /home/jovyan/.jupyter/jupyter_notebook_config.py

CMD ["jupyter", "lab", "--port=8888", "--no-browser", "--ip=0.0.0.0"]
