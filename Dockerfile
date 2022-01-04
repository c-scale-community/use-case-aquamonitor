FROM jupyter/minimal-notebook:lab-3.2.5

USER root

# # required for cartopy
# RUN apt-get update \
#  && apt-get install -y software-properties-common build-essential \
#  && add-apt-repository -y ppa:ubuntugis/ppa \
#  && apt-get update \
#  && apt-get install -y libproj-dev libgeos++-dev libgeos-3.9.1 libgeos-c1v5 libgeos-dev libgeos-doc proj-bin \
#  && apt-get clean \
#  && pip install shapely
RUN conda install cartopy

COPY ./requirements.txt ${HOME}/requirements.txt

RUN pip install -r requirements.txt --no-cache-dir --ignore-installed \
 && jupyter labextension install \
    jupyterlab-system-monitor \
    jupyterlab-topbar-extension \
    jupyter-matplotlib \
 && rm requirements.txt

RUN echo "c.NotebookApp.iopub_data_rate_limit = 10000000" >> /home/jovyan/.jupyter/jupyter_notebook_config.py
RUN echo "c.NotebookApp.iopub_msg_rate_limit = 100000" >> /home/jovyan/.jupyter/jupyter_notebook_config.py

USER ${NB_UID}

CMD ["jupyter", "lab", "--port=8888", "--no-browser", "--ip=0.0.0.0"]
