FROM jupyter/minimal-notebook:lab-3.2.5

# # required for cartopy
# RUN apt-get update \
#  && apt-get install -y software-properties-common build-essential \
#  && add-apt-repository -y ppa:ubuntugis/ppa \
#  && apt-get update \
#  && apt-get install -y libproj-dev libgeos++-dev libgeos-3.9.1 libgeos-c1v5 libgeos-dev libgeos-doc proj-bin \
#  && apt-get clean \
#  && pip install shapely
# RUN conda install pyproj shapely cartopy

# COPY ./requirements.txt ${HOME}/requirements.txt
COPY ./environment.yaml ${HOME}/environment.yaml

# RUN pip install -r requirements.txt --no-cache-dir --ignore-installed \
RUN conda env update --name base --file environment.yaml --prune
RUN jupyter labextension install \
    jupyterlab-system-monitor \
    jupyterlab-topbar-extension \
    jupyter-matplotlib \
 && rm environment.yaml
#  && rm requirements.txt

RUN echo "c.NotebookApp.iopub_data_rate_limit = 10000000" >> /home/jovyan/.jupyter/jupyter_notebook_config.py
RUN echo "c.NotebookApp.iopub_msg_rate_limit = 100000" >> /home/jovyan/.jupyter/jupyter_notebook_config.py

CMD ["jupyter", "lab", "--port=8888", "--no-browser", "--ip=0.0.0.0"]
